from collections import deque
from datetime import datetime, timedelta
from itertools import chain
from typing import Iterator, Optional

import dlt
from dagster import (
    AssetExecutionContext,
    BackfillPolicy,
    instance_for_test,
    MetadataValue,
    MaterializeResult,
    AssetMaterialization,
)
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
from dagster_embedded_elt.dlt.dlt_event_iterator import DltEventIterator, DltEventType
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.typing import RESTAPIConfig

from dagster_shared_gf.automation import automation_daily_cron
from dagster_shared_gf.dlt_shared.dlt_resources import (
    MyDagsterDltTranslator,
    dlt_pipeline_dest_mssql_dwh,
)
from dagster_shared_gf.partitions.time_based import get_daily_partition_def_to_today

# def add_and_remove_fields(response: Response, *args, **kwargs) -> Response:
#     payload = response.json()
#     for record in payload["data"]:
#         record["custom_field"] = "foobar"
#         record.pop("email", None)
#     modified_content: bytes = json.dumps(payload).encode("utf-8")
#     response._content = modified_content
#     return response


@dlt.source
def hontrack_api_source(
    start_date: Optional[datetime] = None, end_date: Optional[datetime] = None
):
    api_key = dlt.secrets["hontrack_api_pipeline.sources.api_key"]
    v_start_date: datetime = (
        start_date
        if start_date
        else (datetime.now() - timedelta(days=6)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    )
    v_end_date: datetime = (
        end_date
        if end_date
        else (
            (datetime.now() + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            - timedelta(seconds=1)
        )
    )
    start_date_str = v_start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_date_str = v_end_date.strftime("%Y-%m-%d %H:%M:%S")

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://rma.hontrack.com/api/external/",
            "auth": {
                "token": api_key,
            },
        },
        "resource_defaults": {
            # "primary_key": "id",
            "write_disposition": "merge",
            # "endpoint": {
            #     "params": {
            #         "per_page": 100,
            # },
            # },
        },
        "resources": [
            {
                "name": "vehicles_resumen",
                "primary_key": ["plate", "date_apl"],
                "endpoint": {
                    "path": "vehicles/get_vehicles_resumen.php",
                    "method": "POST",
                    "params": {},
                    "json": {
                        "from_date": start_date_str,  # "2024-10-29 00:00:00",
                        "to_date": end_date_str,  # "2024-11-01 23:59:59", # es inclusivo segun reunion
                        "api_key": api_key,
                    },
                    "response_actions": [
                        {"status_code": 400, "action": "ignore"},
                        {"status_code": 401, "action": "ignore"},
                        {"status_code": 404, "action": "ignore"},
                    ],
                    "data_selector": "payload.vehicles",
                },
            },
        ],
    }

    # # Assume `source` is your already defined source and `resource_name` is the resource you want to modify
    # resource = source.resources.get('resource_name')

    # # Define a new mapping that includes the new column
    # resource.add_map(lambda row: {
    #     **row,  # keep existing columns
    #     'new_column': compute_new_column_value(row)  # add new column
    # })

    source = rest_api_source(config=config, name="hontrack_api")
    vehicles_resumen = source.resources.get("vehicles_resumen")
    if vehicles_resumen:
        vehicles_resumen.add_map(
            lambda row: {
                **row,  # keep existing columns
                "enterprise_id": "farinter",  # add new column
            }
        )

    return source


hontrack_api_pipeline = dlt_pipeline_dest_mssql_dwh.get_pipeline(
    "hontrack_api_pipeline", "hontrack_api"
)
# pipeline.dev_mode = True
# pipeline.refresh = "drop_sources"

daily_partitions_def = get_daily_partition_def_to_today(
    start_date=datetime(2024, 1, 1, 0, 0, 0),
)


def _daily_partition_iter(start, end):
    start = datetime.fromisoformat(start)
    end = datetime.fromisoformat(end)
    daily_diffs = int((end - start) / timedelta(days=1))
    for i in range(daily_diffs):
        yield (
            (start + timedelta(days=i)),
            (start + timedelta(days=i + 1) - timedelta(seconds=1)),
        )


@dlt_assets(
    dlt_source=hontrack_api_source().with_resources("vehicles_resumen"),
    dlt_pipeline=hontrack_api_pipeline,
    name="hontrack_api",
    group_name="hontrack_api",
    dagster_dlt_translator=MyDagsterDltTranslator(
        automation_condition=automation_daily_cron,
        prefix_key=["DL_FARINTER", "hontrack_api"],
    ),
    partitions_def=daily_partitions_def,
)
def hontrack_api_assets_per_day(
    context: AssetExecutionContext, dlt: DagsterDltResource
):
    first_partition, last_partition = context.partition_key_range
    partition_iter = _daily_partition_iter(first_partition, last_partition)
    context.log.debug(f"date_from: {first_partition}, date_to: {last_partition}")

    def consolidar_resultados():
        for start_of_day, end_of_day in partition_iter:
            result = dlt.run(
                context=context,
                dlt_source=hontrack_api_source(
                    start_date=start_of_day,
                    end_date=end_of_day,
                ),
                dlt_pipeline=hontrack_api_pipeline,
            )
            for event in result:
                yield event

    def integrar_resultados() -> Iterator[DltEventType]:
        unique_events: dict[str, DltEventType] = {}
        lost_events: deque[DltEventType] = deque()
        for event in consolidar_resultados():
            askey = event.asset_key
            if askey:
                key = askey.to_python_identifier()
                if key in unique_events:
                    lost_events.append(event)
                else:
                    unique_events[key] = event

        for event in lost_events:
            askey = event.asset_key
            key = askey.to_python_identifier() if askey else ""
            final = unique_events[key]
            meta_1 = final.metadata
            meta_2 = event.metadata
            rows_loaded_1 = meta_1.get("rows_loaded", 0) if meta_1 else 0
            rows_loaded_2 = meta_2.get("rows_loaded", 0) if meta_2 else 0
            jobs_1 = meta_1.get("jobs", []) if meta_1 else []
            jobs_2 = meta_2.get("jobs", []) if meta_2 else []
            new_meta = {
                "rows_loaded": MetadataValue.int(sum(rows_loaded_1 + rows_loaded_2)),  # type: ignore
                "jobs": [*jobs_1, *jobs_2],  # type: ignore
            }
            final_meta = {**meta_1, **meta_2, **new_meta}  # type: ignore
            unique_events[key] = (
                final.with_metadata(final_meta)
                if type(final) is AssetMaterialization
                else MaterializeResult(asset_key=askey, metadata=final_meta)
            )  # type: ignore

        yield from unique_events.values()

    results = DltEventIterator(
        integrar_resultados(), context=context, dlt_pipeline=hontrack_api_pipeline
    )

    # Consolidar resultados
    yield from results


hontrack_api_assets_per_day = hontrack_api_assets_per_day.with_attributes(
    backfill_policy=BackfillPolicy.single_run(),
)

all_assets = (hontrack_api_assets_per_day,)

if __name__ == "__main__":
    from dagster import (
        PartitionKeyRange,
        build_op_context,
        materialize,
        define_asset_job,
        Definitions,
    )

    with instance_for_test() as instance:
        #test job parti
        test_job = define_asset_job("test_job", selection=[hontrack_api_assets_per_day])
        test_resources = {"dlt": DagsterDltResource()}
        defs = Definitions(
            assets=[hontrack_api_assets_per_day],
            jobs=[test_job],
            resources=test_resources,
        )

        test_job_def = defs.get_job_def("test_job")
        test_job_def.execute_in_process(
            tags={
                "dagster/asset_partition_range_start": "2024-11-02",
                "dagster/asset_partition_range_end": "2024-11-05",
            },
            resources=test_resources,
            instance=instance,
        )
        #test single
        materialize(
            [hontrack_api_assets_per_day],
            instance=instance,
            resources={"dlt": DagsterDltResource()},
            partition_key="2024-10-25",
        )
