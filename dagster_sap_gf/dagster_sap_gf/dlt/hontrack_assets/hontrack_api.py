from collections import deque
import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.typing import RESTAPIConfig
from dagster_shared_gf.dlt_shared.dlt_resources import (
    dlt_pipeline_dest_mssql_dwh,
    MyDagsterDltTranslator,
    BaseDltPipeline,
)
from dagster_shared_gf.automation import automation_daily_cron
from dagster_shared_gf.partitions.time_based import get_daily_partition_def_to_today
from dagster_embedded_elt.dlt import dlt_assets, DagsterDltResource
from dagster_embedded_elt.dlt.dlt_event_iterator import DltEventIterator, DltEventType
from dagster import (
    BackfillPolicy,
    instance_for_test,
    materialize,
    AssetExecutionContext,
    build_asset_context,
    job,
    TimeWindowPartitionMapping,
    asset,
)
from datetime import datetime, timedelta
from typing import Optional
from dlt.common.pipeline import LoadInfo
from itertools import chain
from pendulum import DateTime as p_datetime

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


pipeline = dlt_pipeline_dest_mssql_dwh.get_pipeline(
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
    
    return [str(start + timedelta(days=i)) for i in range(daily_diffs)]


@dlt_assets(
    dlt_source=hontrack_api_source().with_resources("vehicles_resumen"),
    dlt_pipeline=pipeline,
    name="hontrack_api",
    group_name="hontrack_api",
    dagster_dlt_translator=MyDagsterDltTranslator(
        automation_condition=automation_daily_cron,
        prefix_key=["DL_FARINTER", "hontrack_api"],
    ),
    partitions_def=daily_partitions_def,
)
def hontrack_api_assets_7_days(context: AssetExecutionContext, dlt: DagsterDltResource):
    if context.has_partition_key_range or context.has_partition_key:
        v_date_from = datetime.fromisoformat(context.partition_key_range.start)
        v_date_to = (
            datetime.fromisoformat(context.partition_key_range.end)
            + timedelta(days=1)
            - timedelta(seconds=1)
        )
    else:
        part_def = context.assets_def.partitions_def
        first_partition_key = part_def.get_first_partition_key() if part_def else None
        last_partition_key = part_def.get_last_partition_key() if part_def else None
        def_date_from = first_partition_key or datetime(2024, 1, 1, 0, 0, 0).isoformat()
        def_date_to = (
            last_partition_key
            or (
                datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                + timedelta(days=1)
                - timedelta(seconds=1)
            ).isoformat()
        )
        v_date_from = datetime.fromisoformat(def_date_from)
        v_date_to = datetime.fromisoformat(def_date_to)

    context.log.debug(f"v_date_from: {v_date_from}, v_date_to: {v_date_to}")
    if v_date_from <= (v_date_to - timedelta(days=7)):  # rompe el limite del API
        results: deque[DltEventIterator[DltEventType]] = deque()
        for day in range(0, (v_date_to - v_date_from).days, 1):
            results.append(
                dlt.run(
                    context=context,
                    dlt_source=hontrack_api_source(
                        start_date=v_date_from + timedelta(days=day),
                        end_date=v_date_from
                        + timedelta(days=day + 1)
                        - timedelta(seconds=1),
                    ),
                    dlt_pipeline=pipeline,
                )
            )
        # Consolidar resultados
        yield from chain.from_iterable(results)  # ignore empty results

    else:
        yield from dlt.run(
            context=context,
            dlt_source=hontrack_api_source(start_date=v_date_from, end_date=v_date_to),
            dlt_pipeline=pipeline,
        )

hontrack_api_assets_7_days=hontrack_api_assets_7_days.with_attributes(
    backfill_policy=BackfillPolicy.single_run(),
)

if __name__ == "__main__":
    from dagster import PartitionKeyRange, job


    with instance_for_test() as instance:
        context = build_asset_context(
            partition_key_range=PartitionKeyRange("2024-10-01", "2024-11-06"),
            resources={"dlt": DagsterDltResource()},
            instance=instance,
        )

        hontrack_api_assets_7_days(context=context)

        # @job( partitions_def=daily_partitions_def)
        # def hontrack_api_job():
        #     hontrack_api_assets_7_days()

        # hontrack_api_job.execute_in_process(instance=instance)
        # materialize(
        #     [hontrack_api_assets_7_days],
        #     instance=instance,
        #     resources={"dlt": DagsterDltResource()},
        #     #partition_key="2024-10-25",
        # )
