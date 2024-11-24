from collections import deque
from datetime import datetime, timedelta
import decimal
import json
from typing import Iterator, Optional

from dagster_shared_gf.dlt_shared.dlt_resources import merge_dlt_dagster_metadata
import dlt
from dagster import (
    AssetExecutionContext,
    AssetMaterialization,
    BackfillPolicy,
    MaterializeResult,
    instance_for_test,
)
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
from dagster_embedded_elt.dlt.dlt_event_iterator import DltEventIterator, DltEventType
from dlt.sources.rest_api import rest_api_source
from dlt.common.schema.typing import TWriteDisposition
from dlt.sources.rest_api.typing import (
    RESTAPIConfig,
    PaginatorConfig,
    ClientConfig,
    PaginatorType,
    ResponseActionDict,
)
from dlt.extract import DltResource, DltSource

from dagster_shared_gf.automation import automation_daily_delta_2_cron
from dagster_shared_gf.dlt_shared.dlt_resources import (
    MyDagsterDltTranslator,
    dlt_pipeline_dest_mssql_dwh,
    DltPipelineDestMssqlDwh,
)
from dagster_shared_gf.shared_variables import default_timezone_teg
from dagster_shared_gf.partitions.time_based import get_daily_partition_def_to_today
import pendulum
from requests import Response


def remove_fields(response: Response, *args, **kwargs) -> Response:
    payload = response.json()
    for record in payload["data"]:
        for kwarg in kwargs:
            # record[kwarg] = "foobar"
            record.pop(kwarg, None)
    modified_content: bytes = json.dumps(payload).encode("utf-8")
    response._content = modified_content
    return response


def validate_response(response: Response, *args, **kwargs) -> Response:
    response.raise_for_status()
    if response.status_code == 200 and response.json():
        return response

    raise Exception(response.text[:1000])


def to_str_decimal(value):
    value = (
        decimal.Decimal(value, context=decimal.Context(prec=20)).quantize(
            decimal.Decimal("0.0001"), rounding=decimal.ROUND_HALF_UP
        )
        if value is not None
        else None
    )
    # value = format(decimal.Decimal(value), "020.4f") if value else None
    return value


@dlt.source
def hontrack_api_source(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    write_disposition: TWriteDisposition | None = "merge",
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

    paginator_type: PaginatorType = "single_page"
    paginator_config: PaginatorConfig = {"type": paginator_type}

    client_config: ClientConfig = {
        "base_url": "https://rma.hontrack.com/api/external/",
        "auth": {
            "token": api_key,
        },
        "paginator": paginator_config,
    }

    config: RESTAPIConfig = {
        "client": client_config,
        "resource_defaults": {
            # "primary_key": "id",
            "write_disposition": write_disposition,
            "endpoint": {
                "response_actions": [
                    # ResponseActionDict(status_code=200, action=validate_response),
                    # {"status_code": 400, "action": "ignore"},
                    # {"status_code": 401, "action": "ignore"},
                    # {"status_code": 404, "action": "ignore"},
                    ResponseActionDict(action=validate_response),
                ],
            },
        },
        "resources": [
            {
                "name": "vehicles_resumen",
                "primary_key": ["plate", "date_apl"],
                "endpoint": {
                    "path": "vehicles/get_vehicles_resumen.php",
                    "method": "POST",
                    # "params": {},
                    "json": {
                        "from_date": start_date_str,  # "2024-10-29 00:00:00",
                        "to_date": end_date_str,  # "2024-11-01 23:59:59", # es inclusivo segun reunion
                        "api_key": api_key,
                    },
                    "data_selector": "payload.vehicles",
                },
            },
            {
                "name": "sensors_resumen",
                "primary_key": ["plate", "date_apl"],
                "endpoint": {
                    "path": "vehicles/get_sensors_resumen.php",
                    "method": "POST",
                    "json": {
                        "from_date": start_date_str,  # "2024-10-29 00:00:00",
                        "to_date": end_date_str,  # "2024-11-01 23:59:59", # es inclusivo segun reunion
                        "api_key": api_key,
                    },
                    "data_selector": "payload.vehicles",
                },
            },
            {
                "name": "drivers_resumen",
                "primary_key": ["code"],
                "endpoint": {
                    "path": "vehicles/get_drivers_resumen.php",
                    "method": "POST",
                    "json": {
                        "from_date": start_date_str,  # "2024-10-29 00:00:00",
                        "to_date": end_date_str,  # "2024-11-01 23:59:59", # es inclusivo segun reunion
                        "api_key": api_key,
                    },
                    "data_selector": "payload.drivers",
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

    def transform_common(resources: tuple) -> None:
        def transform_doc(doc: dict) -> dict:
            doc["enterprise_id"] = "farinter"
            doc["date_apl"] = pendulum.from_format(
                doc["date_apl"], "YYYY-MM-DD HH:mm:ss", tz=default_timezone_teg
            )

            # recursive data fields double dataype definition

            return doc

        for resource_name in resources:
            source.resources[resource_name].add_map(transform_doc, 1)

    transform_common(("vehicles_resumen", "sensors_resumen"))

    def transform_sensors_resumen(resource: DltResource) -> DltResource:
        def transform_doc(doc: dict) -> dict:
            def transform_data_to_decimals(data: dict) -> dict:
                if isinstance(data, dict):
                    for key, value in data.items():
                        if isinstance(value, dict):
                            data[key] = transform_data_to_decimals(value)
                        elif isinstance(value, list):
                            data[key] = [
                                transform_data_to_decimals(item)
                                if isinstance(item, dict)
                                else to_str_decimal(item)
                                for item in value
                            ]
                        else:
                            data[key] = to_str_decimal(value)
                elif isinstance(data, list):
                    data = [
                        transform_data_to_decimals(item)
                        if isinstance(item, dict)
                        else to_str_decimal(item)
                        for item in data
                    ]
                else:
                    data = to_str_decimal(data)
                return data

            transormed = transform_data_to_decimals(doc["data"])
            # print(transormed)
            doc["data"] = transormed
            return doc

        resource.add_map(transform_doc, 2)

        return resource

    source.resources["sensors_resumen"] = transform_sensors_resumen(
        source.resources["sensors_resumen"]
    )

    def transform_drivers_resumen(resource: DltResource) -> DltResource:
        def transform_doc(doc: dict) -> dict:
            # print(doc)
            # new_doc = dict(next(iter(doc.values()))) #el proveedor corrigio el API
            doc["enterprise_id"] = "farinter"
            for data in doc["data"]:
                data["fchapl"] = pendulum.from_format(
                    data["fchapl"], "YYYY-MM-DD HH:mm:ss", tz=default_timezone_teg
                )
                data["start_time"] = pendulum.from_format(
                    data["start_time"], "YYYY-MM-DD HH:mm:ss", tz=default_timezone_teg
                )
                data["end_time"] = pendulum.from_format(
                    data["end_time"], "YYYY-MM-DD HH:mm:ss", tz=default_timezone_teg
                )
                data["_dlt_id"] = (
                    f"{doc["code"]}_{data['fchapl'].strftime("%Y%m%d")}"
                )
                data["driver_code"] = doc["code"]
            return doc

        resource.add_map(transform_doc)

        return resource

    source.resources["drivers_resumen"] = transform_drivers_resumen(
        source.resources["drivers_resumen"]
    )

    return source


hontrack_api_pipeline = dlt_pipeline_dest_mssql_dwh.get_pipeline(
    "hontrack_api_pipeline", "hontrack_api"
)
# pipeline.dev_mode = True
# pipeline.refresh = "drop_sources"

daily_partitions_def = get_daily_partition_def_to_today(
    start_date=datetime(2024, 9, 1, 0, 0, 0),
)


def _daily_partition_iter(
    start_isodt: str, end_isodt: str
) -> Iterator[tuple[datetime, datetime]]:
    start = datetime.fromisoformat(start_isodt)
    end = datetime.fromisoformat(end_isodt) + timedelta(days=1)
    daily_diffs = int((end - start) / timedelta(days=1))
    for i in range(daily_diffs):
        yield (
            (start + timedelta(days=i)),
            (start + timedelta(days=i + 1) - timedelta(seconds=1)),
        )


@dlt_assets(
    dlt_source=hontrack_api_source().with_resources(
        "vehicles_resumen", "sensors_resumen", "drivers_resumen"
    ),
    dlt_pipeline=hontrack_api_pipeline,
    name="hontrack_api",
    group_name="hontrack_api",
    dagster_dlt_translator=MyDagsterDltTranslator(
        automation_condition=automation_daily_delta_2_cron,
        prefix_key=["DL_FARINTER", "hontrack_api"],
    ),
    partitions_def=daily_partitions_def,
)
def hontrack_api_assets_per_day(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
    dlt_pipeline_dest_mssql_dwh: DltPipelineDestMssqlDwh,
):
    new_pipeline = dlt_pipeline_dest_mssql_dwh.get_pipeline(
        "hontrack_api_pipeline", "hontrack_api"
    )
    new_pipeline.drop_pending_packages()
    first_partition, last_partition = context.partition_key_range
    partition_iter = _daily_partition_iter(first_partition, last_partition)
    context.log.info(f"date_from: {first_partition}, date_to: {last_partition}")
    context.log.info(
        f"write_disp: {dlt_pipeline_dest_mssql_dwh.write_disposition}, refresh: {dlt_pipeline_dest_mssql_dwh.refresh}"
    )

    def consolidar_resultados() -> Iterator[DltEventType]:
        first_iteration = True
        for start_of_day, end_of_day in partition_iter:
            context.log.info(
                f"run_date_from: {start_of_day.isoformat()}, run_date_to: {end_of_day.isoformat()}, date_from: {first_partition}, date_to: {last_partition}"
            )
            result = dlt.run(
                context=context,
                dlt_source=hontrack_api_source(
                    start_date=start_of_day,
                    end_date=end_of_day,
                ),
                dlt_pipeline=new_pipeline,
                write_disposition=dlt_pipeline_dest_mssql_dwh.write_disposition
                if first_iteration or dlt_pipeline_dest_mssql_dwh.write_disposition != "replace"
                else None,
                refresh=dlt_pipeline_dest_mssql_dwh.refresh
                if first_iteration
                else None,
            )
            first_iteration = False
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
            meta_1 = final.metadata if final.metadata else {}
            meta_2 = event.metadata if event.metadata else {}
            final_meta = merge_dlt_dagster_metadata(meta_1, meta_2)
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
        Definitions,
        define_asset_job,
        materialize,
        AssetKey,
        RunConfig,
    )

    with instance_for_test() as instance:
        ### test job parti
        test_job = define_asset_job("test_job", selection=(AssetKey(("DL_FARINTER", "hontrack_api", "sensors_resumen")),))
        test_resources = {
                "dlt": DagsterDltResource(),
                "dlt_pipeline_dest_mssql_dwh": dlt_pipeline_dest_mssql_dwh,
            }
        defs = Definitions(
            assets=[hontrack_api_assets_per_day],
            jobs=[test_job],
            resources=test_resources,
        )

        test_job_def = defs.get_job_def("test_job")
        result = test_job_def.execute_in_process(
            tags={
                "dagster/asset_partition_range_start": "2024-11-01",
                "dagster/asset_partition_range_end": "2024-11-02",
            },
            resources=test_resources,
            instance=instance,
            run_config=RunConfig(
                resources={
                    "dlt_pipeline_dest_mssql_dwh": {
                        "config": {
                            # "dev_mode": True,
                            "write_disposition": "replace",
                            # "refresh": "drop_resources",
                        }
                    }
                }
            ),
        )
        ## test single
        # defs = Definitions(
        #     assets=[hontrack_api_assets_per_day],
        #     resources={
        #         "dlt": DagsterDltResource(),
        #         "dlt_pipeline_dest_mssql_dwh": dlt_pipeline_dest_mssql_dwh,
        #     },
        # )
        # result = materialize(
        #     tuple(val for val in defs.get_repository_def().assets_defs_by_key.values()),
        #     instance=instance,
        #     # resources=defs.resources,
        #     partition_key="2024-11-18",
        #     selection=(AssetKey(("DL_FARINTER", "hontrack_api", "sensors_resumen")),),
        #     run_config=RunConfig(
        #         resources={
        #             "dlt_pipeline_dest_mssql_dwh": {
        #                 "config": {
        #                     # "dev_mode": True,
        #                     "write_disposition": "replace",
        #                     # "refresh": "drop_resources",
        #                 }
        #             }
        #         }
        #     ),
        # )

        print(f"Materialized:{
            [
                mat.step_materialization_data
                for mat in result.get_asset_materialization_events()
            ]
        }")
        ### test runs
        # hontrack_api_pipeline.drop_pending_packages()  # for dev only, to avoid conflicts in the test run
        # hontrack_api_pipeline.drop()
        # hontrack_api_pipeline.extract(
        #     data=hontrack_api_source().with_resources(
        #         "drivers_resumen",
        #     )
        # )
        # hontrack_api_pipeline.normalize()
        # print(hontrack_api_pipeline.schemas.list_schemas())
        # print(hontrack_api_pipeline.schemas["hontrack_api"].to_pretty_json())
