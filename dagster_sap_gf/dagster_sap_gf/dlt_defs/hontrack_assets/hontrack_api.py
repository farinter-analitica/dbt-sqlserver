from collections import deque
from datetime import datetime, timedelta
import decimal
import json
import math
from typing import Any, Iterator, Optional
import hashlib
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
from dagster_dlt.dlt_event_iterator import DltEventIterator, DltEventType
from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import (
    SinglePagePaginator,
    RangePaginator,
)
from dlt.common import jsonpath

from dlt.common.schema.typing import TWriteDisposition
from dlt.sources.rest_api.typing import (
    RESTAPIConfig,
    PaginatorConfig,
    ClientConfig,
    PaginatorType,
    ResponseActionDict,
)
from dlt.extract import DltResource

from dagster_shared_gf.automation import automation_daily_delta_2_cron
from dagster_shared_gf.dlt_shared.dlt_resources import (
    MyDagsterDltTranslator,
    dlt_pipeline_dest_mssql_dwh,
    DltPipelineDestMssqlDwh,
)
from dagster_shared_gf.shared_variables import default_timezone_teg
from dagster_shared_gf.partitions.time_based import get_daily_partition_def_to_today
import pendulum
from requests import Request, Response


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


def hash_sha256_from_str(str: str) -> str:
    return hashlib.sha256(str.encode()).hexdigest()


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


class JsonPageFromRegsPaginator(RangePaginator):
    """A paginator that uses page number-based pagination strategy.

    For example, consider an API located at `https://api.example.com/items`
    that supports pagination through page number and page size query parameters,
    and provides the total number of pages in its responses, as shown below:

        {
            "items": [...],
            "total_regs": 10
        }

    To use `PageNumberPaginator` with such an API, you can instantiate `RESTClient`
    as follows:

        from dlt.sources.helpers.rest_client import RESTClient

        client = RESTClient(
            base_url="https://api.example.com",
            paginator=PageNumberPaginator(
                total_path="total_regs"
            )
        )

        @dlt.resource
        def get_items():
            for page in client.paginate("/items", params={"size": 100}):
                yield page

    Note that we pass the `size` parameter in the initial request to the API.
    The `PageNumberPaginator` will automatically increment the page number for
    each subsequent request until all items are fetched.

    If the API does not provide the total number of pages, you can use the
    `maximum_page` parameter to limit the number of pages to fetch. For example:

        client = RESTClient(
            base_url="https://api.example.com",
            paginator=PageNumberPaginator(
                maximum_page=5,
                total_path=None
            )
        )
        ...

    In this case, pagination will stop after fetching 5 pages of data.
    """

    def __init__(
        self,
        base_page: int = 0,
        page: Optional[int] = None,
        page_json_param: str = "page",
        regs_per_page: int = 10,
        total_regs_path: jsonpath.TJsonPath = "total_regs",
        maximum_page: Optional[int] = None,
        stop_after_empty_page: Optional[bool] = True,
    ):
        """
        Args:
            base_page (int): The index of the first page from the API perspective.
                Determines the page number that the API server uses for the first
                page. Normally, this is 0-based or 1-based (e.g., 1, 2, 3, ...)
                indexing for the pages. Defaults to 0.
            page (int): The page number for the first request. If not provided,
                the initial value will be set to `base_page`.
            page_json_param (str): The query parameter name for the page number.
                Defaults to 'page'.
            regs_per_page (int): The number of items per page. Defaults to 10.
            total_regs_path (jsonpath.TJsonPath): The JSONPath expression for
                the total number of pages. Defaults to 'total_regs'.
            maximum_page (int): The maximum page number. If provided, pagination
                will stop once this page is reached or exceeded, even if more
                data is available. This allows you to limit the maximum number
                of pages for pagination. Defaults to None.
            stop_after_empty_page (bool): Whether pagination should stop when
              a page contains no result items. Defaults to `True`.
        """
        if (
            total_regs_path is None
            and maximum_page is None
            and not stop_after_empty_page
        ):
            raise ValueError(
                "Either `total_regs_path` or `maximum_page` must be provided, or `stop_after_empty_page` must be set to `True`."
            )
        page = page if page is not None else base_page

        self.regs_per_page = regs_per_page
        self.total_pages = None
        super().__init__(
            param_name=page_json_param,
            initial_value=page,
            base_index=base_page,
            total_path=total_regs_path,
            value_step=1,
            maximum_value=maximum_page,
            error_message_items="pages",
            stop_after_empty_page=stop_after_empty_page,
        )

    def init_request(self, request: Request) -> None:
        self._has_next_page = True
        self.current_value = self.initial_value
        if request.json is None:
            request.json = {}

        request.json[self.param_name] = self.current_value

    def __str__(self) -> str:
        return (
            super().__str__()
            + f": current page: {self.current_value} page_param: {self.param_name} total_path:"
            f" {self.total_path} maximum_value: {self.maximum_value}"
        )

    def update_state(
        self, response: Response, data: Optional[list[Any]] = None
    ) -> None:
        if self._stop_after_this_page(data):
            self._has_next_page = False
        else:
            total = None
            if self.total_pages is None and self.total_path:
                response_json = response.json()
                values = jsonpath.find_values(self.total_path, response_json)
                total = values[0] if values else None
                if total is None:
                    self._handle_missing_total(response_json)
                else:
                    try:
                        self.total_pages = math.ceil(int(total) / self.regs_per_page)
                    except ValueError:
                        self._handle_invalid_total(total)

            self.current_value += self.value_step

            total = self.total_pages

            if (
                total is not None and self.current_value >= total + self.base_index
            ) or (
                self.maximum_value is not None
                and self.current_value >= self.maximum_value
            ):
                self._has_next_page = False

    def update_request(self, request: Request) -> None:
        if request.json is None:
            request.json = {}
        request.json[self.param_name] = self.current_value


@dlt.source(root_key=True)
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

    hontrack_client_pages = RESTClient(
        base_url=client_config["base_url"],
        paginator=JsonPageFromRegsPaginator(
            base_page=0, total_regs_path="payload.total_regs", regs_per_page=10
        ),
    )

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
            # {
            #     "name": "drivers_resumen",
            #     "primary_key": ["code"],
            #     "endpoint": {
            #         "path": "vehicles/get_drivers_resumen.php",
            #         "method": "POST",
            #         "json": {
            #             "from_date": start_date_str,  # "2024-10-29 00:00:00",
            #             "to_date": end_date_str,  # "2024-11-01 23:59:59", # es inclusivo segun reunion
            #             "api_key": api_key,
            #         },
            #         "data_selector": "payload.drivers",
            #     },
            # },
            {
                "name": "zones_resumen",
                "primary_key": ["evtdid"],
                "endpoint": {
                    "path": "zones/get_zones_resumen.php",
                    "method": "POST",
                    "json": {
                        "from_date": start_date_str,  # "2024-10-29 00:00:00",
                        "to_date": end_date_str,  # "2024-11-01 23:59:59", # es inclusivo segun reunion
                        "api_key": api_key,
                    },
                    "data_selector": "payload.zones",
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

    def transform_sensors_resumen(resource: DltResource) -> None:
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

    transform_sensors_resumen(source.resources["sensors_resumen"])

    def transform_zones_resumen(resource: DltResource) -> None:
        def transform_doc(doc: dict) -> dict:
            doc["enterprise_id"] = "farinter"
            for data in doc["data"]:
                data["evtdfch"] = pendulum.from_format(
                    data["evtdfch"], "YYYY-MM-DD HH:mm:ss", tz=default_timezone_teg
                )
                data["_dlt_id"] = str(
                    hashlib.md5(
                        f"{doc["evtdid"]}_{data["plate"]}_{data['evtdfch'].strftime("%Y%m%d%H%M%S")}".encode()
                    ).hexdigest()
                )
            return doc

        resource.add_map(transform_doc)

    transform_zones_resumen(source.resources["zones_resumen"])

    @dlt.resource(
        name="drivers_resumen",
        primary_key=["code"],
    )
    def drivers_resumen():
        for page in hontrack_client_pages.paginate(
            path="vehicles/get_drivers_resumen.php",
            method="POST",
            json={
                "from_date": start_date_str,  # "2024-10-29 00:00:00",
                "to_date": end_date_str,  # "2024-11-01 23:59:59", # es inclusivo segun reunion
                "api_key": api_key,
            },
            data_selector="payload.drivers",
        ):
            yield page

    source.resources.add(drivers_resumen)

    def transform_drivers_resumen(resource: DltResource) -> None:
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
                data["_dlt_id"] = f"{doc["code"]}_{data['fchapl'].strftime("%Y%m%d")}"
                data["driver_code"] = doc["code"]
                for vehicle in data.get("vehicles", []):
                    vehicle["end_time"] = pendulum.from_format(
                        vehicle["end_time"],
                        "YYYY-MM-DD HH:mm:ss",
                        tz=default_timezone_teg,
                    )
                    vehicle["_dlt_id"] = hash_sha256_from_str(
                        f"{doc["code"]}_{vehicle['end_time'].strftime("%Y%m%d%H%M%S")}_{vehicle['plate']}"
                    )
                    vehicle["_dlt_parent_id"] = data["_dlt_id"]
                    for session in vehicle.get("Sessions", []):
                        session["end_time"] = pendulum.from_format(
                            session["end_time"],
                            "YYYY-MM-DD HH:mm:ss",
                            tz=default_timezone_teg,
                        )
                        session["start_time"] = pendulum.from_format(
                            session["start_time"],
                            "YYYY-MM-DD HH:mm:ss",
                            tz=default_timezone_teg,
                        )
                        session["_dlt_id"] = hash_sha256_from_str(
                            f"{doc['code']}_{session['end_time'].strftime('%Y%m%d%H%M%S')}_{vehicle['plate']}"
                        )
                        session["_dlt_parent_id"] = vehicle["_dlt_id"]

            return doc

        resource.add_map(transform_doc)

    transform_drivers_resumen(source.resources["drivers_resumen"])

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
        "vehicles_resumen", "sensors_resumen", "drivers_resumen", "zones_resumen"
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
    first_partition, last_partition = (
        context.partition_key_range
        if context.has_partition_key_range or context.has_partition_key
        else (
            daily_partitions_def.get_last_partition_key(),
            daily_partitions_def.get_last_partition_key(),
        )
    )
    if first_partition is None or last_partition is None:
        raise ValueError(
            "Partition range not found, try a partitioned run."
        )

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
                if first_iteration
                or dlt_pipeline_dest_mssql_dwh.write_disposition != "replace"
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
        test_job = define_asset_job(
            "test_job",
            selection=(AssetKey(("DL_FARINTER", "hontrack_api", "drivers_resumen")),),
        )
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
                "dagster/asset_partition_range_start": "2024-09-01",
                "dagster/asset_partition_range_end": "2024-12-03",
            },
            resources=test_resources,
            instance=instance,
            run_config=RunConfig(
                resources={
                    "dlt_pipeline_dest_mssql_dwh": {
                        "config": {
                            # "dev_mode": True,
                            # "write_disposition": "replace",
                            # "refresh": "drop_resources",
                        }
                    }
                }
            ),
        )
        # # test single
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
        #     #partition_key="2024-11-18",
        #     selection=(AssetKey(("DL_FARINTER", "hontrack_api", "drivers_resumen")),),
        #     run_config=RunConfig(
        #         resources={
        #             "dlt_pipeline_dest_mssql_dwh": {
        #                 "config": {
        #                     # "dev_mode": True,
        #                     # "write_disposition": "replace",
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
