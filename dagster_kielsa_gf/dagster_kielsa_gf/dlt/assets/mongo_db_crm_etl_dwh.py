from collections import deque
from datetime import timedelta
from itertools import chain
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

from dagster_shared_gf.dlt_shared.mongodb.helpers import max_dt_with_lag_last_value_func
import dlt
import dlt.extract
from dagster import (
    AssetChecksDefinition,
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    AutomationCondition,
    EnvVar,
    MaterializeResult,
    MetadataValue,
    SourceAsset,
    asset,
    build_last_update_freshness_checks,
    instance_for_test,
    load_asset_checks_from_current_module,
    materialize,
)
from dagster_embedded_elt.dlt import (
    DagsterDltTranslator,
)
from dlt.common import pendulum
from dlt.common.normalizers.naming.snake_case import NamingConvention
from dlt.common.pipeline import LoadInfo
from dlt.extract.resource import DltResource
from pydantic import Field, dataclasses

from dagster_shared_gf.automation import automation_hourly_cron_prd
from dagster_shared_gf.dlt_shared.dlt_resources import BaseDltPipeline
from dagster_shared_gf.dlt_shared.mongodb import mongodb_collection
from dagster_shared_gf.shared_functions import (
    filter_assets_by_tags,
    get_for_current_env,
)
from dagster_shared_gf.shared_variables import tags_repo

dlt.secrets["connection_str_source"] = EnvVar(
    "DAGSTER_SECRET_MONGODB_CRM_HN_CONN_URL"
).get_value()
snake_case_normalizer = NamingConvention()


def default_date_fn():
    pendulum_dt = get_for_current_env(
        {
            "dev": pendulum.now().subtract(years=2),
            "prd": pendulum.now().subtract(years=5),
        }
    )
    return pendulum_dt


@dataclasses.dataclass(frozen=True, config={"arbitrary_types_allowed": True})
class DltResourceCollection:
    collection_name: str
    primary_key: Optional[str | tuple] = None
    table_new_name: Optional[str] = None
    columns_hints: Optional[dict[str, Any]] = None
    columns_to_remove: Optional[tuple[str, ...]] = None
    limit: Optional[int] = None
    cursor_path: Optional[str] = None
    initial_value: Optional[pendulum.DateTime] = None
    automation_condition: Optional[AutomationCondition] = None

    def get_all_configs_dict(self) -> Mapping[str, Any]:
        return {key: value for key, value in self.__dict__.items() if value is not None}


@dataclasses.dataclass(frozen=True, config={"arbitrary_types_allowed": True})
class DltPipelineSourceConfig:
    pipeline_base_name: str
    primary_key: str | tuple
    cursor_path: Optional[str] = None
    initial_value: pendulum.DateTime = Field(default_factory=default_date_fn)
    collections: tuple[DltResourceCollection, ...] = Field(default_factory=tuple)
    dep_pipeline_base_name: Optional[str] = None

    def __post_init__(self):
        for collection in self.collections:
            if collection.primary_key is None:
                object.__setattr__(collection, "primary_key", self.primary_key)
            if collection.cursor_path is None:
                object.__setattr__(collection, "cursor_path", self.cursor_path)
            if collection.initial_value is None:
                object.__setattr__(collection, "initial_value", self.initial_value)

    def get_all_configs_dict(self):
        return {key: value for key, value in self.__dict__.items() if value is not None}


DLTRCol = DltResourceCollection

DltPipelineSourceConfigResourceTuple = tuple[DltPipelineSourceConfig, ...]

read_source_config_updated_at: DltPipelineSourceConfigResourceTuple = (
    DltPipelineSourceConfig(
        cursor_path="updated_at",
        primary_key="_id",
        pipeline_base_name="mongo_crm_hn_updated_at",
        collections=(
            DLTRCol(
                collection_name="crm_email",
            ),
            DLTRCol(
                collection_name="crm_sms",
            ),
            DLTRCol(
                collection_name="crm_incident",
            ),
            DLTRCol(
                collection_name="crm_call",
                columns_hints={
                    "caller_id": {"data_type": "bigint"},
                    "callee_id": {"data_type": "bigint"},
                },
            ),
            DLTRCol(
                collection_name="constantcontactcampaigns",
                table_new_name="constant_contact_campaigns",
            ),
            DLTRCol(
                collection_name="campaignsRecetas",
                columns_to_remove=("created_at",),
                cursor_path="updatedAt",
                automation_condition=automation_hourly_cron_prd,
            ),
            DLTRCol(
                collection_name="clientToCall",
                cursor_path="updatedAt",
                automation_condition=automation_hourly_cron_prd,
            ),
        ),
    ),
    DltPipelineSourceConfig(
        cursor_path="updatedAt",
        primary_key="_id",
        pipeline_base_name="mongo_crm_hn_updatedat",
        collections=(
            DLTRCol(
                collection_name="crm_list",
            ),
        ),
    ),
)

dltrccol_crm_person = DLTRCol(
    collection_name="crm_person",
    columns_hints={
        "id": {"data_type": "bigint"},
        "treatment": {"data_type": "text"},
        "first_name": {"data_type": "text"},
        "middle_name": {"data_type": "text"},
        "last_name": {"data_type": "text"},
        "second_last_name": {"data_type": "text"},
        "email": {"data_type": "text"},
        "dni": {"data_type": "text"},
        "phone_number": {"data_type": "text"},
        "landline": {"data_type": "text"},
        "address": {"data_type": "text"},
        "gender": {"data_type": "text"},
        "code": {"data_type": "text"},
        "sap_company_name": {"data_type": "text"},
        "country_code": {"data_type": "text"},
        "country_name": {"data_type": "text"},
        "department_code": {"data_type": "text"},
        "department_name": {"data_type": "text"},
        "municipality_code": {"data_type": "text"},
        "town_code": {"data_type": "text"},
        "neighborhood_code": {"data_type": "text"},
        "class_name": {"data_type": "text"},
        "business_name": {"data_type": "text"},
        "category_client": {"data_type": "text"},
        "company_id": {"data_type": "bigint"},
        "notes": {"data_type": "text"},
        "sap_company_code": {"data_type": "text"},
        "sap_zone_code": {"data_type": "text"},
        "sap_zone_name": {"data_type": "text"},
        "_id": {"data_type": "text"},
        "birth_date": {"data_type": "date"},
        "created_at": {"data_type": "timestamp"},
        "updated_at": {"data_type": "timestamp"},
        "sap_username": {"data_type": "text"},
        "user_id": {"data_type": "bigint"},
        "sapusercode": {"data_type": "text"},
        "number": {"data_type": "text"},
        "sellers": {"data_type": "complex"},
        "seller_id": {"data_type": "text"},
        "custom_fields": {"data_type": "complex"},
        "debt_collectors": {"data_type": "complex"},
        "facebook_username": {"data_type": "text"},
        "facebook_id": {"data_type": "text"},
        "twitter_username": {"data_type": "text"},
        "twitter_id": {"data_type": "text"},
        "linkedin_username": {"data_type": "text"},
        "instagram_username": {"data_type": "text"},
        "fecha_ingreso": {"data_type": "timestamp"},
    },
    # limit=1000,
)

read_source_config_multi_column: DltPipelineSourceConfigResourceTuple = (
    DltPipelineSourceConfig(
        cursor_path="updated_at",
        primary_key="_id",
        pipeline_base_name="mongo_crm_hn_multi_updated_at",
        initial_value=pendulum.now().subtract(months=1),
        collections=(
            dltrccol_crm_person,
            DLTRCol(collection_name="crm_message"),
            DLTRCol(collection_name="crm_campaign"),
        ),
    ),
    DltPipelineSourceConfig(
        cursor_path="created_at",
        primary_key="_id",
        pipeline_base_name="mongo_crm_hn_multi_created_at",
        dep_pipeline_base_name="mongo_crm_hn_multi_updated_at",
        collections=(
            dltrccol_crm_person,
            DLTRCol(collection_name="crm_message"),
            DLTRCol(collection_name="crm_campaign"),
        ),
    ),
    DltPipelineSourceConfig(
        cursor_path="EndDate",
        primary_key="_id",
        pipeline_base_name="mongo_crm_hn_multi_enddate",
        initial_value=pendulum.now().subtract(months=1),
        collections=(DLTRCol(collection_name="campaignSchedule"),),
    ),
    DltPipelineSourceConfig(
        cursor_path="createdDate",
        primary_key="_id",
        pipeline_base_name="mongo_crm_hn_multi_createddate",
        dep_pipeline_base_name="mongo_crm_hn_multi_enddate",
        collections=(DLTRCol(collection_name="campaignSchedule"),),
    ),
    DltPipelineSourceConfig(
        cursor_path="updatedAt",
        primary_key="_id",
        pipeline_base_name="mongo_crm_hn_multi_updatedat",
        initial_value=pendulum.now().subtract(months=1),
        collections=(DLTRCol(collection_name="dataViewList"),),
    ),
    DltPipelineSourceConfig(
        cursor_path="creationDate",
        primary_key="_id",
        pipeline_base_name="mongo_crm_hn_multi_creationdate",
        dep_pipeline_base_name="mongo_crm_hn_multi_updatedat",
        collections=(DLTRCol(collection_name="dataViewList"),),
    ),
    DltPipelineSourceConfig(
        cursor_path="UpdatedAt",
        primary_key="_id",
        pipeline_base_name="mongo_crm_hn_multi_updatedat",
        initial_value=pendulum.now().subtract(months=1),
        collections=(
            DLTRCol(
                collection_name="crmCall",
                table_new_name="crm_call",
                columns_hints={
                    "clientNumber": {"data_type": "text"},
                    "callerId": {"data_type": "bigint"},
                    "clientId": {"data_type": "bigint", "name": "callee_id"},
                },
            ),
        ),
    ),
    DltPipelineSourceConfig(
        cursor_path="createdAt",
        primary_key="_id",
        pipeline_base_name="mongo_crm_hn_multi_createdat",
        dep_pipeline_base_name="mongo_crm_hn_multi_updatedat",
        collections=(
            DLTRCol(
                collection_name="crmCall",
                table_new_name="crm_call",
                columns_hints={
                    "clientNumber": {"data_type": "text"},
                    "callerId": {"data_type": "bigint"},
                    "clientId": {"data_type": "bigint", "name": "callee_id"},
                },
            ),
        ),
    ),
)


read_source_config_not_incremental: DltPipelineSourceConfigResourceTuple = (
    DltPipelineSourceConfig(
        primary_key="_id",
        pipeline_base_name="mongo_crm_hn_not_incremental",
        collections=(DLTRCol(collection_name="campaignActivity"),),
    ),
)


all_mongo_db_source_configs: DltPipelineSourceConfigResourceTuple = (
    read_source_config_updated_at
    + read_source_config_multi_column
    + read_source_config_not_incremental
)


# def get_config_filtered(
#     dlt_source_config_resource_list: DltPipelineSourceConfigResourceTuple,
#     dlt_source_config: DltPipelineSourceConfig,
# ) -> tuple[str]:
#     return list(chain(dlt_source_config_resource_tuple[dlt_source_config]))


@dataclasses.dataclass
class DagsterDltTranslatorMongodbCRMHN(DagsterDltTranslator):
    config: DltPipelineSourceConfig
    collection: DLTRCol

    def get_asset_key(self, resource: DltResource) -> AssetKey:
        """
        Para evitar duplicados en pipelines multi columnas de updated_at y created_at
        """
        return AssetKey([f"dlt_{self.config.pipeline_base_name}", f"{resource.name}"])

    def get_deps_asset_keys(self, resource: DltResource) -> Iterable[AssetKey]:
        """
        Origen

        """
        return [AssetKey(["mongo_db_crm_hn", f"{resource.name}"])]

    def get_collection_metadata(self, resource: DltResource) -> Mapping[str, Any]:
        collection_dict = self.collection.get_all_configs_dict()
        collection_meta = {}
        if collection_dict:
            collection_meta = {
                key: MetadataValue.text(str(value))
                for key, value in collection_dict.items()
            }
        return (
            collection_meta
            | resource.explicit_args  # type: ignore
            | {"columns_schema": resource.columns}
        )

    def get_normalized_table_identifier(self, resource: DltResource) -> str:
        return snake_case_normalizer.normalize_table_identifier(resource.name)

    def get_normalized_column_identifier(self, column_identifier: str) -> str:
        return snake_case_normalizer.normalize_identifier(column_identifier)

    def get_pipeline_name(self, resource: DltResource) -> str:
        return f"{self.config.pipeline_base_name}_{self.get_normalized_table_identifier(resource)}"

    def remove_columns(
        self, doc: Dict, remove_columns: Optional[Sequence[str]] = None
    ) -> Dict:
        """
        Removes the specified columns from the given document.

        Args:
            doc (Dict): The document from which columns are to be removed.
            remove_columns (Optional[tuple[str]], optional): The list of column names to be removed. Defaults to None.

        Returns:
            Dict: The modified document with the specified columns removed.
        """
        if remove_columns is None:
            remove_columns = []

        for column_name in remove_columns:
            if column_name in doc:
                del doc[column_name]

        return doc


def create_dlt_asset(
    dlt_resource: DltResource,
    group_name,
    dlt_t: DagsterDltTranslatorMongodbCRMHN,
    tags: Mapping[str, str],
    dataset_name: str,
    dep_asset_pipeline: str | None = None,
) -> AssetsDefinition:
    if dep_asset_pipeline is not None:
        dep_asset_pipeline_ak = [
            AssetKey([f"dlt_{dep_asset_pipeline}", f"{dlt_resource.name}"])
        ]
    else:
        dep_asset_pipeline_ak = []

    # target_table_identifier = dlt_t.get_normalized_table_identifier(dlt_resource)
    target_pipeline_name = dlt_t.get_pipeline_name(dlt_resource)

    @asset(
        key=dlt_t.get_asset_key(dlt_resource),
        group_name=group_name,
        description=f"cursor {dlt_t.collection.cursor_path} resource {dlt_t.get_asset_key(dlt_resource)}",
        metadata=dlt_t.get_collection_metadata(dlt_resource),
        deps=list(dlt_t.get_deps_asset_keys(dlt_resource)) + dep_asset_pipeline_ak,
        compute_kind="dlt",
        tags=tags,
        automation_condition=dlt_t.collection.automation_condition,
    )
    def created_dlt_assets(
        context: AssetExecutionContext, dlt_pipeline_dest_mssql_dwh: BaseDltPipeline
    ):
        new_pipeline = dlt_pipeline_dest_mssql_dwh.get_pipeline(
            pipeline_name=target_pipeline_name, dataset_name=dataset_name
        )
        context.log.info(
            {
                "Running dlt pipeline": target_pipeline_name,
                "resource": dlt_t.get_asset_key(dlt_resource),
                "dataset": dataset_name,
                "write_disposition": dlt_pipeline_dest_mssql_dwh.write_disposition,
                "directory": new_pipeline.pipelines_dir,
            }
        )
        # is_first_run = new_pipeline.first_run
        load_info: LoadInfo = dlt_pipeline_dest_mssql_dwh.run_pipeline(
            dlt_resource, new_pipeline
        )
        load_info.raise_on_failed_jobs()

        extracted_resource_metadata = {"error": "LoadInfo vacía."}
        if load_info:
            extracted_resource_metadata = (
                dlt_pipeline_dest_mssql_dwh.extract_resource_metadata(
                    context, dlt_resource, load_info, new_pipeline
                )
            )

        return MaterializeResult(
            asset_key=dlt_t.get_asset_key(dlt_resource),
            metadata=extracted_resource_metadata,
        )

    return created_dlt_assets


def dlt_mongo_db_crm_hn_asset_factory(
    mongo_db_source_configs: DltPipelineSourceConfigResourceTuple,
) -> tuple[AssetsDefinition, ...]:
    dlt_assets_list: deque[AssetsDefinition] = deque()
    for dlt_source_config in mongo_db_source_configs:
        for collection in dlt_source_config.collections:
            # if env_str == "local":
            #     print(f"Processing collection: {collection.collection_name}")
            resource: DltResource = mongodb_collection(
                connection_url=dlt.secrets["connection_str_source"],
                database="pro01",
                collection=collection.collection_name,
                limit=collection.limit,
                incremental=dlt.sources.incremental(
                    cursor_path=collection.cursor_path,  # type: ignore
                    primary_key=collection.primary_key,
                    initial_value=collection.initial_value,
                    last_value_func=max_dt_with_lag_last_value_func,
                ),
                parallel=True,
                # data_item_format="arrow", #aparentemente no con esta combinacion de source / destino
            )
            dlt_t = DagsterDltTranslatorMongodbCRMHN(
                config=dlt_source_config, collection=collection
            )
            if collection.table_new_name:
                resource = resource.apply_hints(table_name=collection.table_new_name)

            if collection.columns_hints:
                resource = resource.apply_hints(columns=collection.columns_hints)

            if collection.columns_to_remove:
                resource = resource.add_map(
                    lambda doc,
                    columns=collection.columns_to_remove: dlt_t.remove_columns(
                        doc=doc, remove_columns=columns
                    )
                )

            if isinstance(collection.primary_key, str):
                resource = resource.apply_hints(
                    columns={
                        collection.primary_key: {"data_type": "text", "precision": 50}
                    }
                )
            elif isinstance(collection.primary_key, tuple):
                resource = resource.apply_hints(
                    columns={
                        key: {"data_type": "text", "precision": 50}
                        for key in collection.primary_key
                    }
                )

            new_assets = create_dlt_asset(
                dlt_resource=resource,
                group_name="dlt_mongo_db_crm_hn_etl_dwh",
                dlt_t=dlt_t,
                dataset_name="mongo_db_crm_hn",
                tags={"dagster/storage_kind": "sqlserver"},
                dep_asset_pipeline=dlt_source_config.dep_pipeline_base_name,
            )
            dlt_assets_list.append(new_assets)

    return tuple(chain(dlt_assets_list))


all_mongo_db_hn_assets = dlt_mongo_db_crm_hn_asset_factory(all_mongo_db_source_configs)

all_mongo_db_hn_source_assets = list(
    SourceAsset(
        key,
        group_name="dlt_mongo_db_crm_hn_etl_dwh",
        tags={"dagster/storage_kind": "mongodb"},
    )
    for key in set(
        chain.from_iterable(
            dlt_assets_.dependency_keys for dlt_assets_ in all_mongo_db_hn_assets
        )
    )
    if key not in set(asset.key for asset in all_mongo_db_hn_assets)
)

all_assets = all_mongo_db_hn_assets

all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(
        all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="exclude_if_any_tag"
    ),
    lower_bound_delta=timedelta(hours=26),
    deadline_cron="0 9 * * 1-6",
)
# print(filter_assets_by_tags(all_assets, tags=hourly_tag, filter_type="any_tag_matches"), "\n")
all_assets_hourly_freshness_checks: Sequence[AssetChecksDefinition] = (
    build_last_update_freshness_checks(
        assets=filter_assets_by_tags(
            all_assets,
            tags_to_match=tags_repo.Hourly.tag,
            filter_type="any_tag_matches",
        ),
        lower_bound_delta=timedelta(hours=13),
        deadline_cron="0 10-16 * * 1-6",
    )
)

all_assets = (*all_assets, *all_mongo_db_hn_source_assets) 
all_asset_checks: Sequence[AssetChecksDefinition] = (
    load_asset_checks_from_current_module()
)
all_asset_freshness_checks = (
    *all_assets_non_hourly_freshness_checks,
    *all_assets_hourly_freshness_checks,
)

if __name__ == "__main__":

    def test_all_assets_loaded():
        configured_total = 0
        for Source in all_mongo_db_source_configs:
            configured_total += len(Source.collections)

        loaded_total = len(all_assets)
        assert (
            configured_total == loaded_total
        ), f"Expected {configured_total} assets, but loaded {loaded_total} assets"

    from dagster_shared_gf.dlt_shared.dlt_resources import dlt_pipeline_dest_mssql_dwh

    with instance_for_test() as instance:
        asset_to_test = tuple(
            asset
            for asset in all_assets
            if asset.key == AssetKey(("dlt_mongo_crm_hn_updated_at", "clientToCall"))
        )
        result = materialize(
            asset_to_test,
            instance=instance,
            resources={"dlt_pipeline_dest_mssql_dwh": dlt_pipeline_dest_mssql_dwh},
        )

        # print(
        #     [mat.step_materialization_data for mat in result.get_asset_materialization_events()]
        # )

