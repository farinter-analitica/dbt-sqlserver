import dlt
from dlt.common import pendulum
from dlt.common.pipeline import LoadInfo
from dlt.extract.resource import DltResource
import dlt.extract
from dlt.pipeline.pipeline import Pipeline
from dagster_shared_gf.dlt_shared.mongodb import mongodb, mongodb_collection
from dagster_shared_gf.shared_functions import get_for_current_env
from dagster_shared_gf.dlt_shared.dlt_resources import BaseDltPipeline
from dagster_shared_gf import shared_variables as shared_vars
from dagster import EnvVar, SourceAsset, asset, AssetExecutionContext, AssetsDefinition, AssetKey, MaterializeResult, MetadataValue
from dagster_embedded_elt.dlt import dlt_assets, DagsterDltResource, DagsterDltTranslator
from typing import Dict, List, Union, Iterable, Any, Mapping
from pydantic import dataclasses, Field
from dataclasses import asdict
from itertools import chain

env_str = shared_vars.env_str
# Set environment variables

connection_str_source:str = EnvVar("DAGSTER_SECRET_MONGODB_CRM_HN_CONN_URL").get_value()


@dataclasses.dataclass(frozen=True, config={"arbitrary_types_allowed": True})
class DltSourceConfig:
    primary_key: str | tuple
    pipeline_name: str
    cursor_path: str | None = None
    initial_value: pendulum.DateTime = Field(default_factory=lambda: get_for_current_env( {"dev": pendulum.now().subtract(years=2), "prd": pendulum.now().subtract(years=5) } ))
    dep: str | None = None

    def all_configs(self):
        dataclass_var_value_dict = asdict(self)
        return dataclass_var_value_dict


DltSourceConfigResourceList = Dict[DltSourceConfig, List[str]]

read_source_config_updated_at: DltSourceConfigResourceList = {
    DltSourceConfig(cursor_path="updated_at", primary_key="_id", pipeline_name="mongo_crm_hn_updated_at"): [
        "crm_email",
        #"crm_portfolio", # no tiene nada
        "crm_sms",
        #"crm_campaign_log", # no tiene nada
        "crm_incident", 
        "crm_call", 
        "constantcontactcampaigns",
    ],
    DltSourceConfig(cursor_path="UpdatedAt", primary_key="_id", pipeline_name="mongo_crm_hn_updatedat"): [
        "crmCall",
        "crm_list",
    ],
}

read_source_config_multi_column: DltSourceConfigResourceList = {
    DltSourceConfig(cursor_path="updated_at", primary_key="_id", pipeline_name="mongo_crm_hn_multi_updated_at", initial_value=pendulum.now().subtract(months=1)): [
        "crm_person",  ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
        "crm_message", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
        "crm_campaign", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
    ],
    DltSourceConfig(cursor_path="created_at", primary_key="_id", pipeline_name="mongo_crm_hn_multi_created_at", dep="mongo_crm_hn_multi_updated_at"): [
        "crm_person",  ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
        "crm_message", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
        "crm_campaign", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
    ],
    DltSourceConfig(cursor_path="EndDate", primary_key="_id", pipeline_name="mongo_crm_hn_multi_enddate", initial_value=pendulum.now().subtract(months=1)): [
        "campaignSchedule", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
    ],
    DltSourceConfig(cursor_path="createdDate", primary_key="_id", pipeline_name="mongo_crm_hn_multi_createddate", dep="mongo_crm_hn_multi_enddate"): [
        "campaignSchedule", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
    ],

}

read_source_config_not_incremental: DltSourceConfigResourceList = {
    DltSourceConfig(primary_key="_id", pipeline_name="mongo_crm_hn_not_incremental"): [
        "campaignActivity",
    ],
}

table_renames: Dict[str, str] = {
    "constantcontactcampaigns": "constant_contact_campaigns",
}

table_columns_select: Dict[str, List[str]] = {
    "crm_person": {
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
        "debt_collectors" : {"data_type": "complex"},
        "facebook_username": {"data_type": "text"},
        "facebook_id": {"data_type": "text"},
        "twitter_username": {"data_type": "text"},
        "twitter_id": {"data_type": "text"},
        "linkedin_username": {"data_type": "text"},
        "instagram_username": {"data_type": "text"},
        "fecha_ingreso": {"data_type": "timestamp"},
    }
    
}

table_limits: Dict[str, int] = {
    #"crm_person": 10000
}

all_mongo_db_source_configs: List[DltSourceConfigResourceList] = [read_source_config_updated_at, read_source_config_multi_column, read_source_config_not_incremental]


def get_config_filtered(dlt_source_config_resource_list: DltSourceConfigResourceList, dlt_source_config: DltSourceConfig) -> List[str]:
    return list(chain(dlt_source_config_resource_list[dlt_source_config]))

@dataclasses.dataclass
class DagsterDltTranslatorMongodbCRMHN(DagsterDltTranslator):
    config: DltSourceConfig
    def get_asset_key(self, resource: DltResource) -> AssetKey:
        """
        Para evitar duplicados en pipelines multi columnas de updated_at y created_at
        """
        return AssetKey([f"dlt_{self.config.pipeline_name}", f"{resource.name}"])          
    def get_deps_asset_keys(self, resource: DltResource) -> Iterable[AssetKey]:
        """
        Origen

        """
        return [AssetKey([f"mongo_db_crm_hn", f"{resource.name}"]) ]
    def get_metadata(self, resource: DltResource) -> Mapping[str, Any]:
        return self.config.all_configs() | resource.explicit_args | {"columns_schema": resource.columns}
    
    def get_config(self) -> DltSourceConfig:
        return self.config

def create_dlt_asset(dlt_source, 
                     group_name,
                    dlt_t: DagsterDltTranslatorMongodbCRMHN,
                    tags: Mapping[str, str],
                    dataset_name: str,
                    dep_pipeline: str | None = None) -> dlt_assets:

    if dep_pipeline is not None:
        dep_pipeline = [AssetKey([f"dlt_{dep_pipeline}", f"{dlt_source.name}"])]
    else:
        dep_pipeline = []

    @asset(
        key=dlt_t.get_asset_key(dlt_source),
        group_name=group_name,
        description=f"cursor {dlt_t.get_config().cursor_path} resource {dlt_t.get_asset_key(dlt_source)}",
        metadata=dlt_t.get_metadata(dlt_source),
        deps=list(dlt_t.get_deps_asset_keys(dlt_source)) + dep_pipeline,
        compute_kind="dlt",
        tags=tags,)
    def created_dlt_assets(context: AssetExecutionContext, dlt_pipeline_dest_mssql: BaseDltPipeline):
        context.log.info(f"Running dlt pipeline: {dlt_t.get_config().pipeline_name} resource {dlt_t.get_asset_key(dlt_source)} loading to dataset: {dataset_name}")
        new_pipeline = dlt_pipeline_dest_mssql.get_pipeline(pipeline_name=dlt_t.get_config().pipeline_name,
                                                            dataset_name=dataset_name)
        extracted_resource_metadata = dlt_pipeline_dest_mssql.run_pipeline(dlt_source, new_pipeline)
        return MaterializeResult(
            asset_key=dlt_t.get_asset_key(dlt_source),
            metadata=extracted_resource_metadata,
        )

    return created_dlt_assets

def dlt_mongo_db_crm_hn_asset_factory(mongo_db_source_configs: List[DltSourceConfigResourceList]) -> List[AssetsDefinition]:
    dlt_assets_list: List[AssetsDefinition] = []
    for dlt_source_config in mongo_db_source_configs:
        for config, collections in dlt_source_config.items():
            for collection in collections:
                resource: dlt.Resource = mongodb_collection(
                    connection_url=connection_str_source,
                    database="pro01",
                    collection=collection,
                    limit=table_limits.get(collection),
                    incremental=dlt.sources.incremental(config.cursor_path
                                                        ,primary_key=config.primary_key
                                                        , initial_value=config.initial_value),
                )

                if collection in table_renames:
                    resource.apply_hints(table_name=table_renames[collection])

                if collection in table_columns_select:
                    resource.apply_hints(columns=table_columns_select[collection])
                
                new_assets = create_dlt_asset(dlt_source=resource
                        ,group_name="dlt_mongo_db_crm_hn_etl_dwh"
                        ,dlt_t=DagsterDltTranslatorMongodbCRMHN(config=config)
                        ,dataset_name="mongo_db_crm_hn"
                        ,tags={"dagster/storage_kind": "sqlserver"}
                        ,dep_pipeline=config.dep
                        )
                dlt_assets_list.append(new_assets)

    return list(chain(dlt_assets_list))


all_mongo_db_hn_assets = dlt_mongo_db_crm_hn_asset_factory(all_mongo_db_source_configs)

all_mongo_db_hn_source_assets = list(
    SourceAsset(
        key,
        group_name="dlt_mongo_db_crm_hn_etl_dwh",
        tags={"dagster/storage_kind": "mongodb"},
    )
    for key in set(
        chain.from_iterable(
            dlt_assets.dependency_keys for dlt_assets in all_mongo_db_hn_assets
        )
    )
    if key not in set(asset.key for asset in all_mongo_db_hn_assets)
)
# print(mongodbdb_source_assets)

if __name__ == "__main__":
    pass
    # def load_select_collection_updated_at(pipeline: Pipeline|None = None) -> LoadInfo:
    #     """Use the mongodb source to reflect an entire database schema and load select tables from it.

    #     This example sources data from a sample mongo database data from [mongodb-sample-dataset](https://github.com/neelabalan/mongodb-sample-dataset).
    #     """
    #     dlt_source_config = DltSourceConfig(cursor_path="updated_at", primary_key="_id")
    #     if pipeline is None:
    #         # Create a pipeline
    #         pipeline = dlt.pipeline(
    #             pipeline_name="mongo_crm_hn_updated_at",
    #             destination=mssql_destination,
    #             dataset_name="mongo_crm_hn",
    #         )
    #     collections: List[str] = get_config_filtered(read_source_config_updated_at
    #                                                 ,dlt_source_config)
    #     # Configure the source to load a few select collections incrementally
    #     sources = mongodb(connection_url=connection_str_source
    #         ,database="pro01"
    #         ,incremental=dlt.sources.incremental(dlt_source_config.cursor_path
    #                                             ,primary_key=dlt_source_config.primary_key
    #                                             , initial_value=pendulum.now().subtract(years=2))
    #         ).with_resources(
    #         *collections
    #     )

    #     info = pipeline.run(sources, write_disposition="merge")

    #     return info


    # def load_select_collections_multi_updated_at(pipeline: Pipeline|None = None) -> LoadInfo:
    #     """Use the mongodb source to reflect an entire database schema and load select tables from it.

    #     This example sources data from a sample mongo database data from [mongodb-sample-dataset](https://github.com/neelabalan/mongodb-sample-dataset).
    #     """
    #     dlt_source_config = DltSourceConfig(cursor_path="updated_at", primary_key="_id")
    #     if pipeline is None:
    #         # Create a pipeline
    #         pipeline = dlt.pipeline(
    #             pipeline_name="mongo_crm_hn_multi_updated_at",
    #             destination=mssql_destination,
    #             dataset_name="mongo_crm_hn",
    #         )
    #     collections: List[str] = get_config_filtered(read_source_config_multi_column
    #                                                 ,dlt_source_config)

    #     # Configure the source to load a few select collections incrementally
    #     sources = mongodb(connection_url=connection_str_source
    #         ,database="pro01"
    #         ,incremental=dlt.sources.incremental(dlt_source_config.cursor_path
    #                                             ,primary_key=dlt_source_config.primary_key
    #                                             , initial_value=pendulum.now().subtract(months=2))
    #         ).with_resources(
    #         *collections
    #     )

    #     info = pipeline.run(sources, write_disposition="merge")

    #     return info

    # def load_select_collections_multi_created_at(pipeline: Pipeline|None = None) -> LoadInfo:
    #     """Use the mongodb source to reflect an entire database schema and load select tables from it.

    #     This example sources data from a sample mongo database data from [mongodb-sample-dataset](https://github.com/neelabalan/mongodb-sample-dataset).
    #     """
    #     dlt_source_config = DltSourceConfig(cursor_path="created_at", primary_key="_id")
    #     if pipeline is None:
    #         # Create a pipeline
    #         pipeline = dlt.pipeline(
    #             pipeline_name="mongo_crm_hn_multi_created_at",
    #             destination=mssql_destination,
    #             dataset_name="mongo_crm_hn",
    #             progress=dlt.progress.log(log_period=1,)
    #         )
    #     collections: List[str] = get_config_filtered(read_source_config_multi_column
    #                                                 ,dlt_source_config)

    #     # Configure the source to load a few select collections incrementally
    #     sources = mongodb(connection_url=connection_str_source
    #         ,database="pro01"
    #         ,incremental=dlt.sources.incremental(dlt_source_config.cursor_path
    #                                             ,primary_key=dlt_source_config.primary_key
    #                                             , initial_value=pendulum.now().subtract(months=2)
                                                
    #                                             )
    #         ).with_resources(
    #         *collections
    #     )

    #     info = pipeline.run(sources, write_disposition="merge")

    #     return info

    # print(load_select_collections_multi_created_at())
