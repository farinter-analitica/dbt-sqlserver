import dlt
from dlt.common import pendulum
from dlt.common.pipeline import LoadInfo
from dlt.extract.resource import DltResource
import dlt.extract
from dlt.common.normalizers.naming.snake_case import NamingConvention
from dlt.pipeline.pipeline import Pipeline
from dagster_shared_gf.dlt_shared.mongodb import mongodb, mongodb_collection
from dagster_shared_gf.dlt_shared.dlt_resources import BaseDltPipeline
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_functions import (
    get_for_current_env,
    get_unique_hash_sha2_256,
    filter_assets_by_tags,
)
from datetime import timedelta
from dagster_shared_gf.shared_variables import env_str, TagsRepositoryGF as tags_repo
from dagster import (
    EnvVar,
    SourceAsset,
    asset,
    AssetExecutionContext,
    AssetsDefinition,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    build_last_update_freshness_checks,
    load_asset_checks_from_current_module,
    AssetChecksDefinition,
)
from dagster_embedded_elt.dlt import (
    dlt_assets,
    DagsterDltResource,
    DagsterDltTranslator,
)
from typing import Dict, List, Union, Iterable, Any, Mapping, Sequence
from pydantic import dataclasses, Field
from dataclasses import asdict
from itertools import chain

connection_str_source:str = EnvVar("DAGSTER_SECRET_MONGODB_CRM_HN_CONN_URL").get_value()
snake_case_normalizer = NamingConvention()


@dataclasses.dataclass(frozen=True, config={"arbitrary_types_allowed": True})
class DltSourceConfig:
    primary_key: str | tuple
    pipeline_name_prefix: str
    cursor_path: str | None = None
    initial_value: pendulum.DateTime = Field(default_factory=lambda: get_for_current_env( {"dev": pendulum.now().subtract(years=2), "prd": pendulum.now().subtract(years=5) } ))
    dep: str | None = None

    def all_configs(self):
        dataclass_var_value_dict = asdict(self)
        return dataclass_var_value_dict


DltSourceConfigResourceList = Dict[DltSourceConfig, List[str]]

read_source_config_updated_at: DltSourceConfigResourceList = {
    DltSourceConfig(cursor_path="updated_at", primary_key="_id", pipeline_name_prefix="mongo_crm_hn_updated_at"): [
        "crm_email",
        #"crm_portfolio", # no tiene nada
        "crm_sms",
        #"crm_campaign_log", # no tiene nada
        "crm_incident", 
        "crm_call", 
        "constantcontactcampaigns",
    ],
    DltSourceConfig(cursor_path="updatedAt", primary_key="_id", pipeline_name_prefix="mongo_crm_hn_updatedat"): [
        "crm_list",
    ],
}

read_source_config_multi_column: DltSourceConfigResourceList = {
    DltSourceConfig(cursor_path="updated_at", primary_key="_id", pipeline_name_prefix="mongo_crm_hn_multi_updated_at", initial_value=pendulum.now().subtract(months=1)): [
        "crm_person",  ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
        "crm_message", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
        "crm_campaign", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
    ],
    DltSourceConfig(cursor_path="created_at", primary_key="_id", pipeline_name_prefix="mongo_crm_hn_multi_created_at", dep="mongo_crm_hn_multi_updated_at"): [
        "crm_person",  ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
        "crm_message", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
        "crm_campaign", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
    ],
    DltSourceConfig(cursor_path="EndDate", primary_key="_id", pipeline_name_prefix="mongo_crm_hn_multi_enddate", initial_value=pendulum.now().subtract(months=1)): [
        "campaignSchedule", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
    ],
    DltSourceConfig(cursor_path="createdDate", primary_key="_id", pipeline_name_prefix="mongo_crm_hn_multi_createddate", dep="mongo_crm_hn_multi_enddate"): [
        "campaignSchedule", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
    ],
    DltSourceConfig(cursor_path="updatedAt", primary_key="_id", pipeline_name_prefix="mongo_crm_hn_multi_updatedat", initial_value=pendulum.now().subtract(months=1)): [
        "dataViewList", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
    ],
    DltSourceConfig(cursor_path="creationDate", primary_key="_id", pipeline_name_prefix="mongo_crm_hn_multi_creationdate", dep="mongo_crm_hn_multi_updatedat"): [
        "dataViewList", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
    ],
    DltSourceConfig(cursor_path="UpdatedAt", primary_key="_id", pipeline_name_prefix="mongo_crm_hn_multi_updatedat", initial_value=pendulum.now().subtract(months=1)): [
        "crmCall", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
    ],
    DltSourceConfig(cursor_path="createdAt", primary_key="_id", pipeline_name_prefix="mongo_crm_hn_multi_createdat", dep="mongo_crm_hn_multi_updatedat"): [
        "crmCall", ##pedir que actualicen de ser necesario para que funcione con updated_at en todos los docs
    ],
}

read_source_config_not_incremental: DltSourceConfigResourceList = {
    DltSourceConfig(primary_key="_id", pipeline_name_prefix="mongo_crm_hn_not_incremental"): [
        "campaignActivity",
    ],
}

table_renames: Dict[str, str] = {
    "constantcontactcampaigns": "constant_contact_campaigns",
    "crmCall": "crm_call",
}

table_columns_hints: Dict[str, List[str]] = {
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
    },
# $._id.$oid	_id_oid	String
# $.campaignId	campaignId	Integer
# $.campaign	campaign	String
# $.campaignType	campaignType	String
# $.callerId	caller_id	Integer
# $.caller	caller	String
# $.clientId	client_idcrm	Long
# $.clientCode	client_code	String
# $.clientName	clientName	String
# $.clientNumber	clientNumber	String
# $.clientBusinessName	client_business_name	String
# $.calledAt.$date	called_at_date	String
# $.createdAt.$date	created_at_date	String
# $.updatedAt.$date	updated_at_date	String
# $.answeredAt.$date	answered_at_date	String
# $.hangedUpAt.$date	hangedup_at_date	String
# $.notes	notes	String
# $.action	action	String
# $.cantidad	cantidad	Integer
# $.codigoPedido	codigoPedido	String
# $.clientId	callee_id	Long
# $.type	type	String


    "crmCall": {
        "clientNumber": {"data_type": "text"},
        "callerId": {"data_type": "bigint"},
        "clientId": {"data_type": "bigint", "name": "callee_id"},

    }, 

# $._id.$oid	_id_oid	String
# $.id	id	Integer
# $.class_name	class_name	String
# $.created_by_id	created_by_id	Integer
# $.updated_by_id	updated_by_id	Integer
# $.caller_id	caller_id	Integer
# $.caller_cid	caller_cid	String
# $.callee_id	client_idcrm	Long
# $.callee_cid	callee_cid	String
# $.incident	incident	Integer
# $.response	response	String
# $.way	way	String
# $.incident_action_type	incident_action_type	String
# $.created_at.$date	created_at_date	String
# $.updated_at.$date	updated_at_date	String
# $.answered_at.$date	answered_at_date	String
# $.hanged_up_at.$date	hangedup_at_date	String
# $.notes	notes	String
# $.action	action	String
# $.callee_id	callee_id	Long
# $.type	type	String

    "crm_call": {
        "caller_id" : {"data_type": "bigint"},
        "callee_id" : {"data_type": "bigint"},     
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
        return AssetKey([f"dlt_{self.config.pipeline_name_prefix}", f"{resource.name}"])          
    def get_deps_asset_keys(self, resource: DltResource) -> Iterable[AssetKey]:
        """
        Origen

        """
        return [AssetKey([f"mongo_db_crm_hn", f"{resource.name}"]) ]
    def get_metadata(self, resource: DltResource) -> Mapping[str, Any]:
        return self.config.all_configs() | resource.explicit_args | {"columns_schema": resource.columns}
    
    def get_config(self) -> DltSourceConfig:
        return self.config
    
    def get_normalized_table_identifier(self,resource: DltResource) -> str:
        return snake_case_normalizer.normalize_table_identifier(resource.name)
    
    def get_normalized_column_identifier(self, column_identifier: str) -> str:
        return snake_case_normalizer.normalize_identifier(column_identifier)
    
    def get_pipeline_name(self, resource: DltResource) -> str:
        return f"{env_str}_{self.config.pipeline_name_prefix}_{self.get_normalized_table_identifier(resource)}"

def create_dlt_asset(dlt_resource: DltResource, 
                     group_name,
                    dlt_t: DagsterDltTranslatorMongodbCRMHN,
                    tags: Mapping[str, str],
                    dataset_name: str,
                    dep_asset_pipeline: str | None = None) -> dlt_assets:

    if dep_asset_pipeline is not None:
        dep_asset_pipeline = [AssetKey([f"dlt_{dep_asset_pipeline}", f"{dlt_resource.name}"])]
    else:
        dep_asset_pipeline = []

    target_table_identifier = dlt_t.get_normalized_table_identifier(dlt_resource)
    target_pipeline_name = dlt_t.get_pipeline_name(dlt_resource)

    @asset(
        key=dlt_t.get_asset_key(dlt_resource),
        group_name=group_name,
        description=f"cursor {dlt_t.get_config().cursor_path} resource {dlt_t.get_asset_key(dlt_resource)}",
        metadata=dlt_t.get_metadata(dlt_resource),
        deps=list(dlt_t.get_deps_asset_keys(dlt_resource)) + dep_asset_pipeline,
        compute_kind="dlt",
        tags=tags,)
    def created_dlt_assets(context: AssetExecutionContext, dlt_pipeline_dest_mssql: BaseDltPipeline):
        context.log.info({
                            "Running dlt pipeline" : target_pipeline_name,
                            "resource":  dlt_t.get_asset_key(dlt_resource) ,
                            "dataset": dataset_name,
                            "write_disposition": dlt_pipeline_dest_mssql.write_disposition
                            }
                         )
        new_pipeline = dlt_pipeline_dest_mssql.get_pipeline(pipeline_name=target_pipeline_name,
                                                            dataset_name=dataset_name)
        #is_first_run = new_pipeline.first_run
        load_info: LoadInfo = dlt_pipeline_dest_mssql.run_pipeline(dlt_resource, new_pipeline)
        load_info.raise_on_failed_jobs()
    
        extracted_resource_metadata = dlt_pipeline_dest_mssql.extract_resource_metadata(dlt_resource, load_info)
        # loaded_schema = load_info.pipeline.schemas.get("mongodb").get_table(dlt_resource.name).get("columns") #no se optiene la tabla normalizada
        # context.log.info(f"schema: {loaded_schema}")        
        #agregar llave indice de llave primaria y de _dlt_id
        # primary_key_columns_list = [column for column, config in loaded_schema.items() if config.get("primary_key")==True]
        # primary_key_columns = ', '.join(primary_key_columns_list)
        # primary_key_columns_hash_name = get_unique_hash_sha2_256(primary_key_columns_list)
        # context.log.info(f"primary_keys: {primary_key_columns} -> hash_name: {primary_key_columns_hash_name}")
        # with dwh_farinter_dl.get_pyodbc_conn() as conn:
        #     query_obj = f"SELECT name FROM sys.objects WHERE name = '{dlt_resource.name}' AND schema_id = SCHEMA_ID('{dataset_name}')"
        #     obj_result = dwh_farinter_dl.query(query_obj, conn)
        #     if obj_result and obj_result[0][0]:
        #         query_pk = f"""SELECT name 
        #         FROM sys.indexes 
        #         WHERE name = 'PK_{dlt_resource.name}_{primary_key_columns_hash_name}' AND object_id = OBJECT_ID('{dataset_name}.{dlt_resource.name}')"""
        #         pk_result = dwh_farinter_dl.query(query_pk, conn)
        #         if not pk_result or not pk_result[0][0]:
        #             dwh_farinter_dl.execute_and_commit(f"CREATE INDEX [PK_{dlt_resource.name}_{primary_key_columns_hash_name}] ON {dataset_name}.{dlt_resource.name} ({primary_key_columns})", conn)
                # query_ix = f"""SELECT name 
                # FROM sys.indexes 
                # WHERE name = 'IX_{dlt_resource.name}_dlt_id' AND object_id = OBJECT_ID('{dataset_name}.{dlt_resource.name}')"""
                # ix_result = dwh_farinter_dl.query(query_ix, conn)
                # if not ix_result or not ix_result[0][0]:
                #     dwh_farinter_dl.execute_and_commit(f"CREATE INDEX IX_{dlt_resource.name}_dlt_id ON {dataset_name}.{dlt_resource.name} ([_dlt_id])", conn)

        return MaterializeResult(
            asset_key=dlt_t.get_asset_key(dlt_resource),
            metadata=extracted_resource_metadata,
        )

    return created_dlt_assets

def dlt_mongo_db_crm_hn_asset_factory(mongo_db_source_configs: List[DltSourceConfigResourceList]) -> List[AssetsDefinition]:
    dlt_assets_list: List[AssetsDefinition] = []
    for dlt_source_config in mongo_db_source_configs:
        for config, collections in dlt_source_config.items():
            for collection in collections:
                resource: DltResource = mongodb_collection(
                    connection_url=connection_str_source,
                    database="pro01",
                    collection=collection,
                    limit=table_limits.get(collection),
                    incremental=dlt.sources.incremental(cursor_path=config.cursor_path
                                                        ,primary_key=config.primary_key
                                                        , initial_value=config.initial_value),
                    parallel=True,
                    #data_item_format="arrow", #aparentemente no con esta combinacion de source / destino
                )

                if collection in table_renames:
                    resource.apply_hints(table_name=table_renames[collection])

                if collection in table_columns_hints:
                    resource.apply_hints(columns=table_columns_hints[collection])

                if isinstance(config.primary_key,str):
                    resource.apply_hints(columns={config.primary_key : {"data_type": "text", "precision": 50}})
                
                new_assets = create_dlt_asset(dlt_resource=resource
                        ,group_name="dlt_mongo_db_crm_hn_etl_dwh"
                        ,dlt_t=DagsterDltTranslatorMongodbCRMHN(config=config)
                        ,dataset_name="mongo_db_crm_hn"
                        ,tags={"dagster/storage_kind": "sqlserver"}
                        ,dep_asset_pipeline=config.dep
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

all_assets = all_mongo_db_hn_assets

all_assets_non_hourly_freshness_checks = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="exclude_if_any_tag"),
    lower_bound_delta=timedelta(hours=26),
    deadline_cron="0 9 * * 1-6",
)
# print(filter_assets_by_tags(all_assets, tags=hourly_tag, filter_type="any_tag_matches"), "\n")
all_assets_hourly_freshness_checks: Sequence[AssetChecksDefinition] = build_last_update_freshness_checks(
    assets=filter_assets_by_tags(all_assets, tags_to_match=tags_repo.Hourly.tag, filter_type="any_tag_matches"),
    lower_bound_delta=timedelta(hours=13),
    deadline_cron="0 10-16 * * 1-6",
)

all_assets = all_assets + all_mongo_db_hn_source_assets
all_asset_checks: Sequence[AssetChecksDefinition] = load_asset_checks_from_current_module()
all_asset_freshness_checks = all_assets_non_hourly_freshness_checks + all_assets_hourly_freshness_checks

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
