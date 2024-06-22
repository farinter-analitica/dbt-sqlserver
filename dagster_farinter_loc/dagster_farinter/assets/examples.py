from dagster import asset, MaterializeResult, MetadataValue, AssetMaterialization, Output, op, Out, In, AssetIn
from dagster_farinter.resources.sql_server_resources import SQLServerResource
import pandas as pd  



@asset
def fetch_data_from_sql(dwh_farinter_adm: SQLServerResource):
    result = dwh_farinter_adm.query("SELECT TOP 10 prueba, int FROM tb_pruebas")
    #print(result)
    return result
    #df = pd.DataFrame(result)
    #return df
    # MaterializeResult sirve mejor para enviar logs y no datos al siguiente asset
    # return MaterializeResult(
    #     metadata={
    #         "num_records": len(result),  # Metadata can be any key-value pair
    #         "preview": MetadataValue.md(df.head().to_markdown()),
    #         # The `MetadataValue` class has useful static methods to build Metadata
    #     }
    # )

@asset(deps=[fetch_data_from_sql]) #Marca una dependencia sin importar los datos
def insert_data_into_sql(dwh_farinter_adm: SQLServerResource) -> None:
    dwh_farinter_adm.execute_and_commit("INSERT INTO tb_pruebas (prueba, int) VALUES ('value1', 1)")
    return None

@asset
def insert_data_into_sql_op(context, fetch_data_from_sql, dwh_farinter_adm: SQLServerResource) -> None: #Marca una dependencia debido a movimiento de datos
    data = fetch_data_from_sql
    for row in data:
        print(row)
        dwh_farinter_adm.execute_and_commit(f"INSERT INTO tb_pruebas (prueba, int) VALUES ('{row[0]}', 2)")
    return None

@asset(deps=[insert_data_into_sql_op,insert_data_into_sql])
def fetch_data_from_sql_final(dwh_farinter_adm: SQLServerResource) -> MaterializeResult:
    result = dwh_farinter_adm.query("SELECT TOP 10 * FROM tb_pruebas")
    df = pd.DataFrame(result)
    return MaterializeResult(
        metadata={
            "num_records": len(result),  # Metadata can be any key-value pair
            "preview": MetadataValue.md(df.head().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )
