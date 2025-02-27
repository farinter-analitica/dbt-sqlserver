import requests
import polars as pl
from datetime import datetime, timedelta
import json
from deep_translator import GoogleTranslator
from dagster import (
    asset,
    AssetChecksDefinition,
    AssetExecutionContext,
    load_asset_checks_from_current_module,
    load_assets_from_current_module,
    AutomationCondition,
)
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.shared_variables import tags_repo

# from translate import Translator
from typing import Sequence


# Función para obtener los días festivos para una lista de países y un rango de fechas específico
def obtener_dias_festivos(codigos_pais, fecha_inicio, fecha_fin) -> pl.DataFrame:
    datos_festivos = []

    # Inicializar el traductor a español
    # traductor = Translator(to_lang="es")
    traductor = GoogleTranslator(target="es", source="en")

    for codigo_pais in codigos_pais:
        # URL base para la API de Calendarific
        url_base = r"https://calendarific.com/api/v2/holidays"
        # Parámetros para la solicitud a la API
        parametros = {
            "api_key": "xHkJmJwBCYlgoRD5GYF1yZNyFd7C0sDb",  # Reemplazar con tu clave de API
            "country": codigo_pais,
            "year": datetime.now().year,
            "start": fecha_inicio,
            "end": fecha_fin,
            "type": "national",  # Filtrar por días festivos nacionales
        }

        # Realizar la solicitud a la API
        respuesta = requests.get(url_base, params=parametros)

        # Verificar si la solicitud fue exitosa
        if respuesta.status_code == 200:
            dias_festivos = respuesta.json().get("response", {}).get("holidays", [])

            # Filtrar los días festivos por el rango de fechas especificado
            for dia_festivo in dias_festivos:
                fecha_dia_festivo = dia_festivo["date"]["iso"]
                if fecha_inicio <= fecha_dia_festivo <= fecha_fin:
                    # Traducir el nombre del día festivo al español
                    motivo_es = traductor.translate(dia_festivo["name"])

                    datos_festivos.append(
                        {
                            "Fecha_Id": fecha_dia_festivo,
                            "Motivo": motivo_es,
                            "Json_Sociedades_Id": '["*"]',  # Marcador de posición ya que no se proporciona información de sociedades
                            "Json_Paises_Id": json.dumps([codigo_pais]),
                        }
                    )
        else:
            print(
                f"Error al obtener datos para {codigo_pais}: {respuesta.status_code} {respuesta.text}"
            )
            raise Exception(
                f"Error al obtener datos para {codigo_pais}: {respuesta.status_code} {respuesta.text}"
            )

    # Convertir a DataFrame de Polars para manejo eficiente
    df_festivos = pl.DataFrame(datos_festivos)

    # Asegurar que la fecha sea única, combinando los países en una lista y asegurando el motivo en español
    df_festivos = df_festivos.group_by("Fecha_Id").agg(
        [
            pl.col("Motivo").first().alias("Motivo"),
            pl.col("Json_Sociedades_Id").first().alias("Json_Sociedades_Id"),
            pl.col("Json_Paises_Id")
            .map_elements(
                lambda x: json.dumps(
                    list({pais for lista in x for pais in json.loads(lista)})
                )
            )
            .alias("Json_Paises_Id"),
        ]
    )

    return df_festivos

    # URL base para la API de Nager.Date (muy incompleta y desactualizada)
    # url_base = f"https://date.nager.at/api/v3/publicholidays/{datetime.now().year}/{codigo_pais}"
    # Realizar la solicitud a la API
    # respuesta = requests.get(url_base)


# Ejemplo de uso


# Escribir en base de datos
@asset(
    key_prefix=["DL_FARINTER", "web_api"],
    tags=tags_repo.SmbDataRepository.tag
    | tags_repo.AutomationOnly
    | {"dagster/storage_kind": "sqlserver", "data_source_kind": "web_api"},
    compute_kind="polars",
    automation_condition=AutomationCondition.on_cron(
        "@monthly"
    ),  # solo funciona bien sin upstreams ojo
)
def DL_Edit_CalendarioNoLaboral_Temp(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
):
    table = "DL_Edit_CalendarioNoLaboral_Temp"
    database = "DL_FARINTER"
    schema = "web_api"

    codigos_pais = ["HN", "NI", "CR", "SV", "GT"]  # Lista de códigos ISO2 de los países
    fecha_inicio = datetime.now().strftime("%Y-%m-%d")  # Formato YYYY-MM-DD
    fecha_fin = (datetime.now() + timedelta(days=365)).strftime(
        "%Y-%m-%d"
    )  # Formato YYYY-MM-DD
    df_festivos = obtener_dias_festivos(codigos_pais, fecha_inicio, fecha_fin)
    # Cargar los datos en la base de datos
    with dwh_farinter_dl.get_connection(engine="sqlalchemy") as conn:
        df_festivos.write_database(
            table_name=f"{schema}.{table}", connection=conn, if_table_exists="replace"
        )
    context.log.info(
        f"Guardado {df_festivos.shape[0]} registros en la tabla {schema}.{table}"
    )

    # Ejemplo: Guardar el DataFrame en un archivo CSV que pueda cargarse en la tabla SQL
    # df_festivos.write_csv("DL_Edit_CalendarioNoLaboral.csv")


QUERY_TEMP_A_FINAL = """;
MERGE INTO [dbo].[DL_Edit_CalendarioNoLaboral] AS TARGET
USING (
SELECT [Fecha_Id]
    ,[Motivo]
    ,[Json_Sociedades_Id]
    ,[Json_Paises_Id]
FROM [DL_FARINTER].[web_api].[DL_Edit_CalendarioNoLaboral_Temp]
--ORDER BY Fecha_Id
) AS SOURCE
ON TARGET.[Fecha_Id] = SOURCE.[Fecha_Id]
WHEN MATCHED 
AND TARGET.[fue_automatico] =1 AND ISNULL(TARGET.[bloquear_modificacion],0)=0 THEN
    UPDATE 
    SET
    TARGET.[Motivo] = SOURCE.[Motivo]
        , TARGET.[Json_Paises_Id] = SOURCE.[Json_Paises_Id]
        , TARGET.[fecha_actualizado] = GETDATE()
        , TARGET.[fue_automatico] = 1
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([Fecha_Id],[Motivo],[Json_Sociedades_Id],[Json_Paises_Id],[fecha_actualizado],[fue_automatico])
    VALUES
        (SOURCE.[Fecha_Id],SOURCE.[Motivo],SOURCE.[Json_Sociedades_Id],SOURCE.[Json_Paises_Id],GETDATE(),1)
WHEN NOT MATCHED BY SOURCE 
AND TARGET.[fue_automatico] =1 AND ISNULL(TARGET.[bloquear_modificacion],0)=0 
AND TARGET.[Fecha_Id]>GETDATE() AND TARGET.[Fecha_Id]<(SELECT MAX(Fecha_Id) FROM [DL_FARINTER].[web_api].[DL_Edit_CalendarioNoLaboral_Temp])
THEN
    DELETE
;
            """

QUERY_ANIO_SIGUIENTE = """;
INSERT INTO [DL_FARINTER].[dbo].[DL_Edit_CalendarioNoLaboral] 
(
[Fecha_Id]
    ,[Motivo]
    ,[Json_Sociedades_Id]
    ,[Json_Paises_Id]
    ,[fue_automatico]
    ,[bloquear_modificacion]
    ,[fecha_actualizado]
    ,[json_paises_fijo_anual]
)
SELECT DATEADD(YEAR,1,CALN.[Fecha_Id]) AS [Fecha_Id]
    ,CALN.[Motivo]
    ,CALN.[Json_Sociedades_Id]
    ,CALN.[json_paises_fijo_anual] [Json_Paises_Id]
    ,1 as [fue_automatico]
    ,CALN.[bloquear_modificacion]
    ,getdate() [fecha_actualizado]
    ,CALN.[json_paises_fijo_anual]
FROM [DL_FARINTER].[dbo].[DL_Edit_CalendarioNoLaboral] CALN
LEFT JOIN [DL_FARINTER].[dbo].[DL_Edit_CalendarioNoLaboral] CAL
ON DATEADD(YEAR,1,CALN.[Fecha_Id])=CAL.Fecha_Id
WHERE CAL.Fecha_Id IS NULL
AND YEAR(CALN.[Fecha_Id]) = YEAR(GETDATE())
AND CALN.json_paises_fijo_anual IS NOT NULL AND ISJSON(CALN.json_paises_fijo_anual)=1 AND CALN.json_paises_fijo_anual<>'[]'                                          
;
        """


@asset(
    key_prefix=["DL_FARINTER", "dbo"],
    tags=tags_repo.SmbDataRepository.tag
    | tags_repo.AutomationOnly
    | {"dagster/storage_kind": "sqlserver", "data_source_kind": "web_api"},
    compute_kind="sqlserver",
    automation_condition=AutomationCondition.cron_tick_passed("@monthly")
    | AutomationCondition.eager(),
    deps=[DL_Edit_CalendarioNoLaboral_Temp],
)
def DL_Edit_CalendarioNoLaboral(
    context: AssetExecutionContext, dwh_farinter_dl: SQLServerResource
):
    with dwh_farinter_dl.get_connection(engine="sqlalchemy") as conn:
        dwh_farinter_dl.execute_and_commit(QUERY_TEMP_A_FINAL, connection=conn)

    with dwh_farinter_dl.get_connection(engine="sqlalchemy") as conn:
        dwh_farinter_dl.execute_and_commit(QUERY_ANIO_SIGUIENTE, connection=conn)


all_assets = tuple(load_assets_from_current_module(group_name="web_api_externo"))

all_asset_checks: Sequence[AssetChecksDefinition] = tuple(
    load_asset_checks_from_current_module()
)

if __name__ == "__main__":
    codigos_pais = ["HN", "NI", "CR", "SV", "GT"]  # Lista de códigos ISO2 de los países
    fecha_inicio = (datetime.now().replace(day=1, month=1)).strftime(
        "%Y-%m-%d"
    )  # Formato YYYY-MM-DD
    fecha_fin = (datetime.now() + timedelta(days=365)).strftime(
        "%Y-%m-%d"
    )  # Formato YYYY-MM-DD

    # Obtener y mostrar los días festivos
    df_festivos = obtener_dias_festivos(codigos_pais, fecha_inicio, fecha_fin)
    if df_festivos is not None:
        print(df_festivos)
