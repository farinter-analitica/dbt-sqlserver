from dagster import (
    sensor,
    RunRequest,
    DagsterRunStatus,
    DefaultSensorStatus,
    SensorDefinition,
    AutoMaterializeSensorDefinition,
    build_sensor_for_freshness_checks,
    SensorEvaluationContext,
    SkipReason
)
import dagster_kielsa_gf.jobs as jobs, json
from dagster_kielsa_gf.assets import (
    dbt_dwh_kielsa,
    knime_asset_factory,
    ldcom_etl_dwh,
    recetas_libros_etl_dwh,
)
from dagster_shared_gf.shared_functions import (
    get_all_instances_of_class,
    get_for_current_env,
)
from dagster_shared_gf import shared_variables as shared_vars
from dagster_shared_gf.resources.correo_e import EmailSenderResource
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
env_str:str=shared_vars.env_str

default_timezone: str = "America/Tegucigalpa"
running_default_sensor_status: DefaultSensorStatus = get_for_current_env({"local":DefaultSensorStatus.STOPPED
                                                                              ,"dev":DefaultSensorStatus.RUNNING
                                                                              ,"prd":DefaultSensorStatus.RUNNING})
stopped_default_sensor_status: DefaultSensorStatus = get_for_current_env({"local":DefaultSensorStatus.STOPPED
                                                                              ,"dev":DefaultSensorStatus.STOPPED
                                                                              ,"prd":DefaultSensorStatus.STOPPED})
@sensor(job=jobs.dbt_dwh_kielsa_marts_job, default_status=stopped_default_sensor_status)
def upstream_completion_sensor(context):
    # Check for the most recent successful run of the upstream job
    last_run = context.instance.get_runs(
        filters={"job_name": "upstream_job", "status": DagsterRunStatus.SUCCESS},
        limit=1,
    )
    if last_run:
        # Trigger the downstream job
        yield RunRequest(run_key=None)

from dagster import op, OpExecutionContext
from typing import Dict, Any
from datetime import datetime, timedelta
from dateutil import parser

class ParServidoresReplicaSQLServer:
    def __init__(
        self,
        sql_server_origen: SQLServerResource,
        sql_server_replica: SQLServerResource,
        relation_origen: str,
        relation_replica: str,
        column_origen: str,
        column_replica: str,
        delta_max: timedelta = timedelta(hours=1),
    ) -> None:
        self.sql_server_origen = sql_server_origen
        self.sql_server_replica = sql_server_replica
        self.relation_origen = relation_origen
        self.relation_replica = relation_replica
        self.column_origen = column_origen
        self.column_replica = column_replica
        self.delta_max = delta_max


def conseguir_max_datetime_val_replicas_sql_server(
    sql_server_origen: SQLServerResource,
    sql_server_replica: SQLServerResource,
    relation_origen: str,
    relation_replica: str,
    column_origen: str,
    column_replica: str,
) -> tuple[Any, Any]:
    max_val_origen = None
    max_val_replica = None
    with sql_server_origen.get_connection() as conn_a:
        results_origen = sql_server_origen.query(
            query=f"SELECT MAX({column_origen}) FROM {relation_origen} WITH (NOLOCK)",
            connection=conn_a,
        )
        if results_origen and results_origen[0][0]:
            max_val_origen = results_origen[0][0]

    with sql_server_replica.get_connection() as conn_b:
        results_replica = sql_server_replica.query(
            query=f"SELECT MAX({column_replica}) FROM {relation_replica} WITH (NOLOCK)",
            connection=conn_b,
        )
        if results_replica and results_replica[0][0]:
            max_val_replica = results_replica[0][0]

   # print(f"servirdor_origen: {sql_server_origen.server}, servidor_replica: {sql_server_replica.server}, {sql_server_replica.__repr_name__}, max_val_origen: {max_val_origen}, max_val_replica: {max_val_replica}")

    return max_val_origen, max_val_replica


def es_delta_max_datetime_superado(val_referencia: datetime, val_comprobado: datetime, delta_max: timedelta = timedelta(hours=1)) -> bool:
    if val_referencia is None and val_comprobado is None:
        ValueError("Ambos valores son None")
    if val_comprobado is None:
        return True
    return (val_referencia - val_comprobado) > delta_max


def obtener_servidores_de_replica_en_alerta(lista_par_servidores: list[ParServidoresReplicaSQLServer]) -> Dict[str, list[Dict[str, str]]]:
    servidores_en_alerta: Dict[str, list[Dict[str, str]]] = {}
    servidores_sin_alerta: Dict[str, list[Dict[str, str]]] = {}
    for par_servidores in lista_par_servidores:
        # max_val_origen, max_val_replica = conseguir_max_datetime_val_replicas_sql_server(par_servidores.sql_server_origen,
        #                                                                                   par_servidores.sql_server_replica,
        #                                                                                   par_servidores.relation_origen,
        #                                                                                   par_servidores.relation_replica,
        #                                                                                   par_servidores.column_origen,
        #                                                                                   par_servidores.column_replica
        #                                                                                   )
        max_val_origen, max_val_replica = datetime.now(), datetime.now()-timedelta(hours=3)

        if not isinstance(max_val_origen, datetime):
            max_val_origen = parser.parse(max_val_origen)
        if not isinstance(max_val_replica, datetime):
            max_val_replica = parser.parse(max_val_replica)

        meta_data = {"servidor_origen": par_servidores.sql_server_origen.server, 
                     "servidor_replica": par_servidores.sql_server_replica.server,
                     "relation_origen": par_servidores.relation_origen,
                     "relation_replica": par_servidores.relation_replica,
                     "max_val_origen": max_val_origen.strftime("%Y-%m-%d %H:%M:%S"),
                     "max_val_replica": max_val_replica.strftime("%Y-%m-%d %H:%M:%S"),
                 }
        if es_delta_max_datetime_superado(max_val_origen, max_val_replica):
            if par_servidores.sql_server_replica.server not in servidores_en_alerta:
                servidores_en_alerta[par_servidores.sql_server_replica.server] = []
            servidores_en_alerta[par_servidores.sql_server_replica.server].append(meta_data)
        else:
            if par_servidores.sql_server_replica.server not in servidores_sin_alerta:
                servidores_sin_alerta[par_servidores.sql_server_replica.server] = []
            servidores_sin_alerta[par_servidores.sql_server_replica.server].append(meta_data)

    #print(servidores_en_alerta)
    #print(servidores_sin_alerta)

    return servidores_en_alerta

EMAIL_BODY = \
"""
Los siguientes servidores de SQL Server tienen un delta de control de replicación superado:

{json_data}
"""

@sensor(minimum_interval_seconds=3600, default_status=DefaultSensorStatus.STOPPED)
def alerta_correo_replicas_ldcom_sensor(context: SensorEvaluationContext,
                                         enviador_correo_e_analitica_farinter: EmailSenderResource,
                                         dwh_farinter_dl: SQLServerResource,
                                         dwh_farinter_dl_prd: SQLServerResource):


    LISTA_PAR_SERVIDORES: list[ParServidoresReplicaSQLServer] = [
        ParServidoresReplicaSQLServer(sql_server_origen=dwh_farinter_dl, 
                                    sql_server_replica=dwh_farinter_dl_prd, 
                                    relation_origen="DL_FARINTER.dbo.DL_Kielsa_FacturaEncabezado", 
                                    relation_replica="DL_FARINTER.dbo.DL_Kielsa_FacturaEncabezado",
                                    column_origen="Factura_Fecha",
                                    column_replica="Factura_Fecha",
                                    delta_max=timedelta(hours=1)
                                    ),
    ]

    #context.log.info(f"Lista de par de servidores: {str(LISTA_PAR_SERVIDORES)}")
    servidores_en_alerta = obtener_servidores_de_replica_en_alerta(LISTA_PAR_SERVIDORES)
    context.log.info(f"Servidores en alerta: {json.dumps(servidores_en_alerta)}")
    if len(servidores_en_alerta) > 0: 
        email_sender = enviador_correo_e_analitica_farinter
        email_subject = "Delta de valor superado por la replica"
        email_body = EMAIL_BODY.format(json_data=json.dumps(servidores_en_alerta, indent=3))
        email_to = ["brian.padilla@farinter.com"]  # Replace with actual recipients

        email_sender.send_email(email_to, email_subject, email_body)
        return SkipReason(skip_message="Correo enviado.")
    
    return SkipReason(skip_message="Correo no enviado.")


from dagster import AssetSelection, AutoMaterializeSensorDefinition, Definitions,  AutoMaterializePolicy, AutoMaterializeRule

my_custom_auto_materialize_sensor = AutoMaterializeSensorDefinition(
    "my_custom_auto_materialize_sensor",
    asset_selection=AssetSelection.all(include_sources=True),
    minimum_interval_seconds=60 * 15,
)

all_asset_freshness_checks = dbt_dwh_kielsa.all_asset_freshness_checks + ldcom_etl_dwh.all_asset_freshness_checks + recetas_libros_etl_dwh.all_asset_freshness_checks + knime_asset_factory.all_asset_freshness_checks
freshness_checks_sensor = build_sensor_for_freshness_checks(
    freshness_checks=all_asset_freshness_checks,
    default_status=running_default_sensor_status,
    minimum_interval_seconds=30 * 60,  # 5 minutes
    )

all_sensors = get_all_instances_of_class([SensorDefinition]) 

__all__ = list(map(lambda x: x.name, all_sensors) )
