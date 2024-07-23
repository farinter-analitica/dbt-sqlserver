from dagster import op, graph, job, schedule, In, Out, InMemoryIOManager, FilesystemIOManager, Output, OpExecutionContext, in_process_executor
from datetime import datetime, timedelta
from dateutil import parser
from typing import Any, Dict, List, Tuple
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.resources.correo_e import EmailSenderResource
import json

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

def obtener_max_datetime_val_replicas_sql_server(
    sql_server_origen: SQLServerResource,
    sql_server_replica: SQLServerResource,
    relation_origen: str,
    relation_replica: str,
    column_origen: str,
    column_replica: str,
) -> Tuple[Any, Any]:
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

    return max_val_origen, max_val_replica


def es_delta_max_datetime_superado(val_referencia: datetime, val_comprobado: datetime, delta_max: timedelta = timedelta(hours=1)) -> bool:
    if val_referencia is None and val_comprobado is None:
        raise ValueError("Ambos valores son None")
    if val_comprobado is None:
        return True
    return (val_referencia - val_comprobado) > delta_max

@op(out=Out(io_manager_key="in_memory_io_manager"))
def obtener_servidores_de_replica_en_alerta(
    context: OpExecutionContext,
    dwh_farinter_dl: SQLServerResource,
    dwh_farinter_dl_prd: SQLServerResource
) -> Dict[str, List[Dict[str, str]]]:
    lista_par_servidores: list[ParServidoresReplicaSQLServer] = [
    ParServidoresReplicaSQLServer(sql_server_origen=dwh_farinter_dl, 
                                sql_server_replica=dwh_farinter_dl_prd, 
                                relation_origen="DL_FARINTER.dbo.DL_Kielsa_FacturaEncabezado", 
                                relation_replica="DL_FARINTER.dbo.DL_Kielsa_FacturaEncabezado",
                                column_origen="Factura_Fecha",
                                column_replica="Factura_Fecha",
                                delta_max=timedelta(hours=1)
                                ),
    ]
    
    servidores_en_alerta: Dict[str, List[Dict[str, str]]] = {}
    for par_servidores in lista_par_servidores:
        max_val_origen, max_val_replica = obtener_max_datetime_val_replicas_sql_server(
            par_servidores.sql_server_origen,
            par_servidores.sql_server_replica,
            par_servidores.relation_origen,
            par_servidores.relation_replica,
            par_servidores.column_origen,
            par_servidores.column_replica,
        )

        if not isinstance(max_val_origen, datetime):
            max_val_origen = parser.parse(max_val_origen)
        if not isinstance(max_val_replica, datetime):
            max_val_replica = parser.parse(max_val_replica)

        meta_data = {
            "servidor_origen": par_servidores.sql_server_origen.server, 
            "servidor_replica": par_servidores.sql_server_replica.server,
            "relation_origen": par_servidores.relation_origen,
            "relation_replica": par_servidores.relation_replica,
            "max_val_origen": max_val_origen.strftime("%Y-%m-%d %H:%M:%S"),
            "max_val_replica": max_val_replica.strftime("%Y-%m-%d %H:%M:%S"),
            "delta_max": str(par_servidores.delta_max),
        }
        if es_delta_max_datetime_superado(max_val_origen, max_val_replica):
            if par_servidores.sql_server_replica.server not in servidores_en_alerta:
                servidores_en_alerta[par_servidores.sql_server_replica.server] = []
            servidores_en_alerta[par_servidores.sql_server_replica.server].append(meta_data)

    return servidores_en_alerta

EMAIL_BODY = \
"""
Los siguientes servidores de SQL Server tienen un delta de control de replicación superado:

{json_data}
"""

@op
def enviar_alertas_si_aplica(context: OpExecutionContext, servidores_en_alerta: Dict[str, List[Dict[str, str]]], enviador_correo_e_analitica_farinter: EmailSenderResource):
    #context.log.info(f"Servidores en alerta: {json.dumps(servidores_en_alerta)}")
    if len(servidores_en_alerta) > 0: 
        email_subject = "Delta de valor superado por la replica"
        email_body = EMAIL_BODY.format(json_data=json.dumps(servidores_en_alerta, indent=3))
        email_to = ["brian.padilla@farinter.com"]  # Replace with actual recipients
        enviador_correo_e_analitica_farinter.send_email(email_to, email_subject, email_body)
        context.log.info(f"Enviado alerta por correo electronico a {email_to} con el siguiente body: {email_body}")

@graph
def replica_check_graph():
    servidores_en_alerta = obtener_servidores_de_replica_en_alerta()
    enviar_alertas_si_aplica(servidores_en_alerta)


replica_check_job = replica_check_graph.to_job(executor_def=in_process_executor)

all_jobs = [replica_check_job]


