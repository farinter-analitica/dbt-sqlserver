from dagster import op, graph, job, schedule, In, Out, InMemoryIOManager, FilesystemIOManager, Output, OpExecutionContext, in_process_executor
from datetime import datetime, timedelta
from dateutil import parser
from typing import Any, Dict, List, Tuple, Optional
from dagster_shared_gf.resources.sql_server_resources import SQLServerResource
from dagster_shared_gf.resources.correo_e import EmailSenderResource
from dagster_shared_gf.shared_variables import env_str
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
    context: OpExecutionContext,
    sql_server_origen: SQLServerResource,
    sql_server_replica: SQLServerResource,
    relation_origen: str,
    relation_replica: str,
    column_origen: str,
    column_replica: str,
) -> Tuple[Any, Any, Dict[str, Any]]:
    max_val_origen = None
    max_val_replica = None
    logs = {"max_val_origen" : "Consulta exitosa", "max_val_replica" : "Consulta exitosa"}
    try:
        with sql_server_origen.get_connection() as conn_a:
            results_origen = sql_server_origen.query(
                query=f"SELECT MAX({column_origen}) FROM {relation_origen} --WITH (NOLOCK)",
                connection=conn_a,
            )
            if results_origen and results_origen[0][0]:
                max_val_origen = results_origen[0][0]
                if not isinstance(max_val_origen, datetime):
                    max_val_origen = parser.parse(max_val_origen)

    except Exception as e:
        logs["ocurrio_error"] = True
        logs["max_val_origen"] = f"Error en consulta, devolviendo null e incluido en alertas: {str(e)}"
        
    try:
        with sql_server_replica.get_connection() as conn_b:
            results_replica = sql_server_replica.query(
                query=f"SELECT MAX({column_replica}) FROM {relation_replica} --WITH (NOLOCK)",
                connection=conn_b,
            )
            if results_replica and results_replica[0][0]:
                max_val_replica = results_replica[0][0]
                if not isinstance(max_val_replica, datetime):
                    max_val_replica = parser.parse(max_val_replica)

    except Exception as e:
        logs["ocurrio_error"] = True
        logs["max_val_replica"] = f"Error en consulta, no se ha encontrado valor maximo: {str(e)}"

    if max_val_origen is None and logs.get("max_val_origen") == "Consulta exitosa":
        logs["ocurrio_error"] = True
        logs["max_val_origen"] = f"Error en consulta, no se ha encontrado valor maximo."

    if max_val_replica is None and logs.get("max_val_replica") == "Consulta exitosa":
        logs["ocurrio_error"] = True
        logs["max_val_replica"] = f"Error en consulta, no se ha encontrado valor maximo."

    return max_val_origen, max_val_replica, logs


def es_delta_max_datetime_superado(val_referencia: datetime | None, val_comprobado: datetime | None, delta_max: timedelta = timedelta(hours=1)) -> bool:
    if val_referencia is None and val_comprobado is None:
        return True
    if val_comprobado is None:
        return True
    if val_referencia is None:
        return False
    return (val_referencia - val_comprobado) > delta_max

@op(out=Out(io_manager_key="in_memory_io_manager"))
def obtener_servidores_de_replica_en_alerta(
    context: OpExecutionContext,
    dwh_farinter_dl: SQLServerResource,
    dwh_farinter_dl_prd: SQLServerResource,
    dwh_farinter_prd_replicas_ldcom: SQLServerResource,
    ldcom_hn_prd_sqlserver: SQLServerResource,
    ldcom_ni_prd_sqlserver: SQLServerResource,
    ldcom_cr_prd_sqlserver: SQLServerResource,
    ldcom_cr_arb_prd_sqlserver: SQLServerResource,
    ldcom_gt_prd_sqlserver: SQLServerResource,
    ldcom_sv_prd_sqlserver: SQLServerResource,
    siteplus_sqlldsubs_sqlserver: SQLServerResource,
    ) -> Dict[str, List[Dict[str, Any]]]:
        
    #definidos en dagster_shared_gf.resources.sql_server_resources, aqui solo de referencia
    LDCOM_SQLSERVER_HOSTS = {
        "HN": r"172.16.2.25",
        "NI": r"172.16.2.42",
        "CR": r"172.16.2.52",
        "GT": r"172.16.2.62",
        "SV": r"172.16.2.72",
        "SQLLDSUBS": r"172.16.2.125\SQLLDSUBS",
    }
    LDCOM_SQLSERVER_DATABASES = {
        "HN": ["LDCOM_KIELSA", "LDFAS_KIELSA"],
        "NI": ["LDCOM_KIELSA_NIC", "LDFAS_KIELSA_NIC"],
        "CR": ["LDCOM_KIELSA_CR", "LDFAS_KIELSA_CR"],
        "GT": ["LDCOM_KIELSA_GT", "LDFAS_KIELSA_GT"],
        "SV": ["LDCOM_KIELSA_SALVADOR", "LDFAS_KIELSA_SALVADOR"],
        "SQLLDSUBS": ["LDCOMREPHN", "LDCOMREPNIC","LDCOMREPSLV","LDCOMREPGT","LDCOMREPCR",
                    "LDFASREPHN","LDFASREPNIC","LDFASREPSLV","LDFASREPGT","LDFASREPCR",
                    "SITEPLUS","RECETAS","KPP_DB"],
    }

    lista_par_servidores: list[ParServidoresReplicaSQLServer] = [
    # ParServidoresReplicaSQLServer(sql_server_origen=dwh_farinter_dl, 
    #                             sql_server_replica=dwh_farinter_dl_prd, 
    #                             relation_origen="DL_FARINTER.dbo.DL_Kielsa_FacturaEncabezado", 
    #                             relation_replica="DL_FARINTER.dbo.DL_Kielsa_FacturaEncabezado",
    #                             column_origen="Factura_Fecha",
    #                             column_replica="Factura_Fecha",
    #                             delta_max=timedelta(hours=1)
    #                             ),
    ParServidoresReplicaSQLServer(sql_server_origen=ldcom_hn_prd_sqlserver,
                                sql_server_replica=dwh_farinter_prd_replicas_ldcom,
                                relation_origen="dbo.Ticket_Encabezado",
                                relation_replica="REP_LDCOM_HN.dbo.Ticket_Encabezado",
                                column_origen="Factura_Fecha",
                                column_replica="Factura_Fecha",
                                delta_max=timedelta(hours=1)
                                ),
    ParServidoresReplicaSQLServer(sql_server_origen=ldcom_ni_prd_sqlserver,
                                sql_server_replica=dwh_farinter_prd_replicas_ldcom,
                                relation_origen="dbo.Factura_Encabezado",
                                relation_replica="REP_LDCOM_NI.dbo.Factura_Encabezado",
                                column_origen="Factura_Fecha",
                                column_replica="Factura_Fecha",
                                delta_max=timedelta(hours=1)
                                ),
    ParServidoresReplicaSQLServer(sql_server_origen=ldcom_cr_prd_sqlserver,
                                sql_server_replica=dwh_farinter_prd_replicas_ldcom,
                                relation_origen="dbo.Factura_Encabezado",
                                relation_replica="REP_LDCOM_CR.dbo.Factura_Encabezado",
                                column_origen="Factura_Fecha",
                                column_replica="Factura_Fecha",
                                delta_max=timedelta(hours=1)
                                ),
    ParServidoresReplicaSQLServer(sql_server_origen=ldcom_cr_arb_prd_sqlserver,
                                sql_server_replica=dwh_farinter_prd_replicas_ldcom,
                                relation_origen="dbo.Factura_Encabezado",
                                relation_replica="REP_LDCOM_ARB.dbo.Factura_Encabezado",
                                column_origen="Factura_Fecha",
                                column_replica="Factura_Fecha",
                                delta_max=timedelta(hours=1)
                                ),
    ParServidoresReplicaSQLServer(sql_server_origen=ldcom_gt_prd_sqlserver,
                                sql_server_replica=dwh_farinter_prd_replicas_ldcom,
                                relation_origen="dbo.Ticket_Encabezado",
                                relation_replica="REP_LDCOM_GT.dbo.Ticket_Encabezado",
                                column_origen="Factura_Fecha",
                                column_replica="Factura_Fecha",
                                delta_max=timedelta(hours=1)
                                ),
    ParServidoresReplicaSQLServer(sql_server_origen=ldcom_sv_prd_sqlserver,
                                sql_server_replica=dwh_farinter_prd_replicas_ldcom,
                                relation_origen="dbo.Ticket_Encabezado",
                                relation_replica="REP_LDCOM_SV.dbo.Ticket_Encabezado",
                                column_origen="Factura_Fecha",
                                column_replica="Factura_Fecha",
                                delta_max=timedelta(hours=1)
                                ),
    ParServidoresReplicaSQLServer(sql_server_origen=ldcom_hn_prd_sqlserver,
                                sql_server_replica=siteplus_sqlldsubs_sqlserver,
                                relation_origen="LDCOM_KIELSA.dbo.Ticket_Encabezado",
                                relation_replica="LDCOMREPHN.dbo.Ticket_Encabezado",
                                column_origen="Factura_Fecha",
                                column_replica="Factura_Fecha",
                                delta_max=timedelta(hours=1)
                                ),
    ParServidoresReplicaSQLServer(sql_server_origen=ldcom_ni_prd_sqlserver,
                                sql_server_replica=siteplus_sqlldsubs_sqlserver,
                                relation_origen="LDCOM_KIELSA_NIC.dbo.Factura_Encabezado",
                                relation_replica="LDCOMREPNIC.dbo.Factura_Encabezado",
                                column_origen="Factura_Fecha",
                                column_replica="Factura_Fecha",
                                delta_max=timedelta(hours=1)
                                ),
    ParServidoresReplicaSQLServer(sql_server_origen=ldcom_cr_prd_sqlserver,
                                sql_server_replica=siteplus_sqlldsubs_sqlserver,
                                relation_origen="LDCOM_KIELSA_CR.dbo.Factura_Encabezado",
                                relation_replica="LDCOMREPCR.dbo.Factura_Encabezado",
                                column_origen="Factura_Fecha",
                                column_replica="Factura_Fecha",
                                delta_max=timedelta(hours=1)
                                ),
    ParServidoresReplicaSQLServer(sql_server_origen=ldcom_cr_arb_prd_sqlserver,
                                sql_server_replica=siteplus_sqlldsubs_sqlserver,
                                relation_origen="LDCOM_KIELSA_CR.dbo.Factura_Encabezado",
                                relation_replica="LDCOMREPARBCR.dbo.Factura_Encabezado",
                                column_origen="Factura_Fecha",
                                column_replica="Factura_Fecha",
                                delta_max=timedelta(hours=1)
                                ),
    ParServidoresReplicaSQLServer(sql_server_origen=ldcom_gt_prd_sqlserver,
                                sql_server_replica=siteplus_sqlldsubs_sqlserver,
                                relation_origen="LDCOM_KIELSA_GT.dbo.Ticket_Encabezado",
                                relation_replica="LDCOMREPGT.dbo.Ticket_Encabezado",
                                column_origen="Factura_Fecha",
                                column_replica="Factura_Fecha",
                                delta_max=timedelta(hours=1)
                                ),
    ParServidoresReplicaSQLServer(sql_server_origen=ldcom_sv_prd_sqlserver,
                                sql_server_replica=siteplus_sqlldsubs_sqlserver,
                                relation_origen="LDCOM_KIELSA_SALVADOR.dbo.Ticket_Encabezado",
                                relation_replica="LDCOMREPSLV.dbo.Ticket_Encabezado",
                                column_origen="Factura_Fecha",
                                column_replica="Factura_Fecha",
                                delta_max=timedelta(hours=1)
                                ),
    ParServidoresReplicaSQLServer(sql_server_origen=ldcom_hn_prd_sqlserver,
                                sql_server_replica=siteplus_sqlldsubs_sqlserver,
                                relation_origen="LDFAS_KIELSA.dbo.Cola_Control",
                                relation_replica="LDFASREPHN.dbo.Cola_Control",
                                column_origen="Fecha",
                                column_replica="Fecha",
                                delta_max=timedelta(hours=1)
                                ),
    ParServidoresReplicaSQLServer(sql_server_origen=ldcom_ni_prd_sqlserver,
                                sql_server_replica=siteplus_sqlldsubs_sqlserver,
                                relation_origen="LDFAS_KIELSA_NIC.dbo.Cola_Control",
                                relation_replica="LDFASREPNIC.dbo.Cola_Control",
                                column_origen="Fecha",
                                column_replica="Fecha",
                                delta_max=timedelta(hours=1)
                                ),
    ParServidoresReplicaSQLServer(sql_server_origen=ldcom_cr_prd_sqlserver,
                                sql_server_replica=siteplus_sqlldsubs_sqlserver,
                                relation_origen="LDFAS_KIELSA_CR.dbo.Cola_Control",
                                relation_replica="LDFASREPCR.dbo.Cola_Control",
                                column_origen="Fecha",
                                column_replica="Fecha",
                                delta_max=timedelta(hours=1)
                                ),
    ParServidoresReplicaSQLServer(sql_server_origen=ldcom_cr_arb_prd_sqlserver,
                                sql_server_replica=siteplus_sqlldsubs_sqlserver,
                                relation_origen="LDFAS_KIELSA_CR.dbo.Cola_Control",
                                relation_replica="LDFASREPARBCR.dbo.Cola_Control",
                                column_origen="Fecha",
                                column_replica="Fecha",
                                delta_max=timedelta(hours=1)
                                ),
    ParServidoresReplicaSQLServer(sql_server_origen=ldcom_gt_prd_sqlserver,
                                sql_server_replica=siteplus_sqlldsubs_sqlserver,
                                relation_origen="LDFAS_KIELSA_GT.dbo.Cola_Control",
                                relation_replica="LDFASREPGT.dbo.Cola_Control",
                                column_origen="Fecha",
                                column_replica="Fecha",
                                delta_max=timedelta(hours=1)
                                ),
    ParServidoresReplicaSQLServer(sql_server_origen=ldcom_sv_prd_sqlserver,
                                sql_server_replica=siteplus_sqlldsubs_sqlserver,
                                relation_origen="LDFAS_KIELSA_SALVADOR.dbo.Cola_Control",
                                relation_replica="LDFASREPSLV.dbo.Cola_Control",
                                column_origen="Fecha",
                                column_replica="Fecha",
                                delta_max=timedelta(hours=1)
                                ),
    ]
    
    servidores_en_alerta: Dict[str, List[Dict[str, Any]]] = {}
    max_val_origen: datetime = None
    max_val_replica: datetime = None
    logs: Dict[str, str] = {}
    for par_servidores in lista_par_servidores:
        max_val_origen, max_val_replica, logs = obtener_max_datetime_val_replicas_sql_server(
            context,
            par_servidores.sql_server_origen,
            par_servidores.sql_server_replica,
            par_servidores.relation_origen,
            par_servidores.relation_replica,
            par_servidores.column_origen,
            par_servidores.column_replica,
        )

        if es_delta_max_datetime_superado(max_val_origen, max_val_replica) or logs.get("ocurrio_error", None) is not None:
            meta_data = {
                "servidor_origen": par_servidores.sql_server_origen.server, 
                #"servidor_replica": par_servidores.sql_server_replica.server,
                "relation_origen": par_servidores.relation_origen,
                "relation_replica": par_servidores.relation_replica,
                "max_val_origen": str(max_val_origen.strftime("%Y-%m-%d %H:%M:%S") if max_val_origen is not None else max_val_origen),
                "max_val_replica": str(max_val_replica.strftime("%Y-%m-%d %H:%M:%S") if max_val_replica is not None else max_val_replica),
                "delta_max": str(par_servidores.delta_max),
            }
            if logs.get("ocurrio_error", None) is not None:
                meta_data["logs_consulta"] = str(logs),         
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
def enviar_alertas_si_aplica(context: OpExecutionContext, servidores_en_alerta: Dict[str, List[Dict[str, Any]]], enviador_correo_e_analitica_farinter: EmailSenderResource):
    #context.log.info(f"Servidores en alerta: {json.dumps(servidores_en_alerta)}")
    if len(servidores_en_alerta) > 0: 
        email_subject = "[analiticastetl][Advertencia] Delta de valor superado por la replica"
        email_body = EMAIL_BODY.format(json_data=json.dumps(servidores_en_alerta, indent=3))
        email_to = ["brian.padilla@farinter.com", "edwin.martinez@farinter.com", "Wilson.zavala@farinter.com"] if env_str == "prd" else ["brian.padilla@farinter.com"]
        enviador_correo_e_analitica_farinter.send_email(email_to, email_subject, email_body)
        context.log.info(f"Enviado alerta por correo electronico a {email_to} con el siguiente body: {email_body}")
    else:
        context.log.info("No hay servidores en alerta")

@graph
def comprobar_sinc_replicas_graph():
    servidores_en_alerta = obtener_servidores_de_replica_en_alerta()
    enviar_alertas_si_aplica(servidores_en_alerta)


comprobar_sinc_replicas_job = comprobar_sinc_replicas_graph.to_job(name="comprobar_sinc_replicas_job", executor_def=in_process_executor)

all_jobs = [comprobar_sinc_replicas_job]

__all__ = list(map(lambda x: x.name, all_jobs) )
