import requests
import pymssql
import pandas as pd
import re
import time
import logging
from typing import TypedDict, Dict, List, Optional
from dagster_shared_gf.config import get_dagster_config
import dagster as dg

# ******************************************************************************
#                  UTILIDADES
# ******************************************************************************


# Esta prueba se conecta a las bases de datos, no hace cambios,
# además envía un mensaje a un numero, solo funciona si el env es local o dev.
# en este modo solo se recoge un solo registro para evitar multiples sms de prueba
# Para evitar olvidos o problemas solo cambiar en if name main al final.
ACTIVAR_PRUEBA_DE_INTEGRACION = False
NUMERO_DE_TELEFONO_INTEGRACION = None


def log_debug(msg: str, *args, **kwargs) -> None:
    """Log a debug message only when running in local mode (cfg.is_local).

    Accepts the same style arguments as logging.debug, e.g. log_debug("x=%s", x)
    """
    cfg = get_dagster_config()
    if cfg.is_local:
        logger = logging.getLogger(__name__)
        # Ensure at least one handler so logs appear when running locally
        if not logging.getLogger().handlers:
            logging.basicConfig(level=logging.DEBUG)
        logger.debug(msg, *args, **kwargs)


def _to_sql_literal(val):
    if val is None:
        return "NULL"
    if isinstance(val, str):
        return "'" + val.replace("'", "''") + "'"
    if isinstance(val, bool):
        return "1" if val else "0"
    return str(val)


def format_query_for_logging(query: str, params):
    """Interpolate params into query for logging only (positional %s or mapping %(name)s)."""
    if not params:
        return query

    # mapping-style (e.g. "WHERE id=%(id)s")
    if isinstance(params, dict):
        try:
            mapped = {k: _to_sql_literal(v) for k, v in params.items()}
            return query % mapped
        except Exception:
            return query

    # sequence-style: replace each %s left-to-right
    parts = query.split("%s")
    if len(parts) == 1:
        return query
    out = parts[0]
    for i, tail in enumerate(parts[1:]):
        val = params[i] if i < len(params) else None
        out += _to_sql_literal(val) + tail
    return out


# ******************************************************************************
#                  DEFINICION DE VARIABLES GLOBALES
# ******************************************************************************
# No leemos variables de entorno fuera de funciones ya que se ejecuta solo con importar el archivo.


# ********************URL de la API*************************
class SMSPayload(TypedDict):
    recipient: str
    message: str
    aplicacion: str
    tipo_mensaje: int
    messages: List[Dict[str, str]]


# Diccionario ejemplo con los datos que enviarás como JSON
# payload = {
#     "recipient": "504999999",
#     "message": "Escribir mensajes aqui",
#     "aplicacion": "Analitica PRD",
#     "tipo_mensaje": 1,
#     "messages": [{"message": "string", "recipient": "string"}],
# }


def enviar_sms(
    payload: SMSPayload,
    api_url: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
) -> requests.Response:
    """Send SMS payload to configured URL. Returns requests.Response on success or None on failure.

    url_override and headers_override can be used for testing or to override the global values.
    """
    cfg = get_dagster_config()
    # If integration test mode is active in a local environment, allow sending a real SMS
    integration_test_active = ACTIVAR_PRUEBA_DE_INTEGRACION and cfg.is_dev_like

    if cfg.is_dev_like and not (
        integration_test_active and NUMERO_DE_TELEFONO_INTEGRACION
    ):
        # In regular dev-like environments we skip sending SMS
        log_debug("Desarrollo - no se enviará SMS")
        log_debug("Payload: %s", str(payload))

        class MockDevResponse(requests.Response):
            status_code = 200

        return MockDevResponse()

    if integration_test_active and NUMERO_DE_TELEFONO_INTEGRACION:
        log_debug("Integration test active - using test recipient")
        payload["recipient"] = NUMERO_DE_TELEFONO_INTEGRACION

    effective_url = api_url or cfg.dagster_sms_api_url
    if not effective_url:
        raise ValueError("No se ha configurado una URL de API para el envío de SMS.")
    effective_headers = headers or {"Content-Type": "application/json"}
    try:
        resp = requests.post(effective_url, json=payload, headers=effective_headers)
        return resp
    except Exception:

        class MockErrorResponse(requests.Response):
            status_code = 500

        return MockErrorResponse()


# ***************DWH SMS KIELSA****************************
# En la funcion
DATABASE_SMS_KIELSA = "SMS_KIELSA"
# ***************LDCOM QA*********************************
# En la funcion
# *********************************************************

# Variables para avisar en bitacora que algo anda mal en este script
# En la funcion


# ******************************************************************************
#                 ENCAPSUALIMIENTO DE Consultas y procesos SQL
# ******************************************************************************


def consultar_datos(Query):
    cfg = get_dagster_config()
    server = cfg.dagster_dwh_farinter_sql_server
    user = cfg.dagster_dwh_farinter_username
    password = cfg.dagster_secret_dwh_farinter_password
    if server is None or user is None or password is None:
        raise ValueError("Configuración del servidor no encontrada.")
    conn = pymssql.connect(
        server=server,
        user=user,
        password=password.get_value(),
        database=DATABASE_SMS_KIELSA,
    )
    cursor = conn.cursor()
    cursor.execute(Query)
    filas = cursor.fetchall()
    encabezados = cursor.description

    datos = pd.DataFrame.from_records(
        filas, columns=[column[0] for column in encabezados]
    )

    conn.close()

    return datos


def ejecutar_proceso(Query, parametros=None):
    cfg = get_dagster_config()
    server = cfg.dagster_dwh_farinter_sql_server
    user = cfg.dagster_dwh_farinter_username
    password = cfg.dagster_secret_dwh_farinter_password

    if server is None or user is None or password is None:
        raise ValueError("Configuración del servidor dwh no encontrada.")

    # If integration test mode is active in a local environment, run check query
    integration_test_active = ACTIVAR_PRUEBA_DE_INTEGRACION and cfg.is_local

    if cfg.is_dev_like:
        log_debug("Desarrollo - no se ejecuta proceso dwh")
        query_final = format_query_for_logging(Query, parametros)
        log_debug("Query: %s", query_final)
        if integration_test_active:
            with pymssql.connect(
                server=server,
                user=user,
                password=password.get_value(),
                database=DATABASE_SMS_KIELSA,
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1", parametros)
        return "ejecutado exitosamente DWH"

    conn = pymssql.connect(
        server=server,
        user=user,
        password=password.get_value(),
        database=DATABASE_SMS_KIELSA,
    )
    cursor = None
    try:
        conn.autocommit(False)
        cursor = conn.cursor()
        if parametros:
            cursor.execute(Query, parametros)
        else:
            cursor.execute(Query)
        # If running in integration test mode on local, rollback so changes are not persisted
        if integration_test_active:
            log_debug(
                "Integration test active - rolling back transaction for Query: %s",
                Query,
            )
            conn.rollback()
            resultado = "ejecutado exitosamente (rollback prueba integración)"
        else:
            conn.commit()
            resultado = "ejecutado exitosamente"
    except Exception as e:
        resultado = "Error al ejecutar:" + str(e)
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

    return resultado


def ejecutar_procesoLD(Query, parametros=None):
    cfg = get_dagster_config()
    LDQA_server = "172.16.2.25"
    LDQA_Database = "SITEPLUS"
    LDQA_user = cfg.dagster_ldcom_prd_ecomm_username
    LDQA_password = cfg.dagster_secret_ldcom_prd_ecomm_password
    # If integration test mode is active in a local environment, run the query but rollback
    integration_test_active = ACTIVAR_PRUEBA_DE_INTEGRACION and cfg.is_local

    if cfg.is_dev_like:
        log_debug("Desarrollo - no se ejecuta proceso LD")
        query_final = format_query_for_logging(Query, parametros)
        log_debug("Query: %s", query_final)

        if integration_test_active:
            with pymssql.connect(
                server=LDQA_server,
                user=LDQA_user,
                password=LDQA_password.get_value(),
                database=LDQA_Database,
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1", parametros)
        return "ejecutado exitosamente LD"
    if LDQA_user is None or LDQA_password is None:
        raise ValueError("Configuración del usuario LD no encontrada.")
    conn = pymssql.connect(
        server=LDQA_server,
        user=LDQA_user,
        password=LDQA_password.get_value(),
        database=LDQA_Database,
    )
    cursor = None
    try:
        conn.autocommit(False)
        cursor = conn.cursor()
        if parametros:
            cursor.execute(Query, parametros)
        else:
            cursor.execute(Query)
        # Rollback when running integration test active on local to avoid persisting changes
        if integration_test_active:
            log_debug(
                "Integration test active - rolling back LD transaction for Query: %s",
                Query,
            )
            conn.rollback()
            resultado = "ejecutado exitosamente (rollback prueba integración)"
        else:
            conn.commit()
            resultado = "ejecutado exitosamente"
    except Exception as e:
        resultado = "Error al ejecutar:" + str(e)
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

    return resultado


# ******************************************************************************
#                           INICIO DE ALGORITMO
# ******************************************************************************


def algoritmo_envio_sms():
    cfg = get_dagster_config()
    Bitacora_Mensaje = ""
    Bitacora_Tipo = "Error"
    Bitacora_Tabla = (
        f"Script Envio SMS {cfg.dagster_instance_current_env.upper()} python"
    )
    Inicio = time.time()
    Final = time.time()
    Duracion_Minutos = 0
    resultado = ""
    # Contadores de envíos: exitosos (status 1) y con errores (status 2 o 3)
    exitosos = 0
    errores = 0
    # Carga mensajes enviados por la acción
    cargaSMS = ejecutar_proceso("EXEC SMS_paCargaMensajes_ClientesNuevos")

    if "ejecutado exitosamente" in cargaSMS:
        Bitacora_Tipo = "Finalizado"
        Bitacora_Mensaje = (
            "python/Dagster/envio_sms_kielsa SMS_paCargaMensajes_ClientesNuevos"
        )
    else:
        Bitacora_Tipo = "Error"
        Bitacora_Mensaje = f"python/Dagster/envio_sms_kielsa {cargaSMS}"

    Final = time.time()

    Duracion_Minutos = (Final - Inicio) / 60

    parametros = (Bitacora_Mensaje, Bitacora_Tipo, Bitacora_Tabla, Duracion_Minutos)

    resultado = ejecutar_proceso(
        "INSERT INTO SMS_CnfBitacora(Bitacora_Fecha, Bitacora_Mensaje, Bitacora_Tipo, Bitacora_Tabla, Duracion_Minutos) values(getdate(), %s, %s, %s, %s)",
        parametros,
    )
    log_debug("cargaSMS: %s", cargaSMS)

    # Actualiza saldos del cliente
    ActualizaSaldos = ejecutar_proceso("EXEC SMS_paActualizaSaldo_Monederos")

    if "ejecutado exitosamente" in ActualizaSaldos:
        Bitacora_Tipo = "Finalizado"
        Bitacora_Mensaje = (
            "python/Dagster/envio_sms_kielsa SMS_paActualizaSaldo_Monederos"
        )
    else:
        Bitacora_Tipo = "Error"
        Bitacora_Mensaje = f"python/Dagster/envio_sms_kielsa {ActualizaSaldos}"

    Final = time.time()

    Duracion_Minutos = (Final - Inicio) / 60

    parametros = (Bitacora_Mensaje, Bitacora_Tipo, Bitacora_Tabla, Duracion_Minutos)

    resultado = ejecutar_proceso(
        "INSERT INTO SMS_CnfBitacora(Bitacora_Fecha, Bitacora_Mensaje, Bitacora_Tipo, Bitacora_Tabla, Duracion_Minutos) values(getdate(), %s, %s, %s, %s)",
        parametros,
    )
    log_debug("ActualizaSaldos: %s", ActualizaSaldos)

    # Set de mensajes a enviar
    datos = consultar_datos(
        "SELECT * FROM SMS_KIELSA.dbo.SMS_Mensajes_Enviados where Emp_Id = 1 and Estado = 0 and Fecha_Envio<getdate()"
    )

    if cfg.is_dev_like:
        log_debug("Desarrollo - limite de registros a 1/10")
        datos = datos.head(10)

    # Set de bonos a realizar justo antes de enviar sms.

    bonos = consultar_datos(
        "select * from SMS_KIELSA.dbo.SMS_Bonos where Emp_Id = 1 and Estado = 'Sin realizar' and getdate() between Fecha_Acreditacion and Fecha_Vencimiento"
    )

    log_debug("bonos filas: %s", bonos.size)

    for idx in datos.index:  # Iternado sobre el set de mensajes pendientes de enviar.
        fila = datos.loc[idx]

        # Claves para filtrar fila unica en sms_mensajes_enviados

        Emp_Id = int(fila["Emp_Id"])
        Proyecto_Id = int(fila["Proyecto_Id"])
        Monedero_Id = fila["Monedero_Id"]
        Accion_Id = int(fila["Accion_Id"])
        AnioMes_Id = int(fila["AnioMes_Id"])
        Mensaje_Id = int(fila["Mensaje_Id"])
        Celular = str(fila["Celular"])

        # Caracteristicas del sms

        Mensaje_Param = consultar_datos(
            "select * from SMS_KIELSA.dbo.SMS_Mensajes where Emp_Id = "
            + str(Emp_Id)
            + " and Mensaje_Id = "
            + str(Mensaje_Id)
        )

        log_debug("Mensaje_Param: %s", Mensaje_Param)

        # Fila de bonos a realizar al cliente con esta accion en este proyecto
        bono_fila = bonos[
            (bonos["Emp_Id"] == Emp_Id)
            & (bonos["Proyecto_Id"] == Proyecto_Id)
            & (bonos["Accion_Id"] == Accion_Id)
            & (bonos["Monedero_Id"] == Monedero_Id)
            & (bonos["Estado"] == "Sin realizar")
        ]

        log_debug("bono_fila: %s", bono_fila)

        Acredita_BonoLD = ""

        RespuestaApi = ""

        Bono_Estado = ""

        SMS_Estado = 0

        if not bono_fila.empty:
            bono_fila = bono_fila.iloc[0]

            MonederoTarj_Id = re.sub(
                r"^(\d{4})(\d{4})(\d{5})$", r"\1-\2-\3", Monedero_Id
            )  # Devolver formato original

            log_debug("MonederoTarj_Id: %s", MonederoTarj_Id)
            log_debug("bono_fila: %s", bono_fila)

            Emp_Id = int(bono_fila["Emp_Id"])

            Bono_Id = int(bono_fila["Bono_Id"])

            Incentivo = int(bono_fila["Monto"])

            parametros = (Emp_Id, MonederoTarj_Id, Incentivo)

            Acredita_BonoLD = ejecutar_procesoLD(
                "EXEC sp_Acredita_Monedero %s, %s, %s", parametros
            )

            # Captura de error si no acredita

            if "ejecutado exitosamente" in Acredita_BonoLD:
                Bitacora_Tipo = "Finalizado"
                Bitacora_Mensaje = "python sp_Acredita_Monedero"
            else:
                Bitacora_Tipo = "Error"
                Bitacora_Mensaje = Acredita_BonoLD

            Final = time.time()

            Duracion_Minutos = (Final - Inicio) / 60

            parametros = (
                Bitacora_Mensaje,
                Bitacora_Tipo,
                Bitacora_Tabla,
                Duracion_Minutos,
            )

            resultado = ejecutar_proceso(
                "INSERT INTO SMS_CnfBitacora(Bitacora_Fecha, Bitacora_Mensaje, Bitacora_Tipo, Bitacora_Tabla, Duracion_Minutos) values(getdate(), %s, %s, %s, %s)",
                parametros,
            )

            # Fin de captura de error
            if "ejecutado exitosamente" in Acredita_BonoLD:
                Bono_Estado = "Realizado"
                parametros = (Bono_Estado, Emp_Id, Bono_Id, Monedero_Id)
                Actualiza_Bono = ejecutar_proceso(
                    "UPDATE SMS_Bonos SET Estado = %s, Fecha_Acreditacion = getdate() WHERE Emp_Id = %s and Bono_Id = %s and Monedero_Id = %s",
                    parametros,
                )
                # Cambio de estado a nivel de dataframe
                bonos.loc[
                    (bonos["Emp_Id"] == Emp_Id)
                    & (bonos["Proyecto_Id"] == Proyecto_Id)
                    & (bonos["Accion_Id"] == Accion_Id)
                    & (bonos["Monedero_Id"] == Monedero_Id),
                    "Estado",
                ] = "Realizado"

            else:
                Bono_Estado = "No realizado"
                parametros = (Bono_Estado, Emp_Id, Bono_Id, Monedero_Id)
                Actualiza_Bono = ejecutar_proceso(
                    "UPDATE SMS_Bonos SET Estado = %s WHERE Emp_Id = %s and Bono_Id = %s and Monedero_Id = %s",
                    parametros,
                )
                # Cambio de estado a nivel de dataframe
                bonos.loc[
                    (bonos["Emp_Id"] == Emp_Id)
                    & (bonos["Proyecto_Id"] == Proyecto_Id)
                    & (bonos["Accion_Id"] == Accion_Id)
                    & (bonos["Monedero_Id"] == Monedero_Id),
                    "Estado",
                ] = "No realizado"
            log_debug("Actualiza_Bono: %s", Actualiza_Bono)
        try:
            payload = SMSPayload(
                recipient=Celular,
                message=str(fila["Mensaje_Texto"]),
                aplicacion=f"Retencion de clientes {cfg.dagster_instance_current_env.upper()}",
                tipo_mensaje=1,
                messages=[{"message": "string", "recipient": "string"}],
            )
            # Notificacion de incentivo inmediato
            if (
                "ejecutado exitosamente" in Acredita_BonoLD
                and fila["Incentivo"] > 0
                and Mensaje_Param["Tipo_Mensaje"].loc[0] == "Credito inmediato"
            ):
                RespuestaApi = enviar_sms(payload)

                if RespuestaApi.status_code == 200:
                    SMS_Estado = 1
                    parametros = (
                        SMS_Estado,
                        Emp_Id,
                        Proyecto_Id,
                        Monedero_Id,
                        Mensaje_Id,
                        Accion_Id,
                        AnioMes_Id,
                    )
                    Actualiza_SMS = ejecutar_proceso(
                        "UPDATE SMS_Mensajes_Enviados SET Estado = %s, fecha_envio = getdate() WHERE Emp_Id = %s and Proyecto_Id = %s and Monedero_Id = %s and Mensaje_Id = %s and Accion_Id = %s and AnioMes_Id = %s",
                        parametros,
                    )

                else:
                    SMS_Estado = 2
                    parametros = (
                        SMS_Estado,
                        Emp_Id,
                        Proyecto_Id,
                        Monedero_Id,
                        Mensaje_Id,
                        Accion_Id,
                        AnioMes_Id,
                    )
                    Actualiza_SMS = ejecutar_proceso(
                        "UPDATE SMS_Mensajes_Enviados SET Estado = %s, fecha_envio = getdate() WHERE Emp_Id = %s and Proyecto_Id = %s and Monedero_Id = %s and Mensaje_Id = %s and Accion_Id = %s and AnioMes_Id = %s",
                        parametros,
                    )

            else:  # Notificación simple con/sin bandera de incentivo (no se ejecutó credito)
                if (
                    fila["Incentivo"] >= 0
                    and Mensaje_Param["Tipo_Mensaje"].loc[0] == "Notificacion"
                ):
                    RespuestaApi = enviar_sms(payload)
                    if RespuestaApi.status_code == 200:
                        SMS_Estado = 1
                        parametros = (
                            SMS_Estado,
                            Emp_Id,
                            Proyecto_Id,
                            Monedero_Id,
                            Mensaje_Id,
                            Accion_Id,
                            AnioMes_Id,
                        )
                        Actualiza_SMS = ejecutar_proceso(
                            "UPDATE SMS_Mensajes_Enviados SET Estado = %s, fecha_envio = getdate() WHERE Emp_Id = %s and Proyecto_Id = %s and Monedero_Id = %s and Mensaje_Id = %s and Accion_Id = %s and AnioMes_Id = %s",
                            parametros,
                        )

                    else:
                        SMS_Estado = 2

                        parametros = (
                            SMS_Estado,
                            Emp_Id,
                            Proyecto_Id,
                            Monedero_Id,
                            Mensaje_Id,
                            Accion_Id,
                            AnioMes_Id,
                        )
                        Actualiza_SMS = ejecutar_proceso(
                            "UPDATE SMS_Mensajes_Enviados SET Estado = %s, fecha_envio = getdate() WHERE Emp_Id = %s and Proyecto_Id = %s and Monedero_Id = %s and Mensaje_Id = %s and Accion_Id = %s and AnioMes_Id = %s",
                            parametros,
                        )
                else:
                    if ACTIVAR_PRUEBA_DE_INTEGRACION and cfg.is_dev_like:
                        enviar_sms(payload)
                    SMS_Estado = 3  # Inconsistencia en el registro ¿No es notificación?
                    parametros = (
                        SMS_Estado,
                        Emp_Id,
                        Proyecto_Id,
                        Monedero_Id,
                        Mensaje_Id,
                        Accion_Id,
                        AnioMes_Id,
                    )
                    Actualiza_SMS = ejecutar_proceso(
                        "UPDATE SMS_Mensajes_Enviados SET Estado = %s, fecha_envio = getdate() WHERE Emp_Id = %s and Proyecto_Id = %s and Monedero_Id = %s and Mensaje_Id = %s and Accion_Id = %s and AnioMes_Id = %s",
                        parametros,
                    )
            log_debug("Actualiza_SMS: %s", Actualiza_SMS)
            log_debug("SMS_Estado: %s", SMS_Estado)
            # Actualizar contadores según estado del envío
            try:
                exitosos += 1
            except Exception:
                # defensivo: no dejar que un fallo en el conteo rompa el proceso
                pass
            if ACTIVAR_PRUEBA_DE_INTEGRACION:
                # Evitar multiples mensajes de prueba
                break

        except Exception as e:
            # Contabilizar como error de envío
            try:
                errores += 1
            except Exception:
                pass
            Bitacora_Mensaje = (
                f"python/Dagster/envio_sms_kielsa Error al enviar sms {str(e)}"
            )
            Final = time.time()
            Bitacora_Tipo = "Error"
            Duracion_Minutos = (Final - Inicio) / 60
            parametros = (
                Bitacora_Mensaje,
                Bitacora_Tipo,
                Bitacora_Tabla,
                Duracion_Minutos,
            )
            resultado = ejecutar_proceso(
                "INSERT INTO SMS_CnfBitacora(Bitacora_Fecha, Bitacora_Mensaje, Bitacora_Tipo, Bitacora_Tabla, Duracion_Minutos) values(getdate(), %s, %s, %s, %s)",
                parametros,
            )
            log_debug("Error enviando SMS: %s", e)

    Bitacora_Mensaje = f"python/Dagster/envio_sms_kielsa Ejecucion exitosa de SMS Envio {cfg.dagster_instance_current_env.upper()}"
    Final = time.time()
    Duracion_Minutos = (Final - Inicio) / 60
    Bitacora_Tipo = "Finalizado"
    parametros = (Bitacora_Mensaje, Bitacora_Tipo, Bitacora_Tabla, Duracion_Minutos)
    resultado = ejecutar_proceso(
        "INSERT INTO SMS_CnfBitacora(Bitacora_Fecha, Bitacora_Mensaje, Bitacora_Tipo, Bitacora_Tabla, Duracion_Minutos) values(getdate(), %s, %s, %s, %s)",
        parametros,
    )
    log_debug("resultado: %s", resultado)

    # Devolver contadores para ser usados por el op
    return {"exitosos": exitosos, "errores": errores}


@dg.op(pool="run_sms_kielsa_op")
def run_sms_kielsa_op() -> dg.Output:
    inicio = time.time()
    counters = algoritmo_envio_sms()
    exitosos = counters.get("exitosos", 0)
    errores = counters.get("errores", 0)

    metadata = {"duracion": time.time() - inicio}
    metadata["envios_exitosos"] = exitosos
    metadata["envios_con_error"] = errores

    if exitosos > 5 and errores / exitosos > 0.9:
        raise Exception(
            f"python/Dagster/envio_sms_kielsa: Alto ratio de errores en envíos SMS: {errores} errores vs {exitosos} exitosos"
        )

    return dg.Output(value=None, metadata=metadata)


@dg.job
def run_sms_kielsa_job() -> None:
    run_sms_kielsa_op()


@dg.schedule(
    cron_schedule="*/15 6-21 * * *" if get_dagster_config().is_prd else "*/15 10 * * *",
    execution_timezone=get_dagster_config().default_timezone_iana,
    job=run_sms_kielsa_job,
    default_status=dg.DefaultScheduleStatus.RUNNING
    if not get_dagster_config().is_local
    else dg.DefaultScheduleStatus.STOPPED,
)
def run_sms_kielsa_schedule() -> dg.RunRequest:
    return dg.RunRequest()


if __name__ == "__main__":
    ACTIVAR_PRUEBA_DE_INTEGRACION = False
    NUMERO_DE_TELEFONO_INTEGRACION = None  # Especificar un numero y enviara un mensaje en modo integracion en dev/local
    print("--- Prueba de integración con la función completa ---")
    print(run_sms_kielsa_op().metadata)

    print("--- Pruebas simples (mocks) para validar la lógica del op ---")
    # Guardar la referencia original y restaurar después
    original_alg = algoritmo_envio_sms

    def mock_alg_ok():
        # 3 exitosos, 1 error -> no debe lanzar excepción
        return {"exitosos": 3, "errores": 1}

    def mock_alg_high_error():
        # 10 exitosos, 10 errores -> ratio = 1.0 > 0.9 y exitosos > 5 -> debe lanzar
        return {"exitosos": 10, "errores": 10}

    try:
        # Caso: comportamiento OK
        algoritmo_envio_sms = mock_alg_ok
        out = run_sms_kielsa_op()
        md = out.metadata
        ok1 = (
            md.get("envios_exitosos").value == 3
            and md.get("envios_con_error").value == 1
        )
        print("TEST mock_alg_ok ->", "PASS" if ok1 else f"FAIL metadata={md}")

        # Caso: alto ratio de errores -> debe levantar excepción
        algoritmo_envio_sms = mock_alg_high_error
        try:
            run_sms_kielsa_op()
            print("TEST mock_alg_high_error -> FAIL (no lanzó excepción)")
        except Exception as e:
            print("TEST mock_alg_high_error -> PASS (excepción lanzada):", str(e))

    finally:
        # Restaurar la función original
        algoritmo_envio_sms = original_alg

    print("--- Pruebas de integración con Dagster ---")
    defs = dg.Definitions(
        jobs=[run_sms_kielsa_job],
        schedules=[run_sms_kielsa_schedule],
    )
    log_debug(
        repr(defs.get_repository_def().get_schedule_def(run_sms_kielsa_schedule.name))
    )
