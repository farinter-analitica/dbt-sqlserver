import requests
import polars as pl
from datetime import datetime, timedelta
import json
from deep_translator import GoogleTranslator

# Función para obtener los días festivos para una lista de países y un rango de fechas específico
def obtener_dias_festivos(codigos_pais, fecha_inicio, fecha_fin):
    datos_festivos = []
    
    # Inicializar el traductor a español
    traductor = GoogleTranslator(source='auto', target='es')
    
    for codigo_pais in codigos_pais:
        # URL base para la API de Nager.Date
        url_base = f"https://date.nager.at/api/v3/publicholidays/{datetime.now().year}/{codigo_pais}"
        
        # Realizar la solicitud a la API
        respuesta = requests.get(url_base)
        
        # Verificar si la solicitud fue exitosa
        if respuesta.status_code == 200:
            dias_festivos = respuesta.json()
            
            # Filtrar los días festivos por el rango de fechas especificado
            for dia_festivo in dias_festivos:
                fecha_dia_festivo = dia_festivo['date']
                if fecha_inicio <= fecha_dia_festivo <= fecha_fin:
                    # Traducir el nombre del día festivo al español
                    motivo_es = traductor.translate(dia_festivo['localName'])
                    
                    datos_festivos.append({
                        'Fecha_Id': fecha_dia_festivo,
                        'Motivo': motivo_es,
                        'Json_Sociedades_Id': '["*"]',  # Marcador de posición ya que no se proporciona información de sociedades
                        'Json_Paises_Id': json.dumps([codigo_pais])
                    })
        else:
            print(f"Error al obtener datos para {codigo_pais}: {respuesta.status_code}")
    
    # Convertir a DataFrame de Polars para manejo eficiente
    df_festivos = pl.DataFrame(datos_festivos)
    
    # Asegurar que la fecha sea única, combinando los países en una lista y asegurando el motivo en español
    df_festivos = df_festivos.group_by('Fecha_Id').agg([
        pl.col('Motivo').first().alias('Motivo'),
        pl.col('Json_Sociedades_Id').first().alias('Json_Sociedades_Id'),
        pl.col('Json_Paises_Id').map_elements(lambda x: json.dumps(list({pais for lista in x for pais in json.loads(lista)}))).alias('Json_Paises_Id')
    ])
    
    return df_festivos

# Ejemplo de uso
codigos_pais = ["HN", "NI", "CR", "SV", "GT"]  # Lista de códigos ISO2 de los países
fecha_inicio = datetime.now().strftime('%Y-%m-%d')  # Formato YYYY-MM-DD
fecha_fin = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')     # Formato YYYY-MM-DD

# Obtener y mostrar los días festivos
df_festivos = obtener_dias_festivos(codigos_pais, fecha_inicio, fecha_fin)
if df_festivos is not None:
    print(df_festivos)

    # Ejemplo: Guardar el DataFrame en un archivo CSV que pueda cargarse en la tabla SQL
    #df_festivos.write_csv("DL_Edit_CalendarioNoLaboral.csv")
