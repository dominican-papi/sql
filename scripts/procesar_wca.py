import pandas as pd
import sqlite3
import requests
import time
import zipfile
import os
import json

print("1. Obteniendo el link oficial del export de la WCA...")

api_url = "https://www.worldcubeassociation.org/api/v0/export/public"
headers = {'User-Agent': 'WCA-Data-Bot/1.0'}

# 1.1 Consultar a la API cuál es el link de hoy
try:
    respuesta_api = requests.get(api_url, headers=headers, timeout=15)
    respuesta_api.raise_for_status()
    datos_api = respuesta_api.json()
    
    # Extraemos el link exacto del archivo TSV
    url_descarga = datos_api['tsv_url']
    print(f"Link dinámico obtenido: {url_descarga}")
    
except Exception as e:
    print(f"Bobo obteniendo el link de la API: {e}")
    raise

print("1.2 Descargando el archivo de la base de datos...")
zip_path = "WCA_export.tsv.zip"
intentos = 3

# 1.2 Bucle de reintentos con el link nuevo
for i in range(intentos):
    try:
        respuesta_descarga = requests.get(url_descarga, headers=headers, timeout=60)
        respuesta_descarga.raise_for_status() 
        
        with open(zip_path, 'wb') as f:
            f.write(respuesta_descarga.content)
            
        print("Descarga completada con éxito.")
        break 
        
    except requests.exceptions.RequestException as e:
        print(f"Falló el intento {i + 1} de {intentos}. Error: {e}")
        if i < intentos - 1:
            print("Reintentando en 10 segundos...")
            time.sleep(10)
        else:
            print("No se pudo descargar el export.")
            raise

print("2. Descomprimiendo archivos necesarios...")
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    nombres_en_zip = zip_ref.namelist()
    print(f"Archivos encontrados en el ZIP: {nombres_en_zip}")
    
    # Buscamos dinámicamente los archivos correctos
    archivo_resultados = next((n for n in nombres_en_zip if 'Results' in n), None)
    archivo_personas = next((n for n in nombres_en_zip if 'Persons' in n), None)
    
    if not archivo_resultados or not archivo_personas:
        raise ValueError("¡Bobo! No se encontraron los archivos de Results o Persons en el ZIP.")
        
    zip_ref.extract(archivo_resultados)
    zip_ref.extract(archivo_personas)

print("3. Cargando datos a la base de datos SQLite temporal...")
conn = sqlite3.connect('wca.db')

# Usamos las variables dinámicas para leer los archivos
columnas_resultados = ['event_id', 'person_id', 'person_name', 'best', 'average', 'country_id']
df_resultados = pd.read_csv(archivo_resultados, sep='\t', usecols=columnas_resultados, low_memory=False)
df_resultados.to_sql('Results', conn, if_exists='replace', index=False)

columnas_personas = ['wca_id', 'name', 'sub_id']
df_personas = pd.read_csv(archivo_personas, sep='\t', usecols=columnas_personas, low_memory=False)
df_personas.to_sql('Persons', conn, if_exists='replace', index=False)

print("4. Ejecutando tu magia en SQL...")
mi_query = """
    SELECT 
        r.person_id AS wca_id,
        p.name AS nombre,
        MIN(r.best) AS mejor_single
    FROM Results r
    JOIN Persons p ON r.person_id = p.wca_id
    WHERE r.eventId = '333' AND r.best > 0 AND p.sub_id = 1 AND r.country_id = 'Dominican Republic'
    GROUP BY r.person_id
    ORDER BY mejor_single ASC
    LIMIT 50;
"""

resultados_finales = pd.read_sql_query(mi_query, conn)

print("5. Exportando a JSON para tu página web...")
os.makedirs('datos', exist_ok=True)
resultados_finales.to_json('datos/top_333.json', orient='records')

# Limpieza usando los nombres dinámicos
conn.close()
os.remove(zip_path)
os.remove(archivo_resultados)
os.remove(archivo_personas)

print("¡Proceso completado con éxito! JSON generado.")