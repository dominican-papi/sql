import pandas as pd
import sqlite3
import urllib.request
import zipfile
import os
import json

print("1. Descargando el export de la WCA...")
url = "https://www.worldcubeassociation.org/results/misc/WCA_export.tsv.zip"
zip_path = "WCA_export.tsv.zip"
urllib.request.urlretrieve(url, zip_path)

print("2. Descomprimiendo archivos necesarios...")
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extract('WCA_export_Results.tsv')
    zip_ref.extract('WCA_export_Persons.tsv')

print("3. Cargando datos a la base de datos SQLite temporal...")
# Conectar a la base de datos (se crea un archivo local wca.db)
conn = sqlite3.connect('wca.db')

# Leer los TSV y meterlos a SQLite. 
# Nota: Limitamos las columnas para que GitHub no se quede sin memoria.
columnas_resultados = ['event_id', 'person_id', 'person_name', 'best', 'average', 'country_id']
df_resultados = pd.read_csv('WCA_export_Results.tsv', sep='\t', usecols=columnas_resultados, low_memory=False)
df_resultados.to_sql('Results', conn, if_exists='replace', index=False)

columnas_personas = ['wca_id', 'name', 'sub_id']
df_personas = pd.read_csv('WCA_export_Persons.tsv', sep='\t', usecols=columnas_personas, low_memory=False)
df_personas.to_sql('Persons', conn, if_exists='replace', index=False)

print("4. Ejecutando tu magia en SQL...")
# Aquí va tu query. Agrupamos por person_id (WCA ID) para evitar errores con nombres.
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

# Ejecutar el query y guardar el resultado en un nuevo DataFrame
resultados_finales = pd.read_sql_query(mi_query, conn)

print("5. Exportando a JSON para tu página web...")
# Crear la carpeta de salida si no existe
os.makedirs('datos', exist_ok=True)

# Guardar como JSON (orient='records' crea una lista de objetos perfecta para JS)
resultados_finales.to_json('datos/top_333.json', orient='records')

# Limpieza (borrar archivos pesados que ya no sirven)
conn.close()
os.remove(zip_path)
os.remove('WCA_export_Results.tsv')
os.remove('WCA_export_Persons.tsv')

print("¡Proceso completado con éxito! JSON generado.")