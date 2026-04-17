import pandas as pd
import sqlite3
import requests
import time
import zipfile
import os

# ==========================================================
# 1. OBTENIENDO EL LINK Y DESCARGANDO (NUEVO BLOQUE DINÁMICO)
# ==========================================================
print("1. Obteniendo el link oficial del export de la WCA...")
api_url = "https://www.worldcubeassociation.org/api/v0/export/public"
headers = {'User-Agent': 'WCA-Data-Bot/1.0'}

try:
    respuesta_api = requests.get(api_url, headers=headers, timeout=15)
    respuesta_api.raise_for_status()
    url_descarga = respuesta_api.json()['tsv_url']
    print(f"Link dinámico obtenido: {url_descarga}")
except Exception as e:
    print(f"Bobo obteniendo el link de la API: {e}")
    raise

print("1.2 Descargando el archivo de la base de datos...")
zip_path = "WCA_export.tsv.zip"
intentos = 3

for i in range(intentos):
    try:
        respuesta_descarga = requests.get(url_descarga, headers=headers, timeout=60)
        respuesta_descarga.raise_for_status() 
        with open(zip_path, 'wb') as f:
            f.write(respuesta_descarga.content)
        print("Descarga completada con éxito.")
        break 
    except requests.exceptions.RequestException as e:
        print(f"Falló el intento {i + 1}. Error: {e}")
        if i < intentos - 1:
            time.sleep(10)
        else:
            raise

# ==========================================================
# 2. DESCOMPRIMIENDO LOS ARCHIVOS (IGNORANDO MAYÚSCULAS)
# ==========================================================
print("2. Descomprimiendo archivos necesarios...")
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    nombres_en_zip = zip_ref.namelist()
    archivo_resultados = next((n for n in nombres_en_zip if 'export_results.tsv' in n.lower()), None)
    archivo_personas = next((n for n in nombres_en_zip if 'export_persons.tsv' in n.lower()), None)
    
    if not archivo_resultados or not archivo_personas:
        raise ValueError("¡Bobo! No se encontraron los archivos de Results o Persons en el ZIP.")
        
    zip_ref.extract(archivo_resultados)
    zip_ref.extract(archivo_personas)

# ==========================================================
# 3. FILTRANDO A RD Y AHORRANDO MEMORIA (CHUNKING)
# ==========================================================
print("3. Filtrando a los tigueres de RD para ahorrar memoria...")
conn = sqlite3.connect('wca.db')

# 3.1 Cargar y filtrar PERSONAS
df_personas = pd.read_csv(archivo_personas, sep='\t', low_memory=False)
# Filtramos solo RD y representantes oficiales (sub_id = 1)
df_rd = df_personas[(df_personas['country_id'] == 'Dominican Republic') & (df_personas['sub_id'] == 1)]
df_rd.to_sql('Persons', conn, if_exists='replace', index=False)

# Guardamos los IDs dominicanos en un set para búsqueda ultra rápida
wca_ids_dominicanos = set(df_rd['wca_id'])
print(f"Encontramos {len(wca_ids_dominicanos)} competidores dominicanos.")

# 3.2 Cargar y filtrar RESULTADOS por pedazos (Chunking)
columnas_res = ['event_id', 'person_id', 'best', 'average', 'pos', 'round_type_id']
# Leemos de 100,000 en 100,000 líneas para no explotar la memoria del GitHub Action
for chunk in pd.read_csv(archivo_resultados, sep='\t', usecols=columnas_res, chunksize=100000, low_memory=False):
    # Solo nos quedamos con las filas donde el person_id (WCA ID) es de un dominicano
    chunk_filtrado = chunk[chunk['person_id'].isin(wca_ids_dominicanos)]
    # Lo vamos agregando a la base de datos SQLite
    chunk_filtrado.to_sql('Results', conn, if_exists='append', index=False)

# ==========================================================
# 4. EJECUTANDO TU MAGIA EN SQL
# ==========================================================
print("4. Ejecutando los queries pesados...")
mis_queries = {
    
    # --- QUERY 1: 3x3 SINGLE ---
    'datos/top_333.json': """
        SELECT r.person_id AS wca_id, p.name AS nombre, MIN(r.best) AS mejor_single
        FROM Results r
        JOIN Persons p ON r.person_id = p.wca_id
        WHERE r.event_id = '333' AND r.best > 0
        GROUP BY r.person_id
        ORDER BY mejor_single ASC LIMIT 50;
    """,
    
    # --- QUERY 2: MEDALLAS ---
    'datos/medallas.json': """
        SELECT r.person_id AS wca_id, p.name AS nombre, 
            SUM(CASE WHEN r.pos = 1 THEN 1 ELSE 0 END) AS Golds, 
            SUM(CASE WHEN r.pos = 2 THEN 1 ELSE 0 END) AS Silvers, 
            SUM(CASE WHEN r.pos = 3 THEN 1 ELSE 0 END) AS Bronzes, 
            SUM(CASE WHEN r.pos <= 3 THEN 1 ELSE 0 END) AS Total
        FROM Results r
        JOIN Persons p ON r.person_id = p.wca_id
        WHERE r.round_type_id IN ('c', 'f') AND r.best > 0
        GROUP BY r.person_id, p.name
        HAVING Total > 0
        ORDER BY Golds DESC, Silvers DESC, Bronzes DESC, nombre ASC LIMIT 50;
    """,

    # --- QUERY 3: SUM OF RANKS (SINGLE) ---
    'datos/sor_single.json': """
        WITH Base AS (
            SELECT r.person_id wca_id, p.name nombre, r.event_id, MIN(r.best) Mejor
            FROM Results r
            JOIN Persons p ON r.person_id = p.wca_id
            WHERE r.best > 0
            GROUP BY r.person_id, p.name, r.event_id
        ),
        Rankings AS (
            SELECT wca_id, event_id, RANK() OVER (PARTITION BY event_id ORDER BY Mejor ASC) AS Posicion
            FROM Base
        ),
        Eventos AS (
            SELECT event_id, COUNT(*) Total_Gente FROM Base GROUP BY event_id
        ),
        TotalEventos AS (
            SELECT SUM(Total_Gente + 1) AS SumaTotal FROM Eventos
        ),
        s AS (
            SELECT r.wca_id, SUM(r.Posicion) AS SumaPosiciones, SUM(e.Total_Gente + 1) AS SumaEventosJugados, COUNT(*) AS Eventos
            FROM Rankings r
            JOIN Eventos e ON r.event_id = e.event_id
            GROUP BY r.wca_id
        )
        SELECT s.wca_id, p.nombre, (s.SumaPosiciones + t.SumaTotal - s.SumaEventosJugados) AS sor_single, s.Eventos
        FROM s
        JOIN TotalEventos t ON 1=1
        JOIN (SELECT DISTINCT wca_id, nombre FROM Base) p ON s.wca_id = p.wca_id
        ORDER BY sor_single ASC, s.Eventos DESC, p.nombre ASC LIMIT 50;
    """,
    
    # --- QUERY 4: KINCH RANKS (NR) ---
    'datos/kinch_nr.json': """
        WITH TiemposBase AS (
            SELECT person_id AS wca_id, event_id, 
                   MIN(CASE WHEN event_id IN ('333bf', '333fm', '444bf', '555bf', '333mbf') THEN best ELSE average END) AS Tu_Tiempo
            FROM Results
            WHERE (CASE WHEN event_id IN ('333bf', '333fm', '444bf', '555bf', '333mbf') THEN best ELSE average END) > 0
            GROUP BY person_id, event_id
        ),
        PuntosKinch AS (
            SELECT wca_id, event_id, 
                   (MIN(Tu_Tiempo) OVER(PARTITION BY event_id) * 1.0 / Tu_Tiempo) * 100.0 AS Puntos 
            FROM TiemposBase
        ),
        KinchTotal AS (
            SELECT wca_id, CAST(SUM(Puntos) / 17.0 AS DECIMAL(5,2)) AS Kinch, COUNT(event_id) AS Eventos
            FROM PuntosKinch
            GROUP BY wca_id
        )
        SELECT k.wca_id, p.name AS nombre, k.Kinch AS kinch_score, k.Eventos AS eventos_jugados
        FROM KinchTotal k
        JOIN Persons p ON k.wca_id = p.wca_id
        ORDER BY k.Kinch DESC, k.Eventos DESC, p.name ASC LIMIT 50;
    """
}

# ==========================================================
# 5. EXPORTANDO A JSON Y LIMPIANDO
# ==========================================================
print("5. Exportando a JSON para la página web...")
os.makedirs('datos', exist_ok=True)

for ruta, query in mis_queries.items():
    print(f"Generando {ruta}...")
    pd.read_sql_query(query, conn).to_json(ruta, orient='records')

print("6. Limpieza del servidor...")
conn.close()
os.remove(zip_path)
os.remove(archivo_resultados)
os.remove(archivo_personas)

print("¡Proceso completado nítido! Todos los archivos JSON fueron generados.")
