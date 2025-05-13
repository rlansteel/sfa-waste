import sqlite3
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any
import logging
import argparse

# --- Configuración del Logging ---
# (Puedes ajustar el nivel y formato según necesites)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.StreamHandler() # Salida a consola
        # Opcionalmente, añade FileHandler si quieres log a archivo
        # logging.FileHandler("sfa_manage_db.log", mode='a', encoding='utf-8'),
    ]
)
logger = logging.getLogger("SFA_ManageWebFindingsDB")

# --- Definiciones SQL ---

SQL_CREATE_WEB_FINDINGS = """
CREATE TABLE IF NOT EXISTS web_findings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id INTEGER NOT NULL, -- FK a cities.id o countries.id
    entity_type TEXT NOT NULL CHECK(entity_type IN ('city', 'country')),
    seed_measurement_id INTEGER, -- FK opcional a city_measurements o country_measurements
    seed_source_text_or_url TEXT,
    query_type TEXT NOT NULL, -- ej. 'direct_url_access', 'source_description_search'
    query_text_used TEXT,
    finding_url TEXT NOT NULL,
    finding_domain TEXT,
    title TEXT,
    snippet_or_summary TEXT,
    content_retrieval_timestamp_utc TEXT NOT NULL, -- ISO 8601
    content_publication_date TEXT, -- ISO 8601 o YYYY-MM-DD
    source_evaluation_score REAL, -- Score de calidad/relevancia (ej. 0-100)
    source_evaluation_details_json TEXT, -- JSON con detalles de evaluación
    scraper_name TEXT, -- ej. 'SFA2_DirectURLValidator'
    scraper_native_score REAL, -- Score original de Tavily si aplica
    tags_json TEXT, -- JSON array de tags ["official report", "statistics"]
    processing_status TEXT NOT NULL, -- ej. 'retrieved', 'evaluated_relevant'
    error_message TEXT,
    data_source_methodology_hint TEXT, -- 'WB', 'WFD' del origen de la medición
    full_content_sha256 TEXT, -- Hash del contenido completo

    UNIQUE(finding_url) -- Evita duplicados de la misma URL
);
"""

SQL_CREATE_INDEX_WF_ENTITY = "CREATE INDEX IF NOT EXISTS idx_web_findings_entity ON web_findings (entity_id, entity_type);"
SQL_CREATE_INDEX_WF_URL = "CREATE INDEX IF NOT EXISTS idx_web_findings_url ON web_findings (finding_url);" # Para búsquedas UNIQUE
SQL_CREATE_INDEX_WF_STATUS = "CREATE INDEX IF NOT EXISTS idx_web_findings_status ON web_findings (processing_status);"
SQL_CREATE_INDEX_WF_DOMAIN = "CREATE INDEX IF NOT EXISTS idx_web_findings_domain ON web_findings (finding_domain);"

SQL_CREATE_WEB_LINKS = """
CREATE TABLE IF NOT EXISTS web_finding_measurement_links (
    link_id INTEGER PRIMARY KEY AUTOINCREMENT,
    finding_id INTEGER NOT NULL, -- FK a web_findings.id
    measurement_id INTEGER NOT NULL, -- ID de city_measurements o country_measurements
    measurement_table_type TEXT NOT NULL CHECK(measurement_table_type IN ('city', 'country')),
    link_timestamp TEXT NOT NULL, -- ISO 8601

    FOREIGN KEY (finding_id) REFERENCES web_findings (id) ON DELETE CASCADE,
    UNIQUE(finding_id, measurement_id, measurement_table_type) -- Evitar enlaces duplicados
);
"""

SQL_CREATE_INDEX_WL_FINDING = "CREATE INDEX IF NOT EXISTS idx_web_links_finding ON web_finding_measurement_links (finding_id);"
SQL_CREATE_INDEX_WL_MEASUREMENT = "CREATE INDEX IF NOT EXISTS idx_web_links_measurement ON web_finding_measurement_links (measurement_id, measurement_table_type);"


# --- Funciones ---

def setup_web_findings_tables(db_conn: sqlite3.Connection):
    """
    Crea las tablas 'web_findings' y 'web_finding_measurement_links' si no existen.
    """
    logger.info("Verificando/Creando tablas 'web_findings' y 'web_finding_measurement_links'...")
    try:
        cursor = db_conn.cursor()
        # Crear tabla web_findings e índices
        logger.debug("Creando tabla web_findings...")
        cursor.execute(SQL_CREATE_WEB_FINDINGS)
        logger.debug("Creando índices para web_findings...")
        cursor.execute(SQL_CREATE_INDEX_WF_ENTITY)
        cursor.execute(SQL_CREATE_INDEX_WF_URL)
        cursor.execute(SQL_CREATE_INDEX_WF_STATUS)
        cursor.execute(SQL_CREATE_INDEX_WF_DOMAIN)

        # Crear tabla web_finding_measurement_links e índices
        logger.debug("Creando tabla web_finding_measurement_links...")
        cursor.execute(SQL_CREATE_WEB_LINKS)
        logger.debug("Creando índices para web_finding_measurement_links...")
        cursor.execute(SQL_CREATE_INDEX_WL_FINDING)
        cursor.execute(SQL_CREATE_INDEX_WL_MEASUREMENT)

        db_conn.commit()
        logger.info("Tablas e índices para Web Findings verificados/creados exitosamente.")
    except sqlite3.Error as e:
        logger.error(f"Error al crear tablas/índices para Web Findings: {e}")
        db_conn.rollback()
        raise # Relanzar el error para que el SFA principal sepa que falló


def add_web_finding(conn: sqlite3.Connection, finding_data: Dict[str, Any]) -> Optional[int]:
    """
    Inserta o ignora un nuevo registro en web_findings. Si ya existe por finding_url,
    devuelve el ID existente. Si se inserta uno nuevo, devuelve el nuevo ID.
    Devuelve None en caso de error.

    Args:
        conn: Conexión a la base de datos SQLite.
        finding_data: Diccionario con los datos a insertar. Debe contener al menos 'finding_url'.
                      Las claves deben coincidir con los nombres de columna de la tabla.
                      Se añadirán automáticamente las columnas faltantes como NULL si no están en el dict.

    Returns:
        El ID (int) del hallazgo insertado o ya existente, o None si ocurre un error.
    """
    if 'finding_url' not in finding_data or not finding_data['finding_url']:
        logger.error("El campo 'finding_url' es obligatorio en finding_data para add_web_finding.")
        return None

    # Obtener columnas de la tabla dinámicamente (menos id)
    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(web_findings)")
        # Nombres de columnas válidas (excluyendo la PK 'id')
        valid_columns = {row[1] for row in cursor.fetchall() if row[1] != 'id'}
    except sqlite3.Error as e:
        logger.error(f"Error al obtener información de la tabla web_findings: {e}")
        return None

    # Preparar datos y columnas para el INSERT
    columns_to_insert = []
    values_to_insert = []

    # Asegurarse de que el timestamp de recuperación exista y esté en formato correcto
    finding_data.setdefault('content_retrieval_timestamp_utc', datetime.now(timezone.utc).isoformat())

    for col in valid_columns:
        if col in finding_data:
            value = finding_data[col]
            # Serializar JSON si es necesario (para tags_json, source_evaluation_details_json)
            if col.endswith('_json') and not isinstance(value, (str, type(None))):
                try:
                    values_to_insert.append(json.dumps(value))
                except TypeError:
                    logger.warning(f"No se pudo serializar el valor para '{col}', se guardará como string: {value}")
                    values_to_insert.append(str(value))
            else:
                values_to_insert.append(value)
            columns_to_insert.append(f'"{col}"') # Usar comillas dobles para nombres de columna
        # No añadir columnas que no están en finding_data (se insertarán como NULL por defecto)

    if not columns_to_insert:
        logger.error("No hay columnas válidas para insertar en finding_data.")
        return None

    sql_columns = ', '.join(columns_to_insert)
    sql_placeholders = ', '.join('?' * len(columns_to_insert))

    sql_insert_ignore = f"INSERT OR IGNORE INTO web_findings ({sql_columns}) VALUES ({sql_placeholders})"
    sql_select_id = "SELECT id FROM web_findings WHERE finding_url = ?"

    try:
        cursor = conn.cursor()
        cursor.execute(sql_insert_ignore, tuple(values_to_insert))
        inserted_rows = cursor.rowcount

        if inserted_rows > 0:
            finding_id = cursor.lastrowid
            logger.info(f"Nuevo hallazgo insertado en web_findings con ID: {finding_id} para URL: {finding_data['finding_url']}")
            conn.commit()
            return finding_id
        else:
            # El registro ya existía (INSERT OR IGNORE no hizo nada)
            cursor.execute(sql_select_id, (finding_data['finding_url'],))
            result = cursor.fetchone()
            if result:
                existing_id = result[0]
                logger.info(f"Hallazgo para URL {finding_data['finding_url']} ya existía en web_findings con ID: {existing_id}. No se reinsertó.")
                # NOTA: Aquí podríamos añadir lógica para *actualizar* el registro existente si quisiéramos
                # (ej. actualizar timestamp, score, etc.), pero por ahora solo devolvemos el ID.
                return existing_id
            else:
                # Esto sería inesperado si INSERT OR IGNORE no insertó y no podemos encontrarlo
                logger.error(f"Error crítico: INSERT OR IGNORE no insertó y no se pudo encontrar el ID para URL: {finding_data['finding_url']}")
                conn.rollback() # Deshacer el INSERT OR IGNORE por si acaso
                return None
    except sqlite3.Error as e:
        logger.error(f"Error de base de datos en add_web_finding para URL {finding_data.get('finding_url', 'N/A')}: {e}")
        conn.rollback()
        return None
    except Exception as e_gen:
        logger.error(f"Error inesperado en add_web_finding: {e_gen}")
        conn.rollback()
        return None


def add_measurement_link(conn: sqlite3.Connection, finding_id: int, measurement_id: int, measurement_table_type: str) -> bool:
    """
    Inserta o ignora un nuevo registro en web_finding_measurement_links para asociar
    un hallazgo web con una medición específica.

    Args:
        conn: Conexión a la base de datos SQLite.
        finding_id: El ID del registro en web_findings.
        measurement_id: El ID del registro en city_measurements o country_measurements.
        measurement_table_type: 'city' o 'country' para indicar a qué tabla pertenece measurement_id.

    Returns:
        True si el enlace se creó o ya existía, False en caso de error.
    """
    if measurement_table_type not in ['city', 'country']:
        logger.error(f"Tipo de tabla de medición inválido: '{measurement_table_type}'. Debe ser 'city' o 'country'.")
        return False

    link_timestamp = datetime.now(timezone.utc).isoformat()
    sql = """
        INSERT OR IGNORE INTO web_finding_measurement_links
        (finding_id, measurement_id, measurement_table_type, link_timestamp)
        VALUES (?, ?, ?, ?)
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql, (finding_id, measurement_id, measurement_table_type, link_timestamp))
        inserted_rows = cursor.rowcount
        conn.commit()
        if inserted_rows > 0:
            logger.info(f"Nuevo enlace creado entre finding_id={finding_id} y measurement_id={measurement_id} (type={measurement_table_type}).")
        else:
            logger.info(f"Enlace entre finding_id={finding_id} y measurement_id={measurement_id} (type={measurement_table_type}) ya existía.")
        return True
    except sqlite3.Error as e:
        logger.error(f"Error de base de datos en add_measurement_link (finding={finding_id}, meas={measurement_id}, type={measurement_table_type}): {e}")
        conn.rollback()
        return False
    except Exception as e_gen:
        logger.error(f"Error inesperado en add_measurement_link: {e_gen}")
        conn.rollback()
        return False

# --- Bloque Principal (Opcional, para ejecución directa de setup) ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script para gestionar (crear) las tablas de Web Findings en la BD.")
    parser.add_argument("--db-file", default="output/db/waste_data_clean.db", help="Ruta al archivo de la base de datos SQLite (default: output/db/waste_data_clean.db).")
    # Podríamos añadir más argumentos si quisiéramos que este script hiciera más cosas

    args = parser.parse_args()
    db_path = Path(args.db_file)

    conn = None
    try:
        logger.info(f"Intentando conectar a la base de datos: {db_path}")
        if not db_path.parent.exists():
            logger.info(f"Creando directorio para la base de datos: {db_path.parent}")
            db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(db_path)
        logger.info("Conexión establecida.")

        # Ejecutar la configuración de las tablas
        setup_web_findings_tables(conn)

    except sqlite3.Error as e:
        logger.error(f"No se pudo conectar o configurar la base de datos en {db_path}: {e}")
    except Exception as e:
        logger.error(f"Error inesperado en el script principal: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("Conexión a la base de datos cerrada.")