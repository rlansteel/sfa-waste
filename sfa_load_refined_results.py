# /// script
# dependencies = [
#   "rich>=13.7.0",
#   "python-dotenv>=0.20.0", # Aunque no usemos keys aquí, es bueno tenerlo por consistencia
# ]
# ///

import argparse
import sqlite3
from pathlib import Path
import logging
import json
from typing import Optional, Dict, Any, List
import os
import sys
from datetime import datetime, timezone

# Importar funciones del script de gestión de BD
try:
    # Asume que sfa_manage_web_findings_db.py está en el mismo directorio
    # Necesitamos setup_tables y add_web_finding
    from sfa_manage_web_findings_db import setup_web_findings_tables, add_web_finding
except ImportError:
    print("ERROR: No se pudo importar desde 'sfa_manage_web_findings_db.py'. Asegúrate de que el archivo existe y está en el PYTHONPATH.")
    sys.exit(1)

# --- Configuración del Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger("SFA_LoadRefinedResults")

# --- Configuración Visual (Rich) ---
try:
    from rich.console import Console
    from rich.progress import track
    console = Console(stderr=True)
    USE_RICH = True
except ImportError:
    USE_RICH = False
    def track(sequence, description="Procesando..."):
        """Fallback simple para track si rich no está instalado."""
        total = len(sequence) if hasattr(sequence, '__len__') else None
        for i, item in enumerate(sequence):
            if total:
                print(f"{description} {i+1}/{total}", end='\r', file=sys.stderr)
            else:
                 print(f"{description} {i+1}", end='\r', file=sys.stderr)
            yield item
        print(file=sys.stderr) # Nueva línea al final

# --- Constantes ---
MAX_SNIPPET_LENGTH_DB = 1500 # Longitud máxima para guardar en snippet_or_summary
# --- CORRECCIÓN: Añadir constante TARGET_METHODOLOGY ---
TARGET_METHODOLOGY = 'WB' # Asumimos que los hallazgos se asocian a la metodología WB por ahora
# --- FIN CORRECCIÓN ---

# --- Funciones ---

def create_db_connection(db_path_str: str) -> Optional[sqlite3.Connection]:
    """Crea una conexión a la base de datos SQLite."""
    db_path = Path(db_path_str)
    logger.info(f"Conectando a la base de datos: {db_path}")
    if not db_path.exists():
        logger.error(f"El archivo de base de datos '{db_path}' no existe.")
        return None
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        logger.info("Conexión a BD establecida.")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error al conectar a la base de datos: {e}")
        return None

def get_country_db_id(conn: sqlite3.Connection, country_iso3: str) -> Optional[int]:
    """Obtiene el ID interno de un país desde la tabla 'countries' usando su ISO3."""
    logger.debug(f"Buscando ID para país ISO3: {country_iso3}")
    sql = "SELECT id FROM countries WHERE country_code_iso3 = ? LIMIT 1"
    try:
        cursor = conn.cursor()
        cursor.execute(sql, (country_iso3.upper(),))
        result = cursor.fetchone()
        if result:
            country_id = result['id']
            logger.debug(f"ID encontrado para {country_iso3}: {country_id}")
            return country_id
        else:
            logger.warning(f"No se encontró ID en la tabla 'countries' para ISO3: {country_iso3}")
            return None
    except sqlite3.Error as e:
        logger.error(f"Error buscando ID de país para {country_iso3}: {e}")
        return None

def extract_domain(url: str) -> Optional[str]:
    """Extrae el dominio principal de una URL."""
    try:
        from urllib.parse import urlparse
        parsed_uri = urlparse(url)
        domain = '{uri.netloc}'.format(uri=parsed_uri)
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain.lower()
    except Exception:
        return None

# --- Lógica Principal ---
def main():
    parser = argparse.ArgumentParser(description="Carga resultados de búsquedas refinadas (JSONs) a la tabla web_findings.")
    parser.add_argument("--results-dir", required=True, help="Directorio que contiene los archivos JSON de resultados generados por run_refined_queries.py.")
    parser.add_argument("--db-file", default="output/waste_data.db", help="Ruta a la BD SQLite.")
    parser.add_argument("--country-iso3", required=True, help="Código ISO 3166-1 alfa-3 del país al que pertenecen estos resultados.")

    args = parser.parse_args()

    results_dir_path = Path(args.results_dir)
    if not results_dir_path.is_dir():
        logger.critical(f"Directorio de resultados no encontrado: {results_dir_path}")
        sys.exit(1)

    conn = create_db_connection(args.db_file)
    if conn is None:
        sys.exit(1)

    try:
        setup_web_findings_tables(conn)
    except Exception:
        logger.critical("Fallo al verificar/crear las tablas de la BD. Abortando.")
        conn.close()
        sys.exit(1)

    country_db_id = get_country_db_id(conn, args.country_iso3)
    if country_db_id is None:
        logger.error(f"No se pudo encontrar el país {args.country_iso3} en la base de datos. No se pueden cargar los hallazgos.")
        conn.close()
        sys.exit(1)

    json_files = list(results_dir_path.glob("*.json"))
    if not json_files:
        logger.warning(f"No se encontraron archivos .json en el directorio: {results_dir_path}")
        conn.close()
        sys.exit(0)

    logger.info(f"Procesando {len(json_files)} archivos JSON desde {results_dir_path.name}...")

    total_reports_processed = 0
    total_reports_added = 0 # Cuenta inserciones + encontrados
    total_files_failed = 0

    iterator = track(json_files, description="Cargando resultados...") if USE_RICH else json_files

    for json_file in iterator:
        logger.debug(f"Procesando archivo: {json_file.name}")
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            reports = data.get("reports", [])
            if not reports:
                logger.warning(f"Archivo {json_file.name} no contiene 'reports' o la lista está vacía.")
                continue

            logger.debug(f"Encontrados {len(reports)} reportes en {json_file.name}")

            for report in reports:
                total_reports_processed += 1
                url = report.get("url")
                if not url:
                    logger.warning(f"Reporte sin URL en {json_file.name}, omitiendo: {report.get('title', 'Sin título')}")
                    continue

                content = report.get('raw_content') or report.get('content') or ""
                snippet = content[:MAX_SNIPPET_LENGTH_DB]
                if len(content) > MAX_SNIPPET_LENGTH_DB:
                    snippet += "..."

                finding_data = {
                    "entity_id": country_db_id,
                    "entity_type": "country", # Asumiendo que son para el país
                    "seed_measurement_id": None,
                    "seed_source_text_or_url": None,
                    "query_type": "refined_gap_filling_search",
                    "query_text_used": f"Refined search leading to: {url}", # Placeholder
                    "finding_url": url,
                    "finding_domain": extract_domain(url),
                    "title": report.get("title"),
                    "snippet_or_summary": snippet,
                    "content_retrieval_timestamp_utc": data.get("metadata", {}).get("collection_date", datetime.now(timezone.utc).isoformat()),
                    "content_publication_date": report.get("date") if report.get("date") != "Unknown" else None,
                    "scraper_name": data.get("metadata", {}).get("agent_name", "SFA_Tavily_Scraper"),
                    "scraper_native_score": None,
                    "source_evaluation_score": None,
                    "source_evaluation_details_json": None,
                    "processing_status": "content_retrieved_pending_analysis",
                    "error_message": None,
                    # --- CORRECCIÓN: Usar la constante definida ---
                    "data_source_methodology_hint": TARGET_METHODOLOGY
                }

                finding_id = add_web_finding(conn, finding_data)
                if finding_id is not None:
                    total_reports_added += 1
                else:
                    logger.error(f"Fallo al añadir/encontrar hallazgo para URL {url} del archivo {json_file.name}")

        except json.JSONDecodeError as e:
            logger.error(f"Error decodificando JSON en {json_file.name}: {e}")
            total_files_failed += 1
        except Exception as e:
            logger.error(f"Error procesando archivo {json_file.name}: {e}", exc_info=True)
            total_files_failed += 1

    # Commit final
    try:
        conn.commit()
        logger.info("Commit final realizado.")
    except sqlite3.Error as e:
        logger.error(f"Error en commit final: {e}")

    logger.info("--- Carga de Resultados Refinados Completada ---")
    logger.info(f"Archivos JSON procesados: {len(json_files)}")
    logger.info(f"Reportes totales encontrados en JSONs: {total_reports_processed}")
    logger.info(f"Reportes añadidos/encontrados en web_findings: {total_reports_added}")
    logger.info(f"Archivos JSON con errores: {total_files_failed}")

    if conn:
        conn.close()
        logger.info("Conexión a la base de datos cerrada.")

if __name__ == "__main__":
    main()
