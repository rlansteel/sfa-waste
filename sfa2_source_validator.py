# sfa2_source_validator.py
# /// script
# dependencies = [
#   "rich>=13.7.0",
#   "requests>=2.25.1",
#   "beautifulsoup4>=4.9.3", # Para parsear HTML
#   "pypdf2>=3.0.0",         # Para leer PDFs
#   "tavily-python>=0.3.0",  # Cliente Tavily
#   "python-dotenv>=0.20.0"
# ]
# ///

import argparse
import sqlite3
from pathlib import Path
import logging
import time
import json
import re
import requests
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
import io
from urllib.parse import urlparse, urljoin
from datetime import datetime, timezone
import hashlib
from typing import Optional, Dict, Any, List, Tuple
import os 
import sys 

try:
    from sfa_manage_web_findings_db import add_web_finding, setup_web_findings_tables
except ImportError:
    print("ERROR CRÍTICO: No se pudo importar desde 'sfa_manage_web_findings_db.py'.")
    print("Asegúrate de que el archivo 'sfa_manage_web_findings_db.py' existe y es accesible.")
    sys.exit(1) 

LOG_LEVEL_DEFAULT = "INFO"
logger = logging.getLogger("SFA2_SourceValidator")
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
log_handler = logging.StreamHandler(sys.stderr) 
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - [%(funcName)s:%(lineno)d] - %(message)s')
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)

USER_AGENT = "SFA2SourceValidator/1.0 (WasteDataProject; contact@example.com)" 
REQUEST_TIMEOUT = 20  
TAVILY_API_KEY_ENV_VAR = "TAVILY_API_KEY" 
MAX_PDF_PAGES_TO_READ = 5 
MAX_CONTENT_LENGTH_FOR_SNIPPET = 2000
# Nueva constante para la longitud máxima del texto de la fuente en la query de Tavily
MAX_SOURCE_TEXT_IN_TAVILY_QUERY = 250 

def get_tavily_client():
    api_key = os.getenv(TAVILY_API_KEY_ENV_VAR)
    if not api_key:
        logger.warning(f"Variable de entorno {TAVILY_API_KEY_ENV_VAR} no encontrada. La búsqueda con Tavily no funcionará.")
        return None
    try:
        from tavily import TavilyClient
        return TavilyClient(api_key=api_key)
    except ImportError:
        logger.error("La librería 'tavily-python' no está instalada. No se puede usar Tavily.")
        return None
    except Exception as e:
        logger.error(f"Error inicializando TavilyClient: {e}")
        return None

def is_valid_url(url_string: str) -> bool:
    if not isinstance(url_string, str):
        return False
    try:
        result = urlparse(url_string)
        return all([result.scheme in ['http', 'https'], result.netloc])
    except ValueError:
        return False

def download_content(url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    logger.debug(f"Intentando descargar contenido de URL: {url}")
    headers = {'User-Agent': USER_AGENT}
    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        response.raise_for_status()  

        content_type_header = response.headers.get('Content-Type', '').lower()
        logger.debug(f"Content-Type recibido: {content_type_header}")

        if 'application/pdf' in content_type_header:
            logger.info(f"Detectado PDF en {url}. Procesando...")
            try:
                pdf_file = io.BytesIO(response.content)
                reader = PdfReader(pdf_file)
                text_parts = []
                num_pages_to_read = min(len(reader.pages), MAX_PDF_PAGES_TO_READ)
                for i in range(num_pages_to_read):
                    try:
                        page_text = reader.pages[i].extract_text()
                        if page_text:
                            text_parts.append(page_text.strip())
                    except Exception as page_err:
                        logger.warning(f"Error extrayendo texto de página {i+1} de PDF {url}: {page_err}")
                full_text = "\n".join(text_parts)
                if not full_text and len(reader.pages) > 0:
                    logger.warning(f"PDF {url} no produjo texto extraíble.")
                logger.info(f"Texto extraído de PDF {url} (primeras {num_pages_to_read} páginas). Longitud: {len(full_text)}")
                return full_text, 'pdf', None 
            except Exception as e_pdf:
                logger.error(f"Error procesando PDF de {url}: {e_pdf}")
                return None, 'pdf_error', str(e_pdf)

        elif 'text/html' in content_type_header:
            logger.info(f"Detectado HTML en {url}. Parseando...")
            soup = BeautifulSoup(response.content, 'html.parser')
            
            for script_or_style in soup(["script", "style"]):
                script_or_style.decompose()
            
            title_tag = soup.find('title')
            title = title_tag.string.strip() if title_tag else None

            body = soup.find('body')
            text_content = body.get_text(separator='\n', strip=True) if body else soup.get_text(separator='\n', strip=True)
            
            logger.info(f"Texto extraído de HTML {url}. Longitud: {len(text_content)}")
            return text_content, 'html', title 
        
        elif 'text/plain' in content_type_header or 'application/json' in content_type_header or 'application/xml' in content_type_header:
            logger.info(f"Detectado texto plano/JSON/XML en {url}.")
            return response.text, 'text', None

        else:
            logger.warning(f"Tipo de contenido no soportado directamente '{content_type_header}' para {url}. Se intentará leer como texto.")
            try:
                return response.text, 'text_unknown_type', None
            except Exception as e_text_fallback:
                 logger.error(f"Fallo al leer como texto contenido desconocido de {url}: {e_text_fallback}")
                 return None, 'unknown_type_error', str(e_text_fallback)

    except requests.exceptions.RequestException as e:
        logger.error(f"Error de red/HTTP descargando {url}: {e}")
        return None, 'network_error', str(e)
    except Exception as e_gen:
        logger.error(f"Error inesperado descargando/procesando {url}: {e_gen}")
        return None, 'general_download_error', str(e_gen)

def search_with_tavily(query_text: str, tavily_client) -> List[Dict[str, Any]]:
    if not tavily_client:
        logger.info("Cliente Tavily no disponible. Omitiendo búsqueda.")
        return []
    
    logger.info(f"Buscando con Tavily: '{query_text}'")
    try:
        response = tavily_client.search(
            query=query_text,
            search_depth="advanced", 
            include_answer=False,   
            include_raw_content=True,
            max_results=3            
        )
        
        results = []
        if response and response.get("results"):
            for res in response["results"]:
                results.append({
                    "url": res.get("url"),
                    "title": res.get("title"),
                    "content": res.get("raw_content") or res.get("content"), 
                    "score": res.get("score", 0.0) 
                })
            logger.info(f"Tavily encontró {len(results)} resultados para '{query_text}'.")
        else:
            logger.info(f"Tavily no encontró resultados para '{query_text}'.")
        return results
    except Exception as e:
        logger.error(f"Error durante la búsqueda con Tavily para '{query_text}': {e}")
        return []

def extract_domain(url: str) -> Optional[str]:
    if not url or not isinstance(url, str): return None
    try:
        return urlparse(url).netloc.lower().replace('www.', '')
    except Exception:
        return None

def get_sha256_hash(text_content: Optional[str]) -> Optional[str]:
    if text_content is None:
        return None
    return hashlib.sha256(text_content.encode('utf-8')).hexdigest()

def process_sources_for_country(db_conn: sqlite3.Connection, country_iso3_filter: str, tavily_client, args):
    logger.info(f"Iniciando procesamiento de fuentes para país: {country_iso3_filter.upper()}")
    
    cursor = db_conn.cursor()
    
    cursor.execute("SELECT id, country_name FROM countries WHERE country_code_iso3 = ?", (country_iso3_filter.upper(),))
    country_db_record = cursor.fetchone()
    if not country_db_record:
        logger.error(f"País con ISO3 '{country_iso3_filter.upper()}' no encontrado en la tabla 'countries'.")
        return
    country_db_id = country_db_record['id']
    country_db_name = country_db_record['country_name']
    logger.debug(f"País DB ID: {country_db_id}, Nombre: {country_db_name}")

    logger.info(f"Buscando mediciones para el país {country_iso3_filter.upper()}...")
    query_country_meas = """
        SELECT id, source 
        FROM country_measurements
        WHERE country_iso3c = ? AND source IS NOT NULL AND source != ''
    """
    cursor.execute(query_country_meas, (country_iso3_filter.upper(),))
    country_measurements_sources = cursor.fetchall()
    logger.info(f"Encontradas {len(country_measurements_sources)} mediciones de país con campo 'source'.")

    for meas_row in country_measurements_sources:
        process_single_measurement_source(
            db_conn=db_conn,
            measurement_id=meas_row['id'],
            source_text_or_url=meas_row['source'],
            entity_id=country_db_id, 
            entity_type='country',
            entity_name_context=country_db_name, 
            country_context=country_db_name, 
            tavily_client=tavily_client,
            methodology_hint=None, 
            args=args
        )

    logger.info(f"Buscando mediciones para ciudades en {country_iso3_filter.upper()}...")
    query_city_meas = """
        SELECT cm.id, cm.source, c.data_source_methodology AS city_methodology_hint,
               c.id as city_db_id, c.municipality as city_name
        FROM city_measurements cm
        JOIN cities c ON cm.city_id = c.id
        WHERE c.iso3c = ? AND cm.source IS NOT NULL AND cm.source != ''
    """
    cursor.execute(query_city_meas, (country_iso3_filter.upper(),))
    city_measurements_sources = cursor.fetchall()
    logger.info(f"Encontradas {len(city_measurements_sources)} mediciones de ciudad con campo 'source' para el país.")

    for meas_row in city_measurements_sources:
        city_methodology_hint_value = meas_row['city_methodology_hint'] if 'city_methodology_hint' in meas_row.keys() else None
        process_single_measurement_source(
            db_conn=db_conn,
            measurement_id=meas_row['id'],
            source_text_or_url=meas_row['source'],
            entity_id=meas_row['city_db_id'], 
            entity_type='city',
            entity_name_context=meas_row['city_name'], 
            country_context=country_db_name, 
            tavily_client=tavily_client,
            methodology_hint=city_methodology_hint_value, 
            args=args
        )

    logger.info(f"Procesamiento de fuentes para {country_iso3_filter.upper()} completado.")


def process_single_measurement_source(db_conn: sqlite3.Connection, measurement_id: int, source_text_or_url: str,
                                      entity_id: int, entity_type: str, 
                                      entity_name_context: str, country_context: str, 
                                      tavily_client, methodology_hint: Optional[str], args):
    logger.info(f"Procesando source para measurement_id={measurement_id} ({entity_type}), entidad_id={entity_id}: '{source_text_or_url[:100]}...'")

    cursor_check = db_conn.cursor()
    
    force_recheck_flag = False
    if args.force_recheck_sources_days > 0:
        cursor_check.execute("""
            SELECT content_retrieval_timestamp_utc FROM web_findings 
            WHERE seed_measurement_id = ? AND entity_type = ? AND seed_source_text_or_url = ?
            ORDER BY content_retrieval_timestamp_utc DESC LIMIT 1
        """, (measurement_id, entity_type, source_text_or_url))
        last_check_row = cursor_check.fetchone()
        if last_check_row and last_check_row['content_retrieval_timestamp_utc']:
            try:
                last_check_dt = datetime.fromisoformat(last_check_row['content_retrieval_timestamp_utc'].replace('Z', '+00:00'))
                if (datetime.now(timezone.utc) - last_check_dt).days > args.force_recheck_sources_days:
                    force_recheck_flag = True
                    logger.info(f"Forzando re-chequeo para measurement_id={measurement_id}, fuente más antigua que {args.force_recheck_sources_days} días.")
            except ValueError:
                logger.warning(f"Timestamp inválido en DB para measurement_id={measurement_id}, se re-chequeará.")
                force_recheck_flag = True 
        else: 
            force_recheck_flag = True 
    
    if not force_recheck_flag: 
        cursor_check.execute("""
            SELECT id FROM web_findings 
            WHERE seed_measurement_id = ? AND entity_type = ? AND seed_source_text_or_url = ? 
            AND processing_status NOT IN ('content_retrieved_pending_analysis', 'error_content_retrieval', 'network_error', 'pdf_error', 'general_download_error', 'unknown_type_error')
        """, (measurement_id, entity_type, source_text_or_url))
        if cursor_check.fetchone():
            logger.debug(f"Fuente para measurement_id={measurement_id} ya procesada anteriormente y no se fuerza re-chequeo. Omitiendo.")
            return

    source_urls_to_process = [] 
    query_type_for_db = ""
    query_text_for_db = source_text_or_url 

    if is_valid_url(source_text_or_url):
        logger.debug(f"Fuente es una URL válida: {source_text_or_url}")
        source_urls_to_process.append({"url": source_text_or_url, "title": None, "content_from_search": None, "score_from_search": None})
        query_type_for_db = "direct_url_access"
    else:
        logger.debug(f"Fuente no es URL, tratando como texto para búsqueda: '{source_text_or_url[:100]}...'")
        query_type_for_db = "source_description_search"
        query_text_for_db = source_text_or_url 
        
        # CORRECCIÓN: Truncar source_text_or_url si es muy largo para la query de Tavily
        source_text_for_tavily = source_text_or_url
        if len(source_text_or_url) > MAX_SOURCE_TEXT_IN_TAVILY_QUERY:
            source_text_for_tavily = source_text_or_url[:MAX_SOURCE_TEXT_IN_TAVILY_QUERY] + "..."
            logger.debug(f"Texto de fuente truncado para query de Tavily: '{source_text_for_tavily}'")

        tavily_search_query = f"{entity_name_context} {country_context} waste management {source_text_for_tavily}"
        logger.info(f"Query para Tavily: {tavily_search_query}")

        tavily_results = search_with_tavily(tavily_search_query, tavily_client)
        if tavily_results:
            for res in tavily_results:
                if res.get("url"):
                    source_urls_to_process.append({
                        "url": res["url"], 
                        "title": res.get("title"), 
                        "content_from_search": res.get("content"),
                        "score_from_search": res.get("score") 
                    })
        else:
            logger.warning(f"Tavily no encontró resultados para la descripción: '{source_text_or_url}'")
            finding_data_no_results = {
                "entity_id": entity_id, "entity_type": entity_type,
                "seed_measurement_id": measurement_id, "seed_source_text_or_url": source_text_or_url,
                "query_type": query_type_for_db, "query_text_used": tavily_search_query, 
                "finding_url": f"search_failed_for_text:{hashlib.md5(source_text_or_url.encode()).hexdigest()}", 
                "finding_domain": "internal_search_failure",
                "title": f"Search failed for: {source_text_or_url[:50]}...",
                "snippet_or_summary": f"Tavily search for '{tavily_search_query}' yielded no results.",
                "content_retrieval_timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "processing_status": 'error_search_no_results',
                "error_message": "Tavily search yielded no results for the provided text.",
                "data_source_methodology_hint": methodology_hint,
            }
            add_web_finding(db_conn, finding_data_no_results)
            return 

    for url_info in source_urls_to_process:
        target_url = url_info["url"]
        original_title_from_search = url_info["title"]
        original_content_from_search = url_info["content_from_search"] 
        original_score_from_search = url_info["score_from_search"]

        logger.info(f"Descargando y procesando URL: {target_url}")
        content_text, content_type, error_or_title_from_download = download_content(target_url)
        
        retrieval_ts = datetime.now(timezone.utc).isoformat()
        final_title = original_title_from_search 
        
        if content_type == 'html' and error_or_title_from_download: 
            final_title = error_or_title_from_download if error_or_title_from_download else original_title_from_search

        snippet_for_db = None
        current_processing_status = 'error_content_retrieval' 
        error_message_for_db = error_or_title_from_download if content_text is None else None

        if content_text:
            snippet_for_db = content_text[:MAX_CONTENT_LENGTH_FOR_SNIPPET]
            if len(content_text) > MAX_CONTENT_LENGTH_FOR_SNIPPET:
                snippet_for_db += "..."
            current_processing_status = 'content_retrieved_pending_analysis'
            error_message_for_db = None 
        elif original_content_from_search: 
            logger.info(f"Usando snippet de Tavily para {target_url} ya que la descarga de contenido completo falló o fue vacía.")
            snippet_for_db = original_content_from_search[:MAX_CONTENT_LENGTH_FOR_SNIPPET]
            if len(original_content_from_search) > MAX_CONTENT_LENGTH_FOR_SNIPPET:
                 snippet_for_db += "..."
            current_processing_status = 'content_retrieved_pending_analysis' 
            error_message_for_db = f"Content download failed (Error: {error_or_title_from_download if error_or_title_from_download else 'Unknown download error'}), using search snippet."

        finding_data = {
            "entity_id": entity_id,
            "entity_type": entity_type,
            "seed_measurement_id": measurement_id,
            "seed_source_text_or_url": source_text_or_url, 
            "query_type": query_type_for_db,
            "query_text_used": query_text_for_db, 
            "finding_url": target_url, 
            "finding_domain": extract_domain(target_url),
            "title": final_title,
            "snippet_or_summary": snippet_for_db,
            "content_retrieval_timestamp_utc": retrieval_ts,
            "content_publication_date": None, 
            "source_evaluation_score": None, 
            "source_evaluation_details_json": None, 
            "scraper_name": "SFA2_SourceValidator",
            "scraper_native_score": original_score_from_search if query_type_for_db == "source_description_search" else None,
            "tags_json": None, 
            "processing_status": current_processing_status,
            "error_message": error_message_for_db,
            "data_source_methodology_hint": methodology_hint,
            "full_content_sha256": get_sha256_hash(content_text) if content_text else None
        }
        
        logger.debug(f"Preparando para llamar a add_web_finding con datos: {json.dumps(finding_data, indent=2, default=str)}")
        web_finding_id = add_web_finding(db_conn, finding_data)

        if web_finding_id is None:
            logger.error(f"No se pudo añadir/obtener web_finding para URL: {target_url}")
        
        time.sleep(args.delay_between_requests if hasattr(args, 'delay_between_requests') else 1.0)

def main():
    parser = argparse.ArgumentParser(description="SFA2 Source Validator: Procesa campos 'source' de mediciones, busca/valida URLs, descarga contenido y lo guarda en 'web_findings'.")
    parser.add_argument("--db-file", default="output/db/waste_data_clean.db", help="Ruta a la BD SQLite (ej. waste_data_clean.db).")
    parser.add_argument("--country-iso3", required=True, help="Código ISO3 del país para procesar sus mediciones y las de sus ciudades.")
    parser.add_argument("--min-data-quality-score", type=float, default=0, help="Filtro (opcional): procesar solo mediciones de entidades con puntaje de calidad >= X.")
    parser.add_argument("--force-recheck-sources-days", type=int, default=0, help="Fuerza el re-chequeo de fuentes ya procesadas si son más antiguas que X días (0 = no forzar).")
    parser.add_argument("--only-missing-waste-profile", action="store_true", help="Filtro (opcional): procesar solo para entidades que aún no tienen un perfil de residuos SFA3.")
    
    parser.add_argument("--log-level", default=LOG_LEVEL_DEFAULT, choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Nivel de logging.")
    parser.add_argument("--delay-between-requests", type=float, default=1.5, help="Pausa en segundos entre descargas de URL.")

    args = parser.parse_args()

    logger.setLevel(args.log_level.upper())
    log_handler.setLevel(args.log_level.upper()) 
    logger.info(f"Nivel de logging establecido a: {args.log_level.upper()}")

    from dotenv import load_dotenv
    load_dotenv()
    tavily_client = get_tavily_client() 

    db_path = Path(args.db_file)
    if not db_path.is_file():
        logger.critical(f"Archivo de base de datos no encontrado: {db_path}")
        sys.exit(1)

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row 
        logger.info(f"Conectado a la base de datos: {db_path}")

        if args.min_data_quality_score > 0:
            logger.info(f"Filtro de calidad de datos activo: >= {args.min_data_quality_score} (Lógica de filtro de mediciones iniciales no implementada en esta plantilla).")
        if args.only_missing_waste_profile:
            logger.info("Procesar solo para entidades sin perfil SFA3 (Lógica de filtro de mediciones iniciales no implementada en esta plantilla).")

        process_sources_for_country(conn, args.country_iso3, tavily_client, args)

    except sqlite3.Error as e:
        logger.error(f"Error de base de datos: {e}", exc_info=True)
    except Exception as e_main:
        logger.error(f"Error inesperado en main: {e_main}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logger.info("Conexión a la base de datos cerrada.")

if __name__ == "__main__":
    main()
