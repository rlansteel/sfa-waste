# sfa_content_analyzer.py
# /// script
# dependencies = [
#   "rich>=13.7.0",
#   "anthropic>=0.20.0",
#   "python-dotenv>=0.20.0",
# ]
# ///

import argparse
import sqlite3
from pathlib import Path
import logging
import time
import json
from typing import Optional, Dict, Any, List, Tuple
import os
import sys
from dotenv import load_dotenv

# --- Cliente LLM ---
try:
    from anthropic import Anthropic, APIError, RateLimitError, APIConnectionError
except ImportError:
    print("ERROR CRÍTICO: 'anthropic' no está instalado. Ejecuta: pip install anthropic")
    sys.exit(1)

# Importar funciones del script de gestión de BD
try:
    from sfa_manage_web_findings_db import add_measurement_link # Ya existe y la usaremos
except ImportError:
    print("ERROR CRÍTICO: No se pudo importar 'add_measurement_link' desde 'sfa_manage_web_findings_db.py'.")
    sys.exit(1)

# --- Configuración del Logging ---
LOG_LEVEL_DEFAULT = "INFO"
logger = logging.getLogger("SFA_ContentAnalyzer")
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
log_handler = logging.StreamHandler(sys.stderr)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - [%(funcName)s:%(lineno)d] - %(message)s')
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)

# --- Constantes ---
LLM_EVALUATION_MODEL_DEFAULT = "claude-3-haiku-20240307"
LLM_RELEVANCE_THRESHOLD_DEFAULT = 70 # Umbral (0-100) para considerar relevante
LLM_MAX_TEXT_CHARS_DEFAULT = 4000    # Limitar texto enviado al LLM
LLM_MAX_RETRIES_DEFAULT = 2
LLM_RETRY_DELAY_DEFAULT = 5
BATCH_SIZE_DEFAULT = 10 # Número de hallazgos a procesar en cada lote de BD

def create_db_connection(db_path_str: str) -> Optional[sqlite3.Connection]:
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

def select_findings_for_analysis(conn: sqlite3.Connection, country_iso3_filter: Optional[str], limit: int) -> List[Dict[str, Any]]:
    """
    Selecciona hallazgos pendientes de análisis.
    Si country_iso3_filter se proporciona, filtra por ese país (y sus ciudades).
    Busca registros con estado 'content_retrieved_pending_analysis'.
    """
    query_parts = [
        "SELECT wf.id AS finding_id, wf.entity_id, wf.entity_type, wf.seed_measurement_id,",
        "wf.finding_url, wf.title, wf.snippet_or_summary AS text_content,",
        "COALESCE(c.municipality, co.country_name, 'Unknown Entity') AS entity_name,",
        "COALESCE(c.country, co.country_name, 'Unknown Country Context') AS country_context",
        "FROM web_findings wf",
        "LEFT JOIN cities c ON wf.entity_id = c.id AND wf.entity_type = 'city'",
        "LEFT JOIN countries co ON wf.entity_id = co.id AND wf.entity_type = 'country'",
        "WHERE wf.processing_status = 'content_retrieved_pending_analysis'"
    ]
    params = []

    if country_iso3_filter:
        country_iso3_upper = country_iso3_filter.upper()
        logger.info(f"Filtrando hallazgos para País ISO3: {country_iso3_upper} (límite: {limit})...")
        query_parts.append("AND ( (wf.entity_type = 'city' AND c.iso3c = ?) OR (wf.entity_type = 'country' AND co.country_code_iso3 = ?) )")
        params.extend([country_iso3_upper, country_iso3_upper])
    else:
        logger.info(f"Seleccionando todos los hallazgos pendientes (límite: {limit})...")

    query_parts.append("ORDER BY wf.id LIMIT ?")
    params.append(limit)
    
    sql_query = "\n".join(query_parts)
    logger.debug(f"SQL para seleccionar hallazgos: {sql_query} con params: {params}")

    findings = []
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query, tuple(params))
        findings = [dict(row) for row in cursor.fetchall()]
        logger.info(f"Seleccionados {len(findings)} hallazgos pendientes de análisis.")
    except sqlite3.Error as e:
        logger.error(f"Error seleccionando hallazgos para análisis: {e}")
    return findings


def evaluate_relevance_with_llm(content_info: Dict[str, Any], llm_client: Anthropic, model: str, max_text_chars: int) -> Dict[str, Any]:
    """
    Evalúa la relevancia del contenido extraído usando un LLM.
    Devuelve un diccionario con 'relevance_score' (0-100) y 'details_json' (respuesta completa del LLM).
    """
    title = content_info.get('title') or "No Title Provided"
    text_content = content_info.get('text_content') or ""
    source_url = content_info.get('finding_url', "URL Desconocida")
    entity_name = content_info.get('entity_name', "Entidad Desconocida")
    country_context = content_info.get('country_context', "Contexto País Desconocido")
    default_error_score = 10 # Score bajo por defecto en caso de error

    if not text_content.strip():
        logger.warning(f"Omitiendo evaluación LLM para {source_url} debido a falta de texto.")
        details = {"evaluation_method": "llm_skipped_no_text", "reason": "No text content provided", "final_llm_score": default_error_score}
        return {"relevance_score": default_error_score, "details_json": json.dumps(details)}

    truncated_text = text_content[:max_text_chars]
    if len(text_content) > max_text_chars:
        logger.debug(f"Texto truncado a {max_text_chars} caracteres para evaluación LLM de {source_url}.")
        truncated_text += "\n... [Content Truncated]"

    prompt = f"""You are an expert environmental data analyst. Your task is to evaluate the relevance of the following web content snippet for understanding **municipal solid waste management** (MSW) aspects of **{entity_name} (in {country_context})**.

Consider aspects like:
- Waste generation data (total, per capita, composition percentages: organic, plastic, paper, etc.)
- Collection methods and coverage (formal/informal sectors, collection rates)
- Treatment and disposal methods (landfills, incineration, recycling plants, composting facilities, open dumping rates)
- Specific policies, laws, or regulations related to MSW for the entity.
- Challenges or successes in MSW management for the entity.
- Specific projects or initiatives related to MSW.

URL: {source_url}
Title: {title}

Content Snippet:
\"\"\"
{truncated_text}
\"\"\"

Based ONLY on the provided Content Snippet and Title:
1.  **Relevance Score (0-100):** How relevant is this content for understanding MSW management for **{entity_name}**?
    - 0-30: Irrelevant or very tangentially related.
    - 31-69: Somewhat relevant, mentions waste or environment but not specifically MSW for the entity, or lacks detail.
    - 70-100: Highly relevant, provides specific data, policies, or detailed descriptions of MSW aspects for **{entity_name}**.
2.  **Justification:** Briefly explain your score (1-2 sentences).
3.  **Keywords:** List up to 5 key terms found in the snippet that relate to MSW (e.g., "solid waste", "recycling", "landfill", "waste collection", "organic waste"). If none, use an empty list [].
4.  **Mentions Entity:** Does the snippet specifically mention "{entity_name}" or similar terms in relation to waste management? (true/false)

Return your response ONLY as a valid JSON object with the keys: "relevance_score", "justification", "keywords", "mentions_entity".

Example:
{{
  "relevance_score": 85,
  "justification": "The snippet provides specific data on recycling rates and landfill capacity for the mentioned city.",
  "keywords": ["recycling rate", "landfill capacity", "municipal waste"],
  "mentions_entity": true
}}
"""
    retries = 0
    while retries <= LLM_MAX_RETRIES_DEFAULT: # Usar constante global o pasar como arg
        try:
            logger.info(f"Llamando a LLM ({model}) para evaluar {source_url} (Intento {retries + 1})...")
            message = llm_client.messages.create(
                model=model,
                max_tokens=500, 
                system="You are an assistant that evaluates text relevance for municipal solid waste management and outputs ONLY a valid JSON object with specified keys.",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )

            if message.content and isinstance(message.content, list):
                raw_response = message.content[0].text.strip()
                logger.debug(f"Respuesta cruda del LLM para {source_url}: {raw_response}")
                try:
                    # Limpiar posible markdown ```json ... ```
                    if raw_response.startswith("```json"): raw_response = raw_response[7:]
                    if raw_response.endswith("```"): raw_response = raw_response[:-3]
                    raw_response = raw_response.strip()

                    llm_result = json.loads(raw_response)
                    if all(k in llm_result for k in ["relevance_score", "justification", "keywords", "mentions_entity"]):
                        score = llm_result.get("relevance_score", 0)
                        try: score = max(0, min(100, int(score)))
                        except (ValueError, TypeError): score = 0; logger.warning(f"LLM devolvió un score no numérico para {source_url}: {score}. Usando 0.")
                        
                        llm_result["relevance_score"] = score 
                        llm_result["evaluation_method"] = "llm_sfa_content_analyzer"
                        logger.info(f"Evaluación LLM para {source_url}: Score={score}")
                        return {"relevance_score": score, "details_json": json.dumps(llm_result)}
                    else:
                        logger.warning(f"Respuesta JSON del LLM para {source_url} no tiene la estructura esperada: {llm_result}")
                except json.JSONDecodeError as json_err:
                    logger.error(f"Error al decodificar JSON de LLM para {source_url}: {json_err}. Respuesta: {raw_response}")
            else:
                logger.warning(f"Respuesta del LLM para {source_url} vacía o inesperada.")

        except RateLimitError: logger.warning(f"Rate Limit Error del LLM para {source_url}. Esperando {LLM_RETRY_DELAY_DEFAULT}s...")
        except APIConnectionError as e: logger.error(f"Error de conexión con LLM para {source_url}: {e}"); break 
        except APIError as e:
            logger.error(f"Error API de LLM para {source_url}: {e.status_code} - {e.message}")
            if e.status_code not in [429, 500, 503]: break
        except Exception as e_llm:
            logger.error(f"Error inesperado llamando al LLM para {source_url}: {e_llm}", exc_info=True)

        retries += 1
        if retries <= LLM_MAX_RETRIES_DEFAULT:
            time.sleep(LLM_RETRY_DELAY_DEFAULT)
            logger.info(f"Reintento LLM {retries} para {source_url}...")

    details = {"evaluation_method": "llm_failed", "reason": "Max retries or API error", "final_llm_score": default_error_score}
    return {"relevance_score": default_error_score, "details_json": json.dumps(details)}

def update_finding_in_db(conn: sqlite3.Connection, finding_id: int, score: float, details_json: str, new_status: str):
    sql = """
        UPDATE web_findings
        SET source_evaluation_score = ?,
            source_evaluation_details_json = ?,
            processing_status = ?
        WHERE id = ?
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql, (score, details_json, new_status, finding_id))
        # Commit se hará al final del lote
        if cursor.rowcount == 0:
            logger.warning(f"No se encontró el finding_id {finding_id} para actualizar evaluación.")
        else:
            logger.debug(f"Evaluación para finding_id {finding_id} preparada para actualizar (Status: {new_status}, Score: {score}).")
        return cursor.rowcount > 0
    except sqlite3.Error as e:
        logger.error(f"Error actualizando evaluación para finding_id {finding_id}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="SFA Content Analyzer: Evalúa hallazgos web pendientes usando LLM.")
    parser.add_argument("--db-file", default="output/db/waste_data_clean.db", help="Ruta a la BD SQLite.")
    parser.add_argument("--country-iso3", default=None, help="Opcional: Código ISO3 del país para filtrar hallazgos. Si no se provee, procesa todos los pendientes.")
    parser.add_argument("--anthropic-api-key", help="API Key para Anthropic (o variable ANTHROPIC_API_KEY).")
    parser.add_argument("--llm-model", default=LLM_EVALUATION_MODEL_DEFAULT, help=f"Modelo Anthropic para evaluación (default: {LLM_EVALUATION_MODEL_DEFAULT}).")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE_DEFAULT, help=f"Número de hallazgos a leer de la BD por lote (default: {BATCH_SIZE_DEFAULT}).")
    parser.add_argument("--max-total-findings", type=int, default=0, help="Número máximo total de hallazgos a procesar en esta ejecución (0 = sin límite).")
    parser.add_argument("--relevance-threshold", type=int, default=LLM_RELEVANCE_THRESHOLD_DEFAULT, help=f"Umbral de score (0-100) para marcar como relevante (default: {LLM_RELEVANCE_THRESHOLD_DEFAULT}).")
    parser.add_argument("--max-text-chars", type=int, default=LLM_MAX_TEXT_CHARS_DEFAULT, help=f"Máx. caracteres del snippet a enviar al LLM (default: {LLM_MAX_TEXT_CHARS_DEFAULT}).")
    parser.add_argument("--log-level", default=LOG_LEVEL_DEFAULT, choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Nivel de logging.")
    
    args = parser.parse_args()

    logger.setLevel(args.log_level.upper())
    log_handler.setLevel(args.log_level.upper())
    logger.info(f"Nivel de logging: {args.log_level.upper()}")

    load_dotenv()
    anthropic_key = args.anthropic_api_key or os.getenv("ANTHROPIC_API_KEY")
    if not anthropic_key:
        logger.critical("Falta ANTHROPIC_API_KEY.")
        sys.exit(1)

    try:
        llm_client = Anthropic(api_key=anthropic_key)
    except Exception as e:
        logger.critical(f"Error inicializando cliente Anthropic: {e}"); sys.exit(1)

    conn = create_db_connection(args.db_file)
    if conn is None: sys.exit(1)

    total_processed_in_run = 0
    total_marked_relevant = 0
    total_errors_llm = 0
    
    country_filter_log = f"para país {args.country_iso3}" if args.country_iso3 else "para todas las entidades"
    logger.info(f"--- Iniciando Análisis de Contenido {country_filter_log} (Modelo: {args.llm_model}, Límite Total: {'Ninguno' if args.max_total_findings == 0 else args.max_total_findings}) ---")

    try:
        while True:
            findings_batch = select_findings_for_analysis(conn, args.country_iso3, args.batch_size)
            if not findings_batch:
                logger.info(f"No hay más hallazgos pendientes de análisis {country_filter_log}.")
                break

            logger.info(f"Procesando lote de {len(findings_batch)} hallazgos...")
            
            updates_in_batch = 0
            for finding_dict in findings_batch:
                if args.max_total_findings > 0 and total_processed_in_run >= args.max_total_findings:
                    logger.info(f"Alcanzado límite máximo de procesamiento ({args.max_total_findings}). Terminando.")
                    break 
                
                finding_id = finding_dict['finding_id']
                logger.info(f"Analizando Finding ID: {finding_id} (URL: {finding_dict['finding_url']})")

                evaluation_result = evaluate_relevance_with_llm(
                    content_info=finding_dict, 
                    llm_client=llm_client, 
                    model=args.llm_model,
                    max_text_chars=args.max_text_chars
                )

                score = evaluation_result.get("relevance_score", 0)
                details_json = evaluation_result.get("details_json", "{}")
                new_status = "evaluated_irrelevant"
                
                if "llm_failed" in details_json:
                    new_status = "error_llm_evaluation"
                    total_errors_llm += 1
                elif score >= args.relevance_threshold:
                    new_status = "evaluated_relevant"
                    total_marked_relevant += 1
                    # Enlazar si es relevante y tiene seed_measurement_id
                    seed_id = finding_dict.get('seed_measurement_id')
                    entity_type_for_link = finding_dict.get('entity_type') # 'city' o 'country'
                    if seed_id is not None and entity_type_for_link:
                        logger.debug(f"Intentando enlazar finding {finding_id} con measurement {seed_id} ({entity_type_for_link})")
                        add_measurement_link(conn, finding_id, seed_id, entity_type_for_link)
                    else:
                        logger.warning(f"No se pudo crear enlace para finding {finding_id}: falta seed_measurement_id ({seed_id}) o entity_type ({entity_type_for_link}).")
                
                if update_finding_in_db(conn, finding_id, score, details_json, new_status):
                    updates_in_batch +=1
                
                total_processed_in_run += 1
                time.sleep(0.2) # Pequeña pausa para no saturar la API del LLM tan rápido

            # Commit después de cada lote
            if updates_in_batch > 0:
                try:
                    conn.commit()
                    logger.info(f"Commit de {updates_in_batch} actualizaciones del lote realizado.")
                except sqlite3.Error as e_commit:
                    logger.error(f"Error durante el commit del lote: {e_commit}")
                    conn.rollback() # Revertir si el commit falla
            
            if args.max_total_findings > 0 and total_processed_in_run >= args.max_total_findings:
                break # Salir del bucle while si se alcanzó el límite

    except Exception as e_main:
        logger.exception(f"Error inesperado en el ciclo principal de análisis: {e_main}")
        if conn: conn.rollback() # Asegurar rollback en caso de error mayor
    finally:
        if conn:
            conn.close()
            logger.info("Conexión a la base de datos cerrada.")

    logger.info(f"--- Análisis de Contenido Completado {country_filter_log} ---")
    logger.info(f"Total procesados en esta ejecución: {total_processed_in_run}")
    logger.info(f"Total marcados como relevantes (score >= {args.relevance_threshold}): {total_marked_relevant}")
    logger.info(f"Total de errores de evaluación LLM: {total_errors_llm}")

if __name__ == "__main__":
    main()
