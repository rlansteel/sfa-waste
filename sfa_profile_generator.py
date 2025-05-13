# sfa_profile_generator.py
# /// script
# dependencies = [
#   "rich>=13.7.0",
#   "anthropic>=0.20.0", # Asegúrate de que esté actualizado
#   "python-dotenv>=0.20.0"
# ]
# ///

import argparse
import sqlite3
from pathlib import Path
import logging
import time
import json
from typing import Optional, Dict, Any, List, Tuple, Union
import os
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv

# --- Cliente LLM ---
try:
    from anthropic import Anthropic, APIError, RateLimitError, APIConnectionError
except ImportError:
    print("ERROR CRÍTICO: 'anthropic' no está instalado. Ejecuta: pip install anthropic")
    sys.exit(1)

# --- Configuración del Logging ---
LOG_LEVEL_DEFAULT = "INFO"
logger = logging.getLogger("SFA_ProfileGenerator")
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
log_handler = logging.StreamHandler(sys.stderr)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - [%(funcName)s:%(lineno)d] - %(message)s')
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)

# --- Constantes ---
LLM_PROFILE_MODEL_DEFAULT = "claude-3-sonnet-20240229" # Modelo principal para generación
LLM_MAX_RETRIES_DEFAULT = 2
LLM_RETRY_DELAY_DEFAULT = 10 # Segundos
OUTPUT_DIR_DEFAULT = "output/profiles_sfa3_refined/"
SFA3_VERSION = "3.1-ProfileJSON_Granular" # Versión actualizada del perfil

# --- Funciones de Base de Datos ---
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

def get_entity_base_data(conn: sqlite3.Connection, entity_type: str, entity_identifier: Union[str, int]) -> Optional[Dict[str, Any]]:
    logger.info(f"Buscando datos base para {entity_type} con identificador: {entity_identifier}")
    if entity_type == "country":
        query = "SELECT * FROM countries WHERE country_code_iso3 = ?"
        params = (str(entity_identifier).upper(),)
    elif entity_type == "city":
        query = "SELECT c.*, co.country_name as country_name_for_city FROM cities c JOIN countries co ON c.iso3c = co.country_code_iso3 WHERE c.id = ?"
        try:
            params = (int(entity_identifier),)
        except ValueError:
            logger.error(f"ID de ciudad debe ser numérico. Recibido: {entity_identifier}")
            return None
    else:
        logger.error(f"Tipo de entidad no soportado: {entity_type}")
        return None

    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        row = cursor.fetchone()
        if row:
            logger.info(f"Datos base encontrados para {entity_type} {entity_identifier}.")
            return dict(row)
        else:
            logger.warning(f"No se encontraron datos base para {entity_type} {entity_identifier}.")
            return None
    except sqlite3.Error as e:
        logger.error(f"Error de BD obteniendo datos base para {entity_type} {entity_identifier}: {e}")
        return None

def get_relevant_web_findings(conn: sqlite3.Connection, entity_db_id: int, entity_type: str) -> List[Dict[str, Any]]:
    logger.info(f"Buscando hallazgos web relevantes para {entity_type}_id: {entity_db_id}")
    query = """
        SELECT id, finding_url, title, snippet_or_summary, source_evaluation_score, data_source_methodology_hint
        FROM web_findings
        WHERE entity_id = ? AND entity_type = ? AND processing_status = 'evaluated_relevant'
        ORDER BY source_evaluation_score DESC, id
    """
    findings = []
    try:
        cursor = conn.cursor()
        cursor.execute(query, (entity_db_id, entity_type))
        findings = [dict(row) for row in cursor.fetchall()]
        logger.info(f"Encontrados {len(findings)} hallazgos web relevantes.")
    except sqlite3.Error as e:
        logger.error(f"Error de BD obteniendo hallazgos web: {e}")
    return findings

# --- Funciones de Generación de Prompts ---

def format_web_findings_for_prompt(web_findings: List[Dict[str, Any]]) -> str:
    if not web_findings:
        return "No relevant web findings were available for this entity."
    
    formatted_findings = []
    for i, finding in enumerate(web_findings):
        snippet = (finding.get('snippet_or_summary') or "No snippet available.")
        # Limitar longitud del snippet para no exceder límites del prompt
        snippet = snippet[:1000] + "..." if len(snippet) > 1000 else snippet
        
        finding_entry = (
            f"* Finding ID {finding.get('id', 'N/A')} (Score: {finding.get('source_evaluation_score', 'N/A')})\n"
            f"  Title: {finding.get('title') or 'No Title'}\n"
            f"  URL: {finding.get('finding_url')}\n"
            f"  Methodology Hint: {finding.get('data_source_methodology_hint') or 'N/A'}\n"
            f"  Snippet: \"{snippet}\""
        )
        formatted_findings.append(finding_entry)
    return "\n\n".join(formatted_findings)

def construct_prompt_for_section(section_name: str, entity_data: Dict[str, Any], 
                                 web_findings_text: str, entity_type: str, 
                                 entity_name_display: str, country_name_display: str) -> str:
    
    common_instructions = (
        f"You are an expert environmental analyst. Your task is to generate a specific section for a waste management profile of "
        f"**{entity_name_display} (in {country_name_display})**. "
        f"Focus on synthesis and analysis, connecting information from the structured data and web findings. "
        f"If information is directly from a web finding, try to subtly indicate that (e.g., 'some reports suggest...', 'according to a national plan...'). "
        f"If specific data is missing from all provided sources for a field, state 'No specific information found in sources.' for that field. "
        f"Output ONLY the valid JSON content for the requested section, with no other text before or after the JSON. "
        f"Ensure all string values within the JSON are properly escaped.\n\n"
        f"**1. Structured Database Data (Key Fields for Context - Do not just repeat, synthesize!):**\n"
        f"```json\n{json.dumps(entity_data, indent=2)}\n```\n\n"
        f"**2. Relevant Web Findings (Snippets/Summaries - Use these to enrich the section):**\n{web_findings_text}\n\n"
        f"--- Now, generate ONLY the JSON for the **'{section_name}'** section: ---"
    )

    prompts = {
        "generation_context": (
            f"{common_instructions}\n"
            "The JSON should have keys: \"scale_and_rate\" (string, describe total and per capita generation, trends if any) and "
            "\"contributing_factors_trends\" (string, describe factors like tourism, industry, population growth affecting waste, and any observed trends)."
        ),
        "waste_stream_composition": (
            f"{common_instructions}\n"
            "The JSON should have keys: \"summary\" (string, describe the main components of the waste stream based on percentages in entity_data, e.g., 'predominantly organic (X%), followed by plastics (Y%)...') and "
            "\"data_notes\" (string, mention data source if known, e.g., 'Based on national study [year]' or 'Composition data from database record.')."
        ),
        "collection_and_transport": (
            f"{common_instructions}\n"
            "The JSON should have keys: \"coverage_and_methods\" (string, describe collection coverage, primary methods, formal/informal sector roles if known from data or web findings) and "
            "\"key_challenges\" (string, list any challenges in collection/transport mentioned or inferred)."
        ),
        "treatment_and_disposal": (
            f"{common_instructions}\n"
            "The JSON should have keys: \"dominant_methods_summary\" (string, describe main treatment/disposal methods and percentages from entity_data, integrate web findings if they add detail/context), "
            "\"infrastructure_highlights\" (string, mention specific facilities or infrastructure types if highlighted in web findings), and "
            "\"key_challenges\" (string, list challenges related to treatment/disposal)."
        ),
        "recycling_and_recovery_initiatives": (
            f"{common_instructions}\n"
            "The JSON should have keys: \"rates_and_targets\" (string, state recycling/composting rates from entity_data, mention targets if found in web findings), "
            "\"programs_mentioned\" (string, list specific programs if mentioned in web findings), and "
            "\"informal_sector_role\" (string, describe role of informal sector if mentioned in web findings)."
        ),
        "policy_and_governance": (
            f"{common_instructions}\n"
            "The JSON should have keys: \"regulatory_framework\" (string, describe national/local laws, agencies, plans from entity_data or web findings) and "
            "\"governance_issues\" (string, list funding, administrative, or other governance issues if mentioned)."
        )
    }
    return prompts.get(section_name, f"Error: Prompt for section '{section_name}' not found.")

def construct_final_synthesis_prompt(generated_sections: Dict[str, Any], entity_data: Dict[str, Any], 
                                     web_findings_text: str, entity_name_display: str, country_name_display: str) -> str:
    prompt = (
        f"You are a lead environmental analyst. You have received detailed draft sections for a waste management profile of "
        f"**{entity_name_display} (in {country_name_display})**. Your task is to write the final 'overall_summary' and 'overall_assessment' sections. "
        f"Base your synthesis PRIMARILY on the content of the provided draft sections. You can also refer to the initial structured data and web finding snippets for overarching context or if a critical piece of information was missed in the drafts.\n\n"
        f"**1. Initial Structured Database Data (for context):**\n"
        f"```json\n{json.dumps(entity_data, indent=2)}\n```\n\n"
        f"**2. Relevant Web Findings (Snippets/Summaries - for context):**\n{web_findings_text}\n\n"
        f"**3. Draft Profile Sections (PRIMARY INPUT FOR YOUR TASK):**\n"
        f"```json\n{json.dumps(generated_sections, indent=2)}\n```\n\n"
        f"--- Now, generate ONLY the JSON for the 'overall_summary' and 'overall_assessment' sections: ---\n"
        "The JSON output should be an object containing two keys: \"overall_summary\" (string) and \"overall_assessment\" (object). "
        "The \"overall_assessment\" object should contain keys: \"strengths\" (list of strings), \"weaknesses_or_challenges\" (list of strings), and \"recent_developments_or_outlook\" (string). "
        "Synthesize concisely. For 'recent_developments_or_outlook', if no specific information was found in the drafts or web findings, state 'No specific recent developments or future outlook information found in the provided sources.' "
        "Output ONLY the valid JSON content, with no other text before or after the JSON. Ensure all string values within the JSON are properly escaped."
    )
    return prompt

# --- Funciones de LLM ---
def call_llm(llm_client: Anthropic, model_name: str, prompt: str, section_name_for_log: str) -> Optional[Dict[str, Any]]:
    logger.info(f"Llamando a LLM ({model_name}) para la sección '{section_name_for_log}' (Prompt con longitud: {len(prompt)})...")
    # logger.debug(f"Prompt para '{section_name_for_log}':\n{prompt}") # Descomentar para debugging detallado del prompt

    retries = 0
    while retries <= LLM_MAX_RETRIES_DEFAULT:
        try:
            message = llm_client.messages.create(
                model=model_name,
                max_tokens=2048, # Aumentado para permitir secciones más largas y JSON
                system="You are an expert assistant that generates structured JSON output for specific sections of a waste management profile based on provided data and instructions. You output ONLY valid JSON.",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3, # Ligeramente más creativo para la redacción, pero aún factual
            )

            if message.content and isinstance(message.content, list) and message.content[0].text:
                raw_response = message.content[0].text.strip()
                logger.debug(f"Respuesta cruda del LLM para '{section_name_for_log}':\n{raw_response}")
                
                # Limpiar posible markdown ```json ... ```
                if raw_response.startswith("```json"): raw_response = raw_response[7:]
                if raw_response.endswith("```"): raw_response = raw_response[:-3]
                raw_response = raw_response.strip()

                try:
                    parsed_json = json.loads(raw_response)
                    logger.info(f"Respuesta JSON parseada exitosamente para '{section_name_for_log}'.")
                    return parsed_json
                except json.JSONDecodeError as json_err:
                    logger.error(f"Error al decodificar JSON de LLM para '{section_name_for_log}': {json_err}. Respuesta: {raw_response}")
            else:
                logger.warning(f"Respuesta del LLM para '{section_name_for_log}' vacía o inesperada.")

        except RateLimitError:
            logger.warning(f"Rate Limit Error del LLM para '{section_name_for_log}'. Esperando {LLM_RETRY_DELAY_DEFAULT}s...")
        except APIConnectionError as e:
            logger.error(f"Error de conexión con LLM para '{section_name_for_log}': {e}"); break 
        except APIError as e:
            logger.error(f"Error API de LLM para '{section_name_for_log}': {e.status_code} - {e.message}")
            if e.status_code not in [429, 500, 503]: break # No reintentar errores no transitorios
        except Exception as e_llm:
            logger.error(f"Error inesperado llamando al LLM para '{section_name_for_log}': {e_llm}", exc_info=True)
        
        retries += 1
        if retries <= LLM_MAX_RETRIES_DEFAULT:
            logger.info(f"Reintento LLM {retries}/{LLM_MAX_RETRIES_DEFAULT} para '{section_name_for_log}' en {LLM_RETRY_DELAY_DEFAULT}s...")
            time.sleep(LLM_RETRY_DELAY_DEFAULT * retries) # Backoff exponencial simple
        else:
            logger.error(f"Máximo de reintentos alcanzado para '{section_name_for_log}'.")

    return None


# --- Función Principal de Generación de Perfil ---
def generate_profile(conn: sqlite3.Connection, entity_type: str, entity_identifier: Union[str, int], 
                     output_dir: str, llm_client: Anthropic, model_name: str):

    entity_data = get_entity_base_data(conn, entity_type, entity_identifier)
    if not entity_data:
        return

    entity_db_id = entity_data['id']
    entity_name_display = entity_data.get('country_name') if entity_type == 'country' else entity_data.get('municipality', f"City ID {entity_db_id}")
    country_name_display = entity_data.get('country_name') if entity_type == 'country' else entity_data.get('country_name_for_city', entity_data.get('country_code_iso3'))


    web_findings = get_relevant_web_findings(conn, entity_db_id, entity_type)
    web_findings_text_for_prompt = format_web_findings_for_prompt(web_findings)
    web_finding_ids_consulted = [wf['id'] for wf in web_findings]

    # Generar cada sección individualmente
    profile_sections = [
        "generation_context", "waste_stream_composition", "collection_and_transport",
        "treatment_and_disposal", "recycling_and_recovery_initiatives", "policy_and_governance"
    ]
    generated_sections_content = {}
    all_sections_generated_successfully = True

    for section_name in profile_sections:
        logger.info(f"Generando sección: {section_name} para {entity_name_display}...")
        prompt_for_section = construct_prompt_for_section(
            section_name, entity_data, web_findings_text_for_prompt, entity_type, entity_name_display, country_name_display
        )
        section_json_content = call_llm(llm_client, model_name, prompt_for_section, section_name)
        
        if section_json_content:
            generated_sections_content[section_name] = section_json_content
        else:
            logger.error(f"No se pudo generar la sección '{section_name}' para {entity_name_display}. Se usará un placeholder.")
            # Crear un placeholder para la sección fallida para no romper la estructura del perfil
            if section_name == "generation_context":
                generated_sections_content[section_name] = {"scale_and_rate": "Error generating this section.", "contributing_factors_trends": "Error generating this section."}
            elif section_name == "waste_stream_composition":
                 generated_sections_content[section_name] = {"summary": "Error generating this section.", "data_notes": "Error generating this section."}
            # ... (placeholders para otras secciones si es necesario)
            else:
                 generated_sections_content[section_name] = {"error": f"Failed to generate content for {section_name}."}
            all_sections_generated_successfully = False
        time.sleep(1) # Pequeña pausa entre llamadas al LLM para secciones

    # Generar las secciones de síntesis
    synthesis_content = None
    if all_sections_generated_successfully: # Solo intentar síntesis si todas las secciones base están OK
        logger.info(f"Generando secciones de síntesis (overall_summary, overall_assessment) para {entity_name_display}...")
        final_synthesis_prompt = construct_final_synthesis_prompt(
            generated_sections_content, entity_data, web_findings_text_for_prompt, entity_name_display, country_name_display
        )
        synthesis_content = call_llm(llm_client, model_name, final_synthesis_prompt, "synthesis_overall")
    
    if not synthesis_content:
        logger.error(f"No se pudo generar las secciones de síntesis para {entity_name_display}. Se usarán placeholders.")
        synthesis_content = {
            "overall_summary": "Error generating overall summary.",
            "overall_assessment": {
                "strengths": ["Error generating strengths."],
                "weaknesses_or_challenges": ["Error generating weaknesses."],
                "recent_developments_or_outlook": "Error generating outlook."
            }
        }
        all_sections_generated_successfully = False # Marcar que hubo un problema en la síntesis

    # Ensamblar el perfil final
    waste_profile_dict = {
        "entity_name": entity_name_display,
        "country": country_name_display if entity_type == 'country' else entity_data.get('country_name_for_city', entity_data.get('country_code_iso3')),
        "iso3": entity_data.get('country_code_iso3') if entity_type == 'country' else entity_data.get('iso3c'),
        "overall_summary": synthesis_content.get("overall_summary", "Summary generation failed."),
        **generated_sections_content, # Desempaquetar las secciones individuales aquí
        "overall_assessment": synthesis_content.get("overall_assessment", {
            "strengths": [], "weaknesses_or_challenges": [], "recent_developments_or_outlook": "Assessment generation failed."
        })
    }

    final_profile_json = {
        "metadata": {
            "entity_id": str(entity_identifier).upper() if entity_type == 'country' else entity_db_id,
            "entity_type": entity_type,
            "profile_generation_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "llm_model_used": model_name,
            "sfa3_version": SFA3_VERSION,
            "generation_mode": "granular_sections"
        },
        "waste_profile": waste_profile_dict,
        "sources_consulted": {
            "database_record": {
                "entity_db_id": entity_db_id,
                "data_quality_score": entity_data.get("data_quality_score")
            },
            "web_finding_ids": web_finding_ids_consulted
        }
    }

    # Guardar el perfil
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    filename_prefix = str(entity_identifier).upper() if entity_type == 'country' else f"city_{entity_db_id}"
    output_file = output_path / f"{filename_prefix}_{entity_type}_profile.json"
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_profile_json, f, indent=2, ensure_ascii=False)
        logger.info(f"Perfil JSON guardado exitosamente en: {output_file}")
    except IOError as e:
        logger.error(f"Error guardando perfil JSON en {output_file}: {e}")

    if not all_sections_generated_successfully:
        logger.warning(f"El perfil para {entity_name_display} se generó con algunos errores o placeholders en secciones.")


def main():
    parser = argparse.ArgumentParser(description=f"SFA3 Profile Generator ({SFA3_VERSION}): Genera perfiles de gestión de residuos.")
    parser.add_argument("--db-file", default="output/db/waste_data_clean.db", help="Ruta a la BD SQLite.")
    parser.add_argument("--entity-type", required=True, choices=["city", "country"], help="Tipo de entidad para generar perfil.")
    parser.add_argument("--entity-id", required=True, help="ID de la ciudad (numérico) o código ISO3 del país (string).")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT, help=f"Directorio de salida para perfiles JSON (default: {OUTPUT_DIR_DEFAULT}).")
    parser.add_argument("--anthropic-api-key", help="API Key para Anthropic (o variable de entorno ANTHROPIC_API_KEY).")
    parser.add_argument("--llm-model", default=LLM_PROFILE_MODEL_DEFAULT, help=f"Modelo Anthropic para generación (default: {LLM_PROFILE_MODEL_DEFAULT}).")
    parser.add_argument("--log-level", default=LOG_LEVEL_DEFAULT, choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Nivel de logging.")

    args = parser.parse_args()

    logger.setLevel(args.log_level.upper())
    log_handler.setLevel(args.log_level.upper())
    logger.info(f"Nivel de logging: {args.log_level.upper()}")

    load_dotenv()
    anthropic_key = args.anthropic_api_key or os.getenv("ANTHROPIC_API_KEY")
    if not anthropic_key:
        logger.critical("Falta ANTHROPIC_API_KEY. Defínala con --anthropic-api-key o la variable de entorno ANTHROPIC_API_KEY.")
        sys.exit(1)
    
    try:
        llm_client = Anthropic(api_key=anthropic_key)
    except Exception as e:
        logger.critical(f"Error inicializando cliente Anthropic: {e}")
        sys.exit(1)

    conn = create_db_connection(args.db_file)
    if conn is None:
        sys.exit(1)

    try:
        generate_profile(conn, args.entity_type, args.entity_id, args.output_dir, llm_client, args.llm_model)
    except Exception as e_main:
        logger.exception(f"Error inesperado en la generación del perfil: {e_main}")
    finally:
        if conn:
            conn.close()
            logger.info("Conexión a la base de datos cerrada.")

if __name__ == "__main__":
    main()
