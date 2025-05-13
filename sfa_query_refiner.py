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
import re
import json
# --- CORRECCIÓN: Importar clase datetime ---
from datetime import datetime, timezone
# --- FIN CORRECCIÓN ---
from typing import Optional, Dict, Any, List, Tuple
import os
import sys
from dotenv import load_dotenv

# --- Cliente LLM ---
try:
    from anthropic import Anthropic, APIError, RateLimitError, APIConnectionError
except ImportError:
    print("ERROR: 'anthropic' no está instalado. Ejecuta: pip install anthropic")
    sys.exit(1)
# --- Fin Cliente LLM ---

# --- Configuración del Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("SFA_QueryRefiner")

# --- Configuración Visual (Rich) ---
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import track
    console = Console(stderr=True)
    USE_RICH = True
    def print_panel(content, title, style="blue"):
         console.print(Panel(content, title=f"[bold {style}]{title}[/bold {style}]", border_style=style, expand=False))
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
    def print_panel(content, title, style=""): print(f"\n--- {title} ---\n{content}\n--- Fin {title} ---")


# --- Constantes ---
LLM_QUERY_MODEL = "claude-3-sonnet-20240229"
LLM_MAX_RETRIES = 2
LLM_RETRY_DELAY = 5
DEFAULT_NUM_QUERIES_PER_GAP = 2
GAP_INDICATORS = ["no specific information found", "not available in the provided data"]
FIELDS_TO_CHECK_FOR_GAPS = {
    "generation_context": ["contributing_factors_trends"],
    "collection_and_transport": ["coverage_and_methods", "key_challenges"],
    "treatment_and_disposal": ["infrastructure_highlights", "key_challenges"],
    "recycling_and_recovery_initiatives": ["programs_mentioned", "informal_sector_role", "rates_and_targets"],
    "policy_and_governance": ["regulatory_framework", "governance_issues"],
    "overall_assessment": ["recent_developments_or_outlook"]
}

# --- Funciones (load_profile_json, identify_gaps, build_query_generation_prompt, call_llm_for_query_generation, output_queries sin cambios) ---

def load_profile_json(file_path: Path) -> Optional[Dict[str, Any]]:
    """Carga y parsea el archivo JSON del perfil."""
    logger.info(f"Cargando perfil desde: {file_path}")
    if not file_path.is_file():
        logger.error(f"Archivo de perfil no encontrado: {file_path}")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            profile_data = json.load(f)
        if "metadata" not in profile_data or "waste_profile" not in profile_data:
            logger.error("El archivo JSON no parece tener la estructura de perfil esperada.")
            return None
        logger.info("Perfil JSON cargado y parseado exitosamente.")
        return profile_data
    except json.JSONDecodeError as e:
        logger.error(f"Error decodificando JSON en {file_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error inesperado cargando perfil {file_path}: {e}")
        return None

def identify_gaps(profile_data: Dict[str, Any]) -> List[Dict[str, str]]:
    """Analiza el perfil e identifica las lagunas de información."""
    gaps = []
    waste_profile = profile_data.get("waste_profile", {})
    entity_name = waste_profile.get("entity_name", "Unknown Entity")
    country_context = waste_profile.get("country", entity_name)

    logger.info(f"Identificando lagunas de información para: {entity_name}, {country_context}")

    for section, fields in FIELDS_TO_CHECK_FOR_GAPS.items():
        section_data = waste_profile.get(section)
        if isinstance(section_data, dict):
            for field in fields:
                field_value = section_data.get(field)
                is_gap = False
                # Comprobar si es string y contiene indicador de gap
                if isinstance(field_value, str) and any(indicator in field_value.lower() for indicator in GAP_INDICATORS):
                    is_gap = True
                # Comprobar si es lista y está vacía
                elif isinstance(field_value, list) and not field_value:
                    is_gap = True
                # Comprobar si es None
                elif field_value is None:
                    is_gap = True

                if is_gap:
                    gap_description = f"Falta información sobre '{field}' en la sección '{section}'."
                    logger.debug(f"Laguna detectada: {gap_description}")
                    gaps.append({
                        "section": section, "field": field, "description": gap_description,
                        "entity_name": entity_name, "country_context": country_context
                    })
        # Comprobar si la sección entera es una lista vacía (ej. strengths)
        elif isinstance(section_data, list) and not section_data and section in fields:
             gap_description = f"Falta información sobre '{section}'."
             logger.debug(f"Laguna detectada: {gap_description}")
             gaps.append({
                 "section": section, "field": section, # Usar nombre de sección como campo
                 "description": gap_description,
                 "entity_name": entity_name, "country_context": country_context
             })

    logger.info(f"Se identificaron {len(gaps)} lagunas de información.")
    return gaps

def build_query_generation_prompt(gap_info: Dict[str, str], num_queries: int) -> str:
    """Construye el prompt para generar consultas específicas para una laguna."""
    entity_name = gap_info['entity_name']
    country_context = gap_info['country_context']
    simple_topic = gap_info['field'].replace('_', ' ')
    # Simplificar tema para el prompt
    if "programs_mentioned" in gap_info['field']: simple_topic = f"specific recycling or circular economy programs for {entity_name}"
    elif "key_challenges" in gap_info['field']: simple_topic = f"key challenges in {gap_info['section'].replace('_', ' ')} for {entity_name}"
    elif "informal_sector_role" in gap_info['field']: simple_topic = f"role of informal sector in recycling for {entity_name}"
    elif "infrastructure_highlights" in gap_info['field']: simple_topic = f"specific waste treatment facilities in {entity_name}"
    elif "governance_issues" in gap_info['field']: simple_topic = f"governance or funding issues in waste management for {entity_name}"
    elif "contributing_factors_trends" in gap_info['field']: simple_topic = f"factors affecting waste generation in {entity_name}"
    elif "recent_developments_or_outlook" in gap_info['field']: simple_topic = f"recent developments or future outlook for waste management in {entity_name}"
    elif "regulatory_framework" in gap_info['field']: simple_topic = f"local waste management plans or regulations for {entity_name}"
    elif "rates_and_targets" in gap_info['field']: simple_topic = f"recycling or composting targets for {entity_name}"
    elif "coverage_and_methods" in gap_info['field']: simple_topic = f"waste collection coverage and methods for {entity_name}"


    preferred_lang = "español" if country_context in ["Mexico", "Colombia", "Spain", "Argentina"] else "inglés" # Añadir más países

    prompt = f"""You are an assistant expert in formulating effective web search queries to find specific environmental data.

The entity is: **{entity_name}, {country_context}**
The missing information is about: **{simple_topic}**

Generate exactly **{num_queries}** distinct and specific web search queries (preferably in **{preferred_lang}**) designed to find the missing information. Include recent years (e.g., {datetime.now().year}, {datetime.now().year - 1}) if relevant. Focus on official sources, reports, news, or project descriptions.

Return the result ONLY as a JSON list of strings.

Example JSON output:
[
  "query string 1",
  "query string 2"
]
"""
    return prompt

def call_llm_for_query_generation(llm_client: Anthropic, model: str, prompt: str) -> List[str]:
    """Llama al LLM para generar la lista de consultas, con reintentos."""
    retries = 0
    while retries <= LLM_MAX_RETRIES:
        try:
            logger.info(f"Llamando a LLM ({model}) para generar consultas (Intento {retries + 1})...")
            message = llm_client.messages.create(
                model=model,
                max_tokens=500,
                system="You generate specific web search queries based on missing information, outputting ONLY a JSON list of strings.",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.5,
            )

            if message.content and isinstance(message.content, list):
                raw_response = message.content[0].text.strip()
                logger.debug(f"Respuesta cruda del LLM (Consultas): {raw_response}")
                try:
                    json_match = re.search(r'\[.*\]', raw_response, re.DOTALL)
                    if json_match:
                        json_string = json_match.group(0)
                        queries = json.loads(json_string)
                        if isinstance(queries, list) and all(isinstance(q, str) for q in queries):
                            logger.info(f"Generadas {len(queries)} consultas por LLM.")
                            return queries
                        else:
                            logger.warning("Respuesta JSON del LLM no es una lista de strings.")
                    else:
                        logger.warning("No se encontró una lista JSON válida en la respuesta del LLM.")
                except json.JSONDecodeError as json_err:
                    logger.error(f"Error al decodificar JSON de respuesta del LLM (Consultas): {json_err}. Respuesta: {raw_response}")
            else:
                logger.warning("Respuesta del LLM (Consultas) vacía o inesperada.")

        except RateLimitError: logger.warning(f"Rate Limit Error del LLM. Esperando {LLM_RETRY_DELAY}s...")
        except APIConnectionError as e: logger.error(f"Error de conexión con LLM: {e}"); break
        except APIError as e:
            logger.error(f"Error API de LLM: {e.status_code} - {e.message}")
            if e.status_code not in [429, 500, 503]: break
        except Exception as e: logger.error(f"Error inesperado llamando al LLM para consultas: {e}", exc_info=True)

        retries += 1
        if retries <= LLM_MAX_RETRIES: time.sleep(LLM_RETRY_DELAY); logger.info(f"Reintento LLM {retries}...")

    logger.error(f"Falló la generación de consultas LLM después de {retries} intentos.")
    return []

def output_queries(queries: List[str], output_file: Optional[Path]):
    """Guarda las consultas en un archivo o las imprime en consola."""
    if not queries:
        logger.info("No se generaron consultas para guardar/imprimir.")
        return

    if output_file:
        try:
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                for query in queries:
                    f.write(query + '\n')
            logger.info(f"Consultas generadas guardadas en: {output_file}")
        except IOError as e:
            logger.error(f"Error escribiendo archivo de consultas {output_file}: {e}")
        except Exception as e:
             logger.error(f"Error inesperado guardando consultas: {e}")
    else:
        logger.info("Consultas Generadas:")
        if USE_RICH:
            from rich.list import List as RichList
            query_list = RichList()
            for q in queries:
                query_list.add_item(f'"{q}"')
            console.print(query_list)
        else:
            for query in queries:
                print(f"- \"{query}\"")

# --- Lógica Principal ---
def main():
    parser = argparse.ArgumentParser(description="SFA5 Query Refiner: Analiza perfiles JSON y genera consultas web para llenar lagunas.")
    parser.add_argument("--profile-json", required=True, help="Ruta al archivo JSON del perfil de entrada.")
    parser.add_argument("--output-queries-file", help="Ruta opcional al archivo .txt para guardar las consultas generadas (una por línea).")
    parser.add_argument("--num-queries-per-gap", type=int, default=DEFAULT_NUM_QUERIES_PER_GAP, help=f"Número de consultas a generar por cada laguna (default: {DEFAULT_NUM_QUERIES_PER_GAP}).")
    parser.add_argument("--anthropic-api-key", help="API Key para Anthropic (o variable ANTHROPIC_API_KEY).")
    parser.add_argument("--llm-model", default=LLM_QUERY_MODEL, help=f"Modelo Anthropic para generación de consultas (default: {LLM_QUERY_MODEL}).")

    args = parser.parse_args()

    load_dotenv()
    anthropic_key = args.anthropic_api_key or os.getenv("ANTHROPIC_API_KEY")
    if not anthropic_key: logger.critical("Falta ANTHROPIC_API_KEY."); sys.exit(1)

    try:
        llm_client = Anthropic(api_key=anthropic_key)
    except Exception as e:
        logger.critical(f"Error inicializando cliente Anthropic: {e}"); sys.exit(1)

    profile_path = Path(args.profile_json)
    profile_data = load_profile_json(profile_path)

    if not profile_data:
        sys.exit(1)

    gaps = identify_gaps(profile_data)

    if not gaps:
        logger.info("No se identificaron lagunas significativas en el perfil proporcionado.")
        sys.exit(0)

    all_generated_queries = []
    logger.info(f"Generando {args.num_queries_per_gap} consultas para cada una de las {len(gaps)} lagunas...")

    iterator = track(gaps, description="Generando consultas...") if USE_RICH else gaps
    for gap_info in iterator:
        prompt = build_query_generation_prompt(gap_info, args.num_queries_per_gap)
        generated_queries = call_llm_for_query_generation(llm_client, args.llm_model, prompt)
        if generated_queries:
            all_generated_queries.extend(generated_queries)
        time.sleep(1) # Pequeña pausa entre llamadas al LLM

    output_file_path = Path(args.output_queries_file) if args.output_queries_file else None
    output_queries(all_generated_queries, output_file_path)

    logger.info("--- Refinamiento de Consultas Completado ---")

if __name__ == "__main__":
    main()
