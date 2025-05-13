#!/usr/bin/env python3

# /// script
# dependencies = [
#   "python-dotenv>=0.20.0",
#   "rich>=13.7.0",
# ]
# ///

"""
Orquestador Principal del Workflow de Reportes Municipales (v6 - Final)

Coordina SFAs con logging, múltiples queries, selección de modelo LLM,
y genera un reporte parcial si SFA3 falla. Reportes en INGLÉS.
"""

import argparse
import subprocess
import json
import os
import sys
from pathlib import Path
import time
import logging
from typing import List, Dict, Any
import re # Para render_template_basic

# --- Configuración del Logging ---
LOG_FILE = "workflow.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'),
        logging.StreamHandler(sys.stderr)
    ]
)
logger = logging.getLogger("Orchestrator")
# --- Fin Configuración Logging ---

# --- Dependencias y Configuración Visual ---
try: from dotenv import load_dotenv; load_dotenv()
except ImportError: logger.warning("python-dotenv not installed...")
try:
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
    from rich.console import Console; console = Console(stderr=True); USE_RICH = True
except ImportError: USE_RICH = False
# --- Fin Dependencias ---

# --- Constantes ---
SFA1_SCRIPT = "sfa_csv_processor.py"
SFA2_SCRIPT = "sfa_tavily_scraper.py"
SFA3_SCRIPT = "sfa_report_generator.py" # Asume versión con plantilla estructurada

CSV_JSON_OUTPUT = "processed_municipal_data.json"
SCRAPED_JSON_ACCUMULATED = "scraped_web_data_all.json"
TEMP_SCRAPER_OUTPUT = "_temp_scraper_output.json"
TEMPLATE_FILE = "report_template.html" # Necesario para reporte parcial

# --- Funciones Auxiliares ---

def run_sfa(script_name: str, args_list: list, step_description: str) -> bool:
    """ Ejecuta un SFA y devuelve True/False. Usa logging. Permite que stderr fluya."""
    sfa_logger = logging.getLogger(f"SFA.{Path(script_name).stem}")
    sfa_logger.info(f"--- Iniciando: {step_description} ---")
    command = [sys.executable, script_name] + args_list
    logged_command = [arg if "--api-key" not in prev_arg else "[REDACTED]" for prev_arg, arg in zip([''] + command[:-1], command)]
    sfa_logger.debug(f"Comando SFA: {' '.join(logged_command)}")
    try:
        # MODIFICACIÓN CLAVE: capture_output=False para permitir que stderr (y rich) se muestren
        # También quitamos text=True y encoding ya que no capturamos directamente.
        # stderr=None (por defecto) permite que fluya a la consola del padre.
        result = subprocess.run(command, check=True, stderr=None, stdout=subprocess.PIPE, timeout=300) # Capturamos stdout por si acaso, pero dejamos stderr libre

        # Ya no podemos loggear result.stdout o e.stderr directamente aquí si no se captura
        # La salida de rich irá directamente a la consola.
        # Los logs del SFA (si los tiene) deberían manejarse por su propia cuenta o ser capturados si es necesario.

        sfa_logger.info(f"--- Completado exitosamente: {step_description} ---")
        return True
    except FileNotFoundError:
        sfa_logger.error(f"Script no encontrado: '{script_name}'")
        return False
    except subprocess.TimeoutExpired:
        sfa_logger.error("Excedió el tiempo límite (timeout).")
        return False
    except subprocess.CalledProcessError as e:
        sfa_logger.error(f"Falló (código {e.returncode}).")
        # Ya no podemos loggear e.stderr directamente si no se captura.
        # El error del SFA debería haberse impreso en la consola directamente.
        return False
    except Exception as e:
        sfa_logger.exception(f"Error inesperado ejecutando {script_name}")
        return False

def sanitize_filename(name: str) -> str:
    """ Limpia un nombre para usarlo como nombre de archivo. """
    sanitized = "".join(c for c in name if c.isalnum() or c in (' ', '_', '-')).strip()
    sanitized = sanitized.replace(' ', '_')
    if len(sanitized) > 100: sanitized = sanitized[:100]
    if not sanitized: sanitized = "unnamed_report"
    return sanitized

def generate_search_queries(municipality: str, country: str, num_queries: int = 3) -> List[str]:
    """Genera consultas de búsqueda específicas y refinadas para un municipio."""
    clean_municipality = municipality.replace('_', ' ')
    clean_country = country.replace('_', ' ')
    # Consultas refinadas
    base_queries = [
        f'"{clean_municipality}" "{clean_country}" waste management statistics OR data OR "annual report"', # Más específico
        f'"{clean_municipality}" recycling programs AND challenges OR results', # Buscar resultados
        f'"{clean_municipality}" landfill OR "waste disposal site" issues OR capacity OR closure', # Añadir cierre
        f'"{clean_municipality}" waste collection service improvement OR efficiency OR coverage', # Añadir cobertura
        f'"{clean_municipality}" "municipal solid waste" policy OR regulation OR plan', # Añadir plan
        f'"{clean_municipality}" circular economy initiatives waste OR "zero waste"', # Añadir zero waste
        # Opcional: Añadir consultas extra aquí si aumentas num_queries
        # f'"{clean_municipality}" electronic waste OR e-waste management',
        # f'"{clean_municipality}" organic waste composting OR digestion',
    ]
    # Devolver las primeras 'num_queries'
    logger.debug(f"Generated queries for {municipality}: {base_queries[:num_queries]}")
    return base_queries[:num_queries]

def render_template_basic(template_content: str, context: dict) -> str:
    """ Versión simplificada de render_template para reportes fallidos. """
    rendered = template_content
    key_mapping = {"municipality_name": "municipality", "country_name": "country"}
    render_context = context.copy()
    for template_key, context_key in key_mapping.items():
        if context_key in render_context: render_context[template_key] = render_context[context_key]
    for key, value in render_context.items():
        if isinstance(value, (int, float)):
            if float(value).is_integer(): placeholder_value = f"{int(value):,}"
            else: placeholder_value = f"{value:,.2f}"
        elif value is None: placeholder_value = "N/A"
        else: placeholder_value = str(value)
        rendered = rendered.replace(f"{{{{{key}}}}}", placeholder_value)
    rendered = re.sub(r"\{\{[^}]+\}\}", "N/A", rendered)
    return rendered

def generate_failed_report(template_path: str, municipality_info: Dict[str, Any], output_html_path: Path, error_message: str = "LLM analysis could not be generated due to an error."):
    """Genera un HTML básico usando la plantilla, solo con datos CSV y un mensaje de error."""
    logger.warning(f"Generando reporte parcial/fallido para {municipality_info.get('municipality', 'Unknown')}")
    try:
        with open(template_path, 'r', encoding='utf-8') as f_template:
            template_content = f_template.read()

        generation_date_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        context = municipality_info.copy()
        context["llm_analysis_section"] = f'<p style="color: red; font-weight: bold;">{error_message}</p>'
        context["generation_date"] = generation_date_str
        context.setdefault('municipality', 'N/A'); context.setdefault('country', 'N/A')
        context.setdefault('population', None); context.setdefault('total_waste_tons_year', None)

        final_html = render_template_basic(template_content, context)

        output_html_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_html_path, 'w', encoding='utf-8') as f:
            f.write(final_html)
        logger.info(f"Reporte parcial guardado en: {output_html_path}")

    except FileNotFoundError: logger.error(f"No se encontró la plantilla '{template_path}' al generar reporte fallido.")
    except Exception as e: logger.error(f"Error inesperado generando reporte fallido para {municipality_info.get('municipality', 'Unknown')}: {e}")

# --- Flujo Principal ---
def main():
    parser = argparse.ArgumentParser(description="Orquestador v6.1 (Consultas Refinadas).")
    # (Mantener los mismos argumentos que antes)
    parser.add_argument("--input-csv", required=True, help="Path to the input CSV file.")
    parser.add_argument("--output-dir", default="final_reports_en_structured_v2", help="Directory for final English HTML reports.") # Nuevo dir por defecto
    parser.add_argument("--skip-sfa1", action="store_true", help="Skip SFA1 if processed JSON exists.")
    parser.add_argument("--skip-sfa2", action="store_true", help="Skip SFA2 if accumulated scraping JSON exists.")
    parser.add_argument("--stop-on-error", action="store_true", help="Stop the entire process if any SFA fails.")
    parser.add_argument("--queries-per-municipality", type=int, default=3, help="Number of specific search queries per municipality for SFA2.")
    parser.add_argument("--max-scrape-results-per-query", type=int, default=3, help="Max results per specific scrape query in SFA2.")
    parser.add_argument("--scrape-delay", type=float, default=0.7, help="Delay (s) between SFA2 calls.")
    parser.add_argument("--llm-model", default="claude-3-5-sonnet-20241022", help="Anthropic model for SFA3 (defaulting to Sonnet now).") # Cambiar default a Sonnet
    args = parser.parse_args()

    logger.info("--- Iniciando Workflow v6.1 (Consultas Refinadas) ---")
    logger.info(f"Argumentos: {args}")

    # ... (Validación de API Keys) ...
    TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")
    LLM_API_KEY = os.getenv("ANTHROPIC_API_KEY")
    if not TAVILY_API_KEY: logger.critical("Falta TAVILY_API_KEY."); sys.exit(1)
    if not LLM_API_KEY: logger.critical("Falta ANTHROPIC_API_KEY."); sys.exit(1)

    sfa2_query_errors = 0
    sfa3_errors = 0

    # --- Paso 1: Procesar CSV ---
    if args.skip_sfa1 and Path(CSV_JSON_OUTPUT).exists(): logger.warning(f"Omitiendo SFA1...")
    else:
        if not run_sfa(SFA1_SCRIPT, ["--input-csv", args.input_csv, "--output-json", CSV_JSON_OUTPUT], "Procesar CSV"):
            logger.critical("SFA1 falló."); sys.exit(1)
    try:
        with open(CSV_JSON_OUTPUT, 'r', encoding='utf-8') as f: municipalities_data = json.load(f)
        if not municipalities_data: logger.critical(f"{CSV_JSON_OUTPUT} vacío."); sys.exit(1)
        logger.info(f"Se procesarán {len(municipalities_data)} municipios.")
    except Exception as e: logger.critical(f"Error leyendo {CSV_JSON_OUTPUT}: {e}"); sys.exit(1)

    # --- Paso 2: Scraping Web ---
    all_scraped_reports = []
    if args.skip_sfa2 and Path(SCRAPED_JSON_ACCUMULATED).exists(): logger.warning(f"Omitiendo SFA2...") # Carga omitida por brevedad
    else:
        logger.info("--- Iniciando Scraping Web (Múltiples Queries) ---")
        progress_context = Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), TimeElapsedColumn(), console=console if USE_RICH else None, disable=not USE_RICH)
        with progress_context as progress:
            task_mun = progress.add_task("Procesando Municipios...", total=len(municipalities_data))
            for i, municipality_info in enumerate(municipalities_data):
                 municipio_name = municipality_info.get("municipality", f"Unknown_{i}")
                 country_name = municipality_info.get("country", "")
                 if USE_RICH: progress.update(task_mun, description=f"Scraping {municipio_name[:20]}...")
                 else: logger.info(f"Procesando scraping para: {municipio_name} ({i+1}/{len(municipalities_data)})")
                 queries = generate_search_queries(municipio_name, country_name, args.queries_per_municipality)
                 logger.info(f"Generadas {len(queries)} consultas para {municipio_name}")
                 for q_idx, query in enumerate(queries):
                      sfa2_args = ["--query", query, "--output-json", TEMP_SCRAPER_OUTPUT, "--api-key", TAVILY_API_KEY, "--max-results", str(args.max_scrape_results_per_query)]
                      if run_sfa(SFA2_SCRIPT, sfa2_args, f"Query {q_idx+1} para {municipio_name}"):
                           try:
                                with open(TEMP_SCRAPER_OUTPUT, 'r', encoding='utf-8') as f_temp:
                                     scraped_data_single = json.load(f_temp)
                                     reports_found = scraped_data_single.get("reports", [])
                                     for report in reports_found: report["query_municipality"] = municipio_name; report["specific_query"] = query
                                     all_scraped_reports.extend(reports_found)
                                     logger.info(f"    Query '{query[:30]}...' encontró {len(reports_found)} resultados.")
                           except Exception as e: logger.warning(f"    No se pudo leer/decodificar salida de SFA2 para query '{query[:30]}...': {e}")
                      else:
                           logger.warning(f"  SFA2 falló para query '{query[:30]}...' (Municipio: {municipio_name}).")
                           sfa2_query_errors += 1
                           if args.stop_on_error: logger.critical("Detenido por error en SFA2."); sys.exit(1)
                      if args.scrape_delay > 0: time.sleep(args.scrape_delay)
                 if USE_RICH: progress.update(task_mun, advance=1)
        # Guardar resultados acumulados
        if not (args.skip_sfa2 and Path(SCRAPED_JSON_ACCUMULATED).exists()):
             final_scraped_output = {"metadata": {"description": "Accumulated scraping results"},"total_reports": len(all_scraped_reports),"sfa2_query_errors": sfa2_query_errors,"reports": all_scraped_reports}
             try:
                 logger.info(f"Guardando {len(all_scraped_reports)} resultados de scraping acumulados en: {SCRAPED_JSON_ACCUMULATED}")
                 with open(SCRAPED_JSON_ACCUMULATED, 'w', encoding='utf-8') as f_acc: json.dump(final_scraped_output, f_acc, indent=2, ensure_ascii=False)
             except IOError as e: logger.error(f"No se pudo guardar {SCRAPED_JSON_ACCUMULATED}: {e}")
             finally:
                 if Path(TEMP_SCRAPER_OUTPUT).exists():
                      try: os.remove(TEMP_SCRAPER_OUTPUT)
                      except OSError as e: logger.warning(f"No se pudo eliminar {TEMP_SCRAPER_OUTPUT}: {e}")

    # --- Paso 3: Generación de Reportes ---
    final_report_dir = Path(args.output_dir)
    final_report_dir.mkdir(parents=True, exist_ok=True)
    logger.info("--- Iniciando Generación de Reportes HTML (por municipio) ---")
    progress_context_rep = Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), TimeElapsedColumn(), console=console if USE_RICH else None, disable=not USE_RICH)
    with progress_context_rep as progress:
        task_rep = progress.add_task("Generating reports...", total=len(municipalities_data))
        for municipality_info in municipalities_data:
            municipio_name = municipality_info.get("municipality", "Unknown")
            if USE_RICH: progress.update(task_rep, description=f"Report {municipio_name[:20]}...")
            else: logger.info(f"Procesando reporte para: {municipio_name} ({progress.tasks[task_rep].completed + 1}/{len(municipalities_data)})")

            relevant_scraped_reports = [r for r in all_scraped_reports if r.get("query_municipality") == municipio_name]
            logger.info(f"  Usando {len(relevant_scraped_reports)} resultados de scraping para {municipio_name}")
            municipality_data_str = json.dumps(municipality_info)
            scraped_data_str = json.dumps(relevant_scraped_reports)
            safe_filename = sanitize_filename(municipio_name) + ".html"
            output_html_path = final_report_dir / safe_filename
            sfa3_args = [
                "--municipality-data-json", municipality_data_str,
                "--scraped-data-json", scraped_data_str,
                "--output-html-file", str(output_html_path),
                "--api-key", LLM_API_KEY,
                "--llm-model", args.llm_model
            ]

            # --- Manejo de Error SFA3 con Reporte Parcial ---
            if not run_sfa(SFA3_SCRIPT, sfa3_args, f"Generando reporte para {municipio_name}"):
                 logger.warning(f"  SFA3 falló para {municipio_name}. Generando reporte parcial.")
                 sfa3_errors += 1
                 # Intentar generar reporte parcial
                 generate_failed_report(TEMPLATE_FILE, municipality_info, output_html_path)
                 if args.stop_on_error:
                      logger.critical("Deteniendo proceso por error en SFA3 y --stop-on-error.")
                      sys.exit(1)
            # --- Fin Manejo Error SFA3 ---

            if USE_RICH: progress.update(task_rep, advance=1)

    logger.info(f"--- Workflow Completado! Reportes guardados en {final_report_dir.resolve()} ---")
    if sfa2_query_errors > 0 or sfa3_errors > 0: logger.warning(f"Ejecución finalizada con errores: {sfa2_query_errors} queries SFA2 fallidas, {sfa3_errors} reportes SFA3 fallidos/parciales. Revisa '{LOG_FILE}'.")

if __name__ == "__main__":
    main()
