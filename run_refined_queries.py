# /// script
# dependencies = [
#   "rich>=13.7.0",
#   "python-dotenv>=0.20.0",
# ]
# ///

import argparse
import subprocess
import sys
import os
from pathlib import Path
import time
import logging
from typing import List, Dict, Any
import hashlib # Para generar nombres de archivo únicos

# --- Configuración del Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger("RefinedQueryRunner")

# --- Configuración Visual (Rich) ---
try:
    from rich.console import Console
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
    console = Console(stderr=True)
    USE_RICH = True
except ImportError:
    USE_RICH = False

# --- Constantes ---
SFA_SCRAPER_SCRIPT = "sfa_tavily_scraper.py"
DEFAULT_MAX_RESULTS_PER_QUERY = 5
DEFAULT_DELAY_BETWEEN_QUERIES = 1.0 # Segundos

# --- Funciones Auxiliares ---

def run_sfa(script_name: str, args_list: list, step_description: str) -> bool:
    """ Ejecuta un SFA y devuelve True/False. Usa logging. """
    sfa_logger = logging.getLogger(f"SFA.{Path(script_name).stem}")
    sfa_logger.info(f"--- Iniciando: {step_description} ---")
    command = [sys.executable, script_name] + args_list
    logged_command = [arg if "--api-key" not in prev_arg else "[REDACTED]" for prev_arg, arg in zip([''] + command[:-1], command)]
    sfa_logger.debug(f"Comando SFA: {' '.join(logged_command)}")
    try:
        result = subprocess.run(command, check=True, stderr=None, stdout=subprocess.PIPE, timeout=300)
        sfa_logger.info(f"--- Completado exitosamente: {step_description} ---")
        return True
    except FileNotFoundError:
        sfa_logger.error(f"Script no encontrado: '{script_name}'")
        return False
    except subprocess.TimeoutExpired:
        sfa_logger.error("Excedió el tiempo límite (timeout).")
        return False
    except subprocess.CalledProcessError as e:
        sfa_logger.error(f"Falló (código {e.returncode}). La salida del error debería estar visible arriba.")
        return False
    except Exception as e:
        sfa_logger.exception(f"Error inesperado ejecutando {script_name}")
        return False

def generate_output_filename(query: str, output_dir: Path) -> Path:
    """Genera un nombre de archivo único basado en el hash de la consulta."""
    query_hash = hashlib.md5(query.encode()).hexdigest()[:10] # Hash corto
    filename = f"query_{query_hash}.json"
    return output_dir / filename

# --- Lógica Principal ---
def main():
    parser = argparse.ArgumentParser(description="Ejecuta múltiples consultas refinadas usando sfa_tavily_scraper.py.")
    # PROPUESTA CAMBIO: Cambiar --queries-file a --queries-input-path
    parser.add_argument("--queries-input-path", required=True, help="Ruta a un archivo .txt de consultas o a un directorio que contenga archivos .txt de consultas.")
    parser.add_argument("--output-dir", default="output/refined_search_results", help="Directorio base para guardar los archivos JSON de resultados individuales.")
    parser.add_argument("--tavily-api-key", help="API Key para Tavily (o variable TAVILY_API_KEY).")
    parser.add_argument("--max-results", type=int, default=DEFAULT_MAX_RESULTS_PER_QUERY, help=f"Máximo de resultados por consulta (default: {DEFAULT_MAX_RESULTS_PER_QUERY}).")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_BETWEEN_QUERIES, help=f"Pausa en segundos entre consultas (default: {DEFAULT_DELAY_BETWEEN_QUERIES}).")
    parser.add_argument("--stop-on-error", action="store_true", help="Detener el proceso si alguna consulta falla.")

    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv()
    tavily_key = args.tavily_api_key or os.getenv("TAVILY_API_KEY")
    if not tavily_key:
        logger.critical("Falta TAVILY_API_KEY.")
        sys.exit(1)

    queries_input_path = Path(args.queries_input_path)
    base_output_dir_path = Path(args.output_dir) # Directorio base para todos los resultados

    # PROPUESTA CAMBIO: Lógica para manejar archivo o directorio de entrada
    query_files_to_process = []
    if queries_input_path.is_file():
        query_files_to_process.append(queries_input_path)
    elif queries_input_path.is_dir():
        query_files_to_process.extend(list(queries_input_path.glob("*_refined_queries.txt"))) # O un patrón más general si es necesario
        if not query_files_to_process:
            logger.warning(f"No se encontraron archivos de consulta (ej. *_refined_queries.txt) en el directorio: {queries_input_path}")
            sys.exit(0)
    else:
        logger.critical(f"La ruta de entrada de consultas no es un archivo ni un directorio válido: {queries_input_path}")
        sys.exit(1)

    all_queries_map = {} # entity_code -> [queries]
    for query_file_path in query_files_to_process:
        entity_code = query_file_path.stem.replace("_refined_queries", "") # Ej: ARG, DNK
        try:
            with open(query_file_path, 'r', encoding='utf-8') as f:
                queries_from_file = [line.strip() for line in f if line.strip()]
            if queries_from_file:
                all_queries_map[entity_code] = queries_from_file
                logger.info(f"Cargadas {len(queries_from_file)} consultas para '{entity_code}' desde {query_file_path.name}.")
            else:
                logger.warning(f"El archivo de consultas '{query_file_path.name}' para '{entity_code}' está vacío.")
        except Exception as e:
            logger.error(f"Error leyendo archivo de consultas {query_file_path}: {e}")
            if args.stop_on_error: sys.exit(1)
            continue # Saltar este archivo si hay error y no se detiene

    if not all_queries_map:
        logger.critical("No se cargaron consultas válidas de ninguna fuente. Terminando.")
        sys.exit(1)

    total_queries_to_run = sum(len(qs) for qs in all_queries_map.values())
    logger.info(f"Total de consultas a ejecutar en todos los archivos: {total_queries_to_run}")

    errors_occurred = 0
    successful_queries_count = 0

    progress_context = Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), TimeElapsedColumn(), console=console if USE_RICH else None, disable=not USE_RICH)

    with progress_context as progress:
        overall_task = progress.add_task("Ejecutando consultas refinadas...", total=total_queries_to_run)
        current_query_global_index = 0

        for entity_code, queries in all_queries_map.items():
            logger.info(f"--- Iniciando procesamiento para entidad: {entity_code} ({len(queries)} consultas) ---")
            
            # PROPUESTA CAMBIO: Directorio de salida específico para esta entidad/archivo
            entity_specific_output_dir = base_output_dir_path / entity_code
            entity_specific_output_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Resultados para '{entity_code}' se guardarán en: {entity_specific_output_dir.resolve()}")

            for i, query in enumerate(queries):
                current_query_global_index += 1
                query_num_in_file = i + 1
                
                if USE_RICH:
                    progress.update(overall_task, description=f"Entidad {entity_code} - Query {query_num_in_file}/{len(queries)}: '{query[:30]}...'")
                else:
                    logger.info(f"Ejecutando Query {query_num_in_file}/{len(queries)} para {entity_code} (Global: {current_query_global_index}/{total_queries_to_run})")

                # PROPUESTA CAMBIO: Usar entity_specific_output_dir
                output_json_path = generate_output_filename(query, entity_specific_output_dir)

                sfa_args = [
                    "--prompt", query,
                    "--output", str(output_json_path),
                    "--api-key", tavily_key,
                    "--max-results", str(args.max_results)
                ]

                success = run_sfa(SFA_SCRAPER_SCRIPT, sfa_args, f"Scraper para query {query_num_in_file} ({entity_code})")

                if success:
                    successful_queries_count += 1
                else:
                    errors_occurred += 1
                    if args.stop_on_error:
                        logger.critical(f"Deteniendo proceso debido a error en query {query_num_in_file} ({entity_code}) y opción --stop-on-error.")
                        sys.exit(1)

                if args.delay > 0 and current_query_global_index < total_queries_to_run:
                    logger.debug(f"Esperando {args.delay}s antes de la siguiente consulta...")
                    time.sleep(args.delay)
                
                if USE_RICH:
                    progress.update(overall_task, advance=1)

    logger.info("--- Ejecución de Todas las Consultas Refinadas Completada ---")
    logger.info(f"Consultas totales ejecutadas exitosamente: {successful_queries_count}")
    logger.info(f"Consultas totales fallidas: {errors_occurred}")
    logger.info(f"Resultados individuales guardados en subdirectorios dentro de: {base_output_dir_path.resolve()}")

if __name__ == "__main__":
    main()