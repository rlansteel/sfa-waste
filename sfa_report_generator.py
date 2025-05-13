#!/usr/bin/env python3

# /// script
# dependencies = [
#   "anthropic>=0.20.0",
#   "rich>=13.7.0",
#   "python-dotenv>=0.20.0",
#   "ftfy>=6.0",
#   "pandas>=2.0.0" # Añadido para pd.notna
# ]
# ///

"""
Single File Agent (SFA) 3: Report Generator (v2 - Campos Enriquecidos)

Genera reportes HTML en INGLÉS usando una plantilla, análisis estructurado del LLM,
incluye citaciones de fuentes web, y maneja los campos CSV enriquecidos.
Acepta el modelo LLM como argumento.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from datetime import datetime, timezone # Importar timezone
import re
import ftfy
import html # Para escapar HTML en valores si fuera necesario
import pandas as pd # Importar pandas para pd.notna

# --- Dependencias y Configuración Visual ---
try:
    from anthropic import Anthropic, APIError, RateLimitError, APIConnectionError
except ImportError: print("ERROR: 'anthropic' not installed.", file=sys.stderr); sys.exit(1)
try:
    from rich.console import Console, Panel
    console = Console(stderr=True); USE_RICH = True
    def print_info(message): console.print(f"[cyan]ℹ️ {message}[/cyan]")
    def print_success(message): console.print(f"[green]✅ {message}[/green]")
    def print_warning(message): console.print(f"[yellow]⚠️ {message}[/yellow]")
    def print_error(message): console.print(Panel(f"[bold red]❌ ERROR:[/bold red]\n{message}", border_style="red"))
except ImportError:
    USE_RICH = False
    def print_info(message): print(f"INFO: {message}", file=sys.stderr)
    def print_success(message): print(f"SUCCESS: {message}", file=sys.stderr)
    def print_warning(message): print(f"WARNING: {message}", file=sys.stderr)
    def print_error(message): print(f"ERROR: {message}", file=sys.stderr)
try: from dotenv import load_dotenv; load_dotenv()
except ImportError: print_warning(".env file not found or python-dotenv not installed.")
# --- Fin Dependencias ---

# --- Constantes ---
DEFAULT_LLM_MODEL = "claude-3-haiku-20240307"
MAX_SCRAPED_ITEMS = 7
MAX_CONTENT_LENGTH = 700
TEMPLATE_FILE = "report_template.html" # Asegúrate que este archivo exista y tenga los nuevos placeholders
# --- Fin Constantes ---

# --- Funciones ---

def fix_double_encoding(text):
    """Intenta corregir texto con posible doble codificación usando ftfy."""
    if isinstance(text, str):
        try:
            # Aplicar fix_text y normalizar a NFC (Forma de Composición Normalizada)
            return ftfy.fix_text(text, normalization='NFC')
        except Exception as e:
            print_warning(f"ftfy encontró un error al procesar '{text[:50]}...': {e}. Devolviendo original.")
            return text # Devolver original si ftfy falla
    return text

def create_llm_analysis_prompt_en_structured_citations(municipality_data: dict, scraped_reports: list) -> str:
    """
    Construye el prompt para análisis ESTRUCTURADO en INGLÉS con CITACIONES.
    """
    prompt_lines = []
    # Usar fix_double_encoding aquí también por si acaso
    municipality_name = fix_double_encoding(municipality_data.get('municipality', 'N/A'))
    country_name = fix_double_encoding(municipality_data.get('country', 'N/A'))

    prompt_lines.append(f"You are an analyst specializing in urban waste management. Your task is to analyze the following web findings about {municipality_name}, {country_name} and generate an analytical summary formatted as specific HTML sections.")
    prompt_lines.append("The summary must contain the following HTML sections, ONLY if relevant information is found in the web findings:")
    prompt_lines.append("1. A section `<h2>Key Initiatives and Programs</h2>` followed by a `<ul>` list of specific programs, projects, or notable actions mentioned.")
    prompt_lines.append("2. A section `<h2>Identified Challenges</h2>` followed by a `<ul>` list describing specific problems, inefficiencies, or obstacles mentioned.")
    prompt_lines.append("3. A section `<h2>Data and Statistics</h2>` followed by a `<ul>` list of any specific figures, percentages, or quantitative data mentioned regarding waste generation, recycling rates, etc.")
    prompt_lines.append("4. A section `<h2>Collaborations and Partnerships</h2>` followed by a `<ul>` list mentioning any partnerships (e.g., public-private, international).")
    prompt_lines.append("If no relevant information is found for a specific section, OMIT that section entirely (including the <h2> tag).")
    prompt_lines.append("If NO relevant information is found for ANY section, respond ONLY with the single paragraph: `<p>No specific details on initiatives, challenges, data, or collaborations were found in the provided web search results for this municipality.</p>`")
    prompt_lines.append("Do NOT include a main title (like <h1>) or repeat the basic municipality information (population, total waste tons).")
    prompt_lines.append("\n**IMPORTANT CITATION INSTRUCTION:** When mentioning a specific finding or data point derived from the web findings, **you MUST include the source domain in parentheses** immediately after the point, like this: `Finding details (Source: example.com)`. Use the 'Source' field provided for each web finding below.")

    prompt_lines.append("\n--- Web Findings to Analyze ---")
    if scraped_reports:
        limited_reports = scraped_reports[:MAX_SCRAPED_ITEMS]
        prompt_lines.append(f"(Based on the {len(limited_reports)} most relevant results out of {len(scraped_reports)} found)")
        for i, report in enumerate(limited_reports):
            title = report.get('title', 'No Title')
            # Aplicar ftfy al contenido del scraping también
            content_snippet = fix_double_encoding(report.get('content', ''))
            source = report.get('source', 'Unknown Source') # Fuente para citación
            if len(content_snippet) > MAX_CONTENT_LENGTH:
                content_snippet = content_snippet[:MAX_CONTENT_LENGTH] + "..."
            prompt_lines.append(f"\n{i+1}. Source: {source} - Title: {title}")
            prompt_lines.append(f"   Content: {content_snippet}")
    else:
        prompt_lines.append("No specific web findings were provided for analysis.")

    prompt_lines.append("\n--- Required Output Format ---")
    prompt_lines.append("Respond ONLY with the requested HTML sections (<h2> and <ul><li> with citations) or the single <p> if no information is found. **Generate the response exclusively in English.** Do NOT add ANYTHING before the first <h2> or <p> tag or after the last </ul> or </p> tag. Do not use <html>, <body>, <head> tags, or HTML comments.")

    return "\n".join(prompt_lines)

def render_template(template_content: str, context: dict) -> str:
    """
    Rellena la plantilla HTML con el contexto proporcionado,
    formateando números y manejando valores None/NaN.
    """
    rendered = template_content

    # Mapeo de claves de contexto a placeholders de plantilla (si son diferentes)
    key_mapping = {
        "municipality_name": "municipality",
        "country_name": "country"
        # Añadir más si los nombres en la plantilla difieren de las claves del JSON
    }

    # Crear un contexto de renderizado para no modificar el original
    render_context = context.copy()
    for template_key, context_key in key_mapping.items():
        if context_key in render_context:
            render_context[template_key] = render_context[context_key]

    # Iterar sobre todas las claves esperadas en la plantilla (o en el contexto)
    # Esto asegura que todos los placeholders se intenten reemplazar.
    # Obtener placeholders de la plantilla: {{key}}
    placeholders = re.findall(r"\{\{([^}]+)\}\}", template_content)

    na_html = '<span class="na-value">N/A</span>' # HTML para N/A

    for key in placeholders:
        key = key.strip() # Quitar espacios
        value = render_context.get(key) # Obtener valor del contexto
        placeholder_value = na_html # Valor por defecto si no se encuentra o es None/NaN

        # Usar pd.notna para chequear None y NaN de pandas
        if pd.notna(value):
            if key in ['population', 'total_waste_tons_year','composition_food_organic', 'composition_glass', 'composition_metal',
             'composition_paper_cardboard', 'composition_plastic', 'composition_other'
            ]:
                try:
                    # Intentar convertir a int primero para quitar decimales innecesarios
                    placeholder_value = f"{int(float(value)):,}" # Formato con comas para miles
                except (ValueError, TypeError):
                    placeholder_value = html.escape(str(value)) # Escapar si no es número entero
            elif key in ['recycling_rate_percent', 'collection_coverage_population_percent']:
                 try:
                    # Intentar convertir a float
                    placeholder_value = f"{float(value):.1f}%" # Formato porcentaje con 1 decimal
                 except (ValueError, TypeError):
                    placeholder_value = html.escape(str(value)) # Escapar si no es número flotante
            elif key == 'generation_date':
                 placeholder_value = html.escape(str(value)) # Fecha ya formateada
            elif key == 'llm_model_name':
                 placeholder_value = html.escape(str(value)) # Nombre del modelo
            elif key == 'llm_analysis_section':
                 placeholder_value = str(value) # El análisis ya es HTML, no escapar
            else:
                # Para otros valores (texto), aplicar fix_encoding y escapar HTML
                placeholder_value = html.escape(fix_double_encoding(str(value)))
        # Si es None o NaN, placeholder_value se queda como na_html

        # Reemplazar el placeholder {{key}} con el valor formateado
        # Usar re.escape en la clave para manejar caracteres especiales si los hubiera
        rendered = re.sub(r"\{\{\s*" + re.escape(key) + r"\s*\}\}", placeholder_value, rendered)

    # Opcional: Reemplazar cualquier placeholder restante que no estuviera en el contexto
    # rendered = re.sub(r"\{\{[^}]+\}\}", na_html, rendered)

    return rendered


def generate_report_with_template(municipality_data_str: str, scraped_data_str: str, output_html_file: str, api_key: str, llm_model: str):
    """ Genera reporte HTML usando plantilla y análisis estructurado + citado de Claude. """
    municipality_name = "Unknown Municipality"
    try:
        # Cargar datos JSON
        municipality_data = json.loads(municipality_data_str)
        scraped_reports = json.loads(scraped_data_str)

        # Corregir encoding en nombre de municipio para logs y título
        municipality_name = fix_double_encoding(municipality_data.get('municipality', municipality_name))
        municipality_data['municipality'] = municipality_name # Actualizar en el dict
        if 'country' in municipality_data:
             municipality_data['country'] = fix_double_encoding(municipality_data['country'])
        # Corregir otros campos de texto si es necesario (aunque SFA1 ya debería haberlo hecho)
        if 'income_level' in municipality_data: municipality_data['income_level'] = fix_double_encoding(municipality_data.get('income_level'))
        if 'primary_collection_mode' in municipality_data: municipality_data['primary_collection_mode'] = fix_double_encoding(municipality_data.get('primary_collection_mode'))


        print_info(f"Generating structured report with citations for: {municipality_name} (Model: {llm_model})")

        if not api_key: raise ValueError("Anthropic API Key not provided.")

        # Leer plantilla
        try:
            template_path = Path(TEMPLATE_FILE)
            if not template_path.is_file():
                 raise FileNotFoundError(f"Template file '{TEMPLATE_FILE}' not found in current directory or specified path.")
            with open(template_path, 'r', encoding='utf-8') as f_template:
                template_content = f_template.read()
            print_info(f"Template '{TEMPLATE_FILE}' read.")
        except FileNotFoundError as e:
            print_error(str(e))
            sys.exit(1)
        except IOError as e:
             print_error(f"Error reading template file '{TEMPLATE_FILE}': {e}")
             sys.exit(1)


        # Llamada al LLM para análisis
        client = Anthropic(api_key=api_key)
        analysis_prompt = create_llm_analysis_prompt_en_structured_citations(municipality_data, scraped_reports)
        print_info(f"Calling Anthropic for structured analysis with citations...")
        llm_analysis_content = '<p class="na-value">Analysis could not be generated.</p>' # Default con estilo N/A
        try:
            message = client.messages.create(
                model=llm_model,
                max_tokens=1800,
                system="You are an expert assistant synthesizing web findings about municipal waste management into specific HTML sections, including source citations in parentheses. Respond ONLY with the requested HTML sections in English.",
                messages=[{"role": "user", "content": analysis_prompt}],
                temperature=0.3,
            )
            # Extraer texto de la respuesta
            analysis_text = ""
            if message.content and isinstance(message.content, list):
                 for block in message.content:
                      if hasattr(block, 'text'): analysis_text = block.text.strip(); break
            elif hasattr(message, 'text'): analysis_text = message.text.strip()

            # Validar inicio de la respuesta
            no_info_msg = "No specific details on initiatives"
            if analysis_text and (analysis_text.startswith('<h2>') or (analysis_text.startswith('<p>') and no_info_msg in analysis_text)):
                 llm_analysis_content = analysis_text
                 print_success("LLM analysis generated successfully.")
            elif analysis_text:
                 print_warning("LLM response did not start with <h2> or the 'no info' <p>. Using raw response wrapped in <p>.")
                 llm_analysis_content = f"<p>{html.escape(analysis_text)}</p>" # Escapar por si acaso
            else:
                 print_warning("LLM response was empty.")
                 llm_analysis_content = '<p class="na-value">No analysis provided by LLM.</p>'

        # Manejo de errores específicos de la API
        except RateLimitError: print_error(f"Rate Limit Error for {municipality_name}."); llm_analysis_content = '<p class="na-value"><strong>Error:</strong> API rate limit exceeded.</p>'
        except APIConnectionError as e: print_error(f"Connection Error for {municipality_name}: {e}"); llm_analysis_content = '<p class="na-value"><strong>Error:</strong> API connection failed.</p>'
        except APIError as e: print_error(f"API Error for {municipality_name}: {e.status_code} - {e.message}"); llm_analysis_content = f'<p class="na-value"><strong>API Error {e.status_code}</strong>.</p>'
        except Exception as e: print_error(f"LLM call error for {municipality_name}: {e}"); llm_analysis_content = '<p class="na-value"><strong>Unexpected error</strong> during analysis generation.</p>'

        # Preparar contexto final para la plantilla
        generation_date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC") # Usar timezone.utc
        context = municipality_data.copy() # Empezar con los datos del municipio
        context["llm_analysis_section"] = llm_analysis_content
        context["generation_date"] = generation_date_str
        context["llm_model_name"] = llm_model # Añadir el nombre del modelo al contexto

        # Asegurar que todas las claves esperadas por la plantilla existan en el contexto
        # (incluso si son None, para que render_template las maneje)
        expected_keys = ['municipality', 'country', 'population', 'total_waste_tons_year',
                         'income_level', 'recycling_rate_percent',
                         'collection_coverage_population_percent', 'primary_collection_mode']
        for key in expected_keys:
            context.setdefault(key, None)

        # Renderizar la plantilla
        final_html = render_template(template_content, context)

        # Guardar reporte
        output_path = Path(output_html_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        print_info(f"Saving structured HTML report (with citations) to: {output_html_file}")
        with open(output_path, 'w', encoding='utf-8') as f: f.write(final_html)
        print_success(f"Report for {municipality_name} saved successfully.")

    # Manejo de errores generales
    except json.JSONDecodeError as e: print_error(f"JSON Decode Error processing data for {municipality_name}: {e}"); sys.exit(1)
    except ValueError as e: print_error(f"Value Error for {municipality_name}: {e}"); sys.exit(1)
    except FileNotFoundError as e: print_error(f"File Error: {e}"); sys.exit(1) # Más genérico por si falla lectura JSON
    except Exception as e: print_error(f"Unexpected error generating report for {municipality_name}: {e}"); import traceback; print(traceback.format_exc(), file=sys.stderr); sys.exit(1)

def main():
    # (Argument parser sin cambios)
    parser = argparse.ArgumentParser(description="SFA3 v2: Generates enriched HTML report with citations using template and LLM (Anthropic).")
    parser.add_argument("--municipality-data-json", required=True, help="JSON string with data for one municipality.")
    parser.add_argument("--scraped-data-json", required=True, help="JSON string with scraping results.")
    parser.add_argument("--output-html-file", required=True, help="Full path to save the output HTML file.")
    parser.add_argument("--api-key", help="Anthropic API Key (or use ANTHROPIC_API_KEY env var).")
    parser.add_argument("--llm-model", default=DEFAULT_LLM_MODEL, help=f"Anthropic model (default: {DEFAULT_LLM_MODEL}).")
    args = parser.parse_args()
    api_key = args.api_key or os.getenv("ANTHROPIC_API_KEY")

    generate_report_with_template(args.municipality_data_json, args.scraped_data_json, args.output_html_file, api_key, args.llm_model)

if __name__ == "__main__":
    main()
