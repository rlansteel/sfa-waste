# /// script
# dependencies = [
#   "pandas>=2.0.0",
#   "rich>=13.7.0",
#   "ftfy>=6.0",
#   "anthropic>=0.20.0",
#   "python-dotenv>=0.20.0",
#   "numpy>=1.20.0"
# ]
# ///

"""
Single File Agent (SFA) 1: Generic CSV Processor (Iteración 7 - v7 Config Driven)

Lee un CSV y un archivo de configuración JSON. Usa LLM para mapeo dinámico
basado en config, corrige encoding, valida, imputa selectivamente (según config),
ejecuta pasos personalizados (según config, ej. Recalcular Otros),
y guarda JSON limpio y trazable.
"""

import argparse
import pandas as pd
import json # MODIFICACIÓN: Importar json
import os
import sys
from pathlib import Path
import time
import ftfy
import csv
import numpy as np
from typing import Union, Dict, Any, List # MODIFICACIÓN: Tipos más específicos

# --- Dependencias y Configuración Visual (Sin cambios) ---
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    console = Console(stderr=True); USE_RICH = True
    def print_info(message): console.print(f"[cyan]ℹ️ {message}[/cyan]")
    def print_success(message): console.print(f"[green]✅ {message}[/green]")
    def print_warning(message): console.print(f"[yellow]⚠️ {message}[/yellow]")
    def print_error(message): console.print(Panel(f"[bold red]❌ ERROR:[/bold red]\n{message}", border_style="red"))
    def print_heading(title): console.print(f"\n[bold underline magenta]{title}[/bold underline magenta]\\n")
except ImportError:
    USE_RICH = False
    # ... (funciones print sin Rich) ...
    def print_info(message): print(f"INFO: {message}", file=sys.stderr)
    def print_success(message): print(f"SUCCESS: {message}", file=sys.stderr)
    def print_warning(message): print(f"WARNING: {message}", file=sys.stderr)
    def print_error(message): print(f"ERROR: {message}", file=sys.stderr)
    def print_heading(title): print(f"\n--- {title} ---\n", file=sys.stderr)

try:
    from anthropic import Anthropic, APIError, RateLimitError, APIConnectionError
except ImportError:
    print_error("La biblioteca 'anthropic' no está instalada. Ejecuta: pip install anthropic")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print_warning("python-dotenv not installed...")
# --- Fin Dependencias ---

# --- Constantes (Configuración General) ---
# MODIFICACIÓN: Eliminadas constantes codificadas como TARGET_CONCEPTS, etc.
# Se leerán del archivo de configuración.
DEFAULT_LLM_MODEL = "claude-3-haiku-20240307" # Aún podemos tener un default para el modelo
MAX_HEADER_ROWS = 5
LLM_RETRY_DELAY = 5
LLM_MAX_RETRIES = 2
# --- Fin Constantes ---

# --- Funciones Auxiliares ---
# fix_double_encoding (Sin cambios)
def fix_double_encoding(text):
    if isinstance(text, str):
        try: return ftfy.fix_text(text, normalization='NFC')
        except Exception: return text
    return text

# read_csv_header (Sin cambios)
def read_csv_header(file_path: str, num_rows: int) -> tuple[list[str], list[list[str]], str | None]:
    encodings_to_try = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
    detected_encoding = None
    delimiter = ',' # Start assuming comma
    for encoding in encodings_to_try:
        try:
            print_info(f"Intentando leer cabecera con codificación: {encoding} y delimitador '{delimiter}'")
            # Use csv module to handle potential quoting issues better during header read
            headers = []
            data_rows = []
            with open(file_path, 'r', encoding=encoding, newline='') as f:
                reader = csv.reader(f, delimiter=delimiter)
                headers = next(reader) # Read first line as header
                for i, row in enumerate(reader):
                    if i >= num_rows: break
                    data_rows.append([str(item) if item is not None else "" for item in row]) # Ensure strings

            # Basic check if headers seem reasonable (e.g., not just one long string)
            if len(headers) <= 1 and delimiter == ',':
                 raise ValueError("Pocas cabeceras detectadas con coma, podría ser otro delimitador.")

            detected_encoding = encoding
            print_info(f"Lectura de cabecera exitosa con {encoding} y delimitador '{delimiter}'.")
            return headers, data_rows, detected_encoding
        except (UnicodeDecodeError, ValueError) as e:
            print_warning(f"Falló lectura/decodificación con {encoding} y delimitador '{delimiter}': {e}")
            # Try sniffing if standard comma failed or gave suspicious headers
            if delimiter == ',': # Only sniff if comma failed
                try:
                    print_info(f"Intentando detectar delimitador para {encoding}...")
                    sniffer = csv.Sniffer()
                    with open(file_path, 'r', encoding=encoding, newline='') as f:
                        # Read enough lines for sniffing, handle potential short files
                        sample = "".join(f.readline() for _ in range(min(20, num_rows + 10)))
                        if not sample: continue # Skip if file seems empty
                    dialect = sniffer.sniff(sample)
                    detected_delimiter = dialect.delimiter
                    if detected_delimiter != ',':
                        print_info(f"Delimitador detectado: '{detected_delimiter}' para {encoding}. Reintentando lectura...")
                        delimiter = detected_delimiter # Use detected delimiter for next attempt
                        # Rerun the reading logic with the new delimiter
                        with open(file_path, 'r', encoding=encoding, newline='') as f:
                             reader = csv.reader(f, delimiter=delimiter)
                             headers = next(reader)
                             data_rows = []
                             for i, row in enumerate(reader):
                                 if i >= num_rows: break
                                 data_rows.append([str(item) if item is not None else "" for item in row])
                        detected_encoding = encoding
                        print_info(f"Lectura de cabecera exitosa con {encoding} y delimitador '{delimiter}'.")
                        return headers, data_rows, detected_encoding
                    else:
                         print_warning(f"Sniffer confirmó delimitador coma, pero la lectura falló.")
                except csv.Error as sniff_error:
                     print_warning(f"No se pudo detectar delimitador con {encoding}: {sniff_error}")
                except Exception as read_error:
                     print_warning(f"No se pudo leer con delimitador detectado y {encoding}: {read_error}")
            # Reset delimiter for next encoding attempt if sniffing didn't change it
            delimiter = ','

        except Exception as e:
            print_warning(f"Error inesperado al leer cabecera con {encoding} y delimitador '{delimiter}': {e}")
            delimiter = ',' # Reset delimiter

    raise ValueError(f"No se pudo leer la cabecera del archivo CSV '{file_path}' con las codificaciones y delimitadores probados.")


# define_column_mapping_tool (MODIFICADO para usar config)
def define_column_mapping_tool(target_concepts: Dict[str, str]) -> dict:
    """Define la estructura de la herramienta LLM para el mapeo."""
    properties = {}
    for concept, description in target_concepts.items(): # MODIFICACIÓN: Usa target_concepts del argumento
        properties[concept] = {
            "type": ["string", "null"],
            "description": f"The exact column name from the CSV header that best represents: {description}. Use null if no suitable column is found."
        }
    tool_schema = {
        "name": "map_csv_columns_to_concepts",
        "description": f"Maps the required concepts ({', '.join(target_concepts.keys())}) to the most appropriate column names found in the provided CSV header and example rows. Returns null for a concept if no suitable column is identified.",
        "input_schema": { "type": "object", "properties": properties }
    }
    return tool_schema

# build_llm_prompt (MODIFICADO para usar config)
def build_llm_prompt(target_concepts: Dict[str, str], csv_headers: list[str], csv_data_rows: list[list[str]]) -> str:
    """Construye el prompt para el LLM."""
    # MODIFICACIÓN: Usa target_concepts del argumento
    prompt = f"""Analyze the following CSV header and example data rows to identify the columns that correspond *exactly* to the required concepts: {', '.join(target_concepts.keys())}.

CSV Header:
`{', '.join(csv_headers)}`

Example Data Rows (up to {MAX_HEADER_ROWS}):
"""
    for i, row in enumerate(csv_data_rows):
        if i >= MAX_HEADER_ROWS: break
        prompt += "`" + "`, `".join(map(str, row)) + "`\n"
    prompt += f"""
Based *only* on the provided header and data, determine the best matching column name for each of the following concepts:
"""
    for concept, description in target_concepts.items(): # MODIFICACIÓN: Usa target_concepts del argumento
        prompt += f"- **{concept}**: {description}\n"
    prompt += """
Use the 'map_csv_columns_to_concepts' tool to provide the mapping. For each concept, specify the *exact column name* from the header. If you cannot find a suitable column for a concept based *strictly* on the header and data provided, use the value `null` for that concept in the tool input. Do not guess or infer columns that are not clearly represented.
"""
    return prompt

# get_dynamic_column_mapping (MODIFICADO para usar config)
def get_dynamic_column_mapping(api_key: str, model: str, config: Dict[str, Any], csv_headers: list[str], csv_data_rows: list[list[str]]) -> dict | None:
    """Obtiene y valida el mapeo de columnas usando LLM y config."""
    # MODIFICACIÓN: Acepta 'config' como argumento
    if not api_key:
        print_error("No se proporcionó la API key de Anthropic."); return None
    client = Anthropic(api_key=api_key)

    target_concepts = config.get('target_concepts', {}) # MODIFICACIÓN: Obtiene conceptos del config
    if not target_concepts:
        print_error("No se encontraron 'target_concepts' en el archivo de configuración."); return None

    tool_definition = define_column_mapping_tool(target_concepts) # MODIFICACIÓN: Pasa target_concepts
    prompt = build_llm_prompt(target_concepts, csv_headers, csv_data_rows) # MODIFICACIÓN: Pasa target_concepts

    if USE_RICH:
        # ... (código Rich para mostrar prompt) ...
        prompt_lines = prompt.splitlines(); max_lines = 30
        display_prompt = "\n".join(prompt_lines[:max_lines])
        if len(prompt_lines) > max_lines: display_prompt += f"\n... (prompt truncated, {len(prompt_lines) - max_lines} more lines)"
        console.print(Panel(display_prompt, title="[bold blue]Prompt Enviado a Claude (Mapeo)[/bold blue]", border_style="blue", expand=False))
    else: print_info("--- Prompt Enviado a Claude (Mapeo) ---\n" + prompt + "\n--- Fin del Prompt (Mapeo) ---")

    retries = 0
    while retries <= LLM_MAX_RETRIES:
        try:
            print_info(f"Llamando a Anthropic ({model}) para mapeo (Intento {retries + 1}/{LLM_MAX_RETRIES + 1})...")
            message = client.messages.create(model=model, max_tokens=1500, # Aumentado por si config es grande
                system="You are an expert data analyst assisting in mapping CSV columns to predefined concepts based *strictly* on the provided header and sample data. Respond ONLY by using the provided tool. Use null if a concept cannot be mapped.",
                messages=[{"role": "user", "content": prompt}], tools=[tool_definition],
                tool_choice={"type": "tool", "name": tool_definition["name"]}, temperature=0.0)

            if message.content and isinstance(message.content, list):
                for block in message.content:
                    if block.type == 'tool_use' and block.name == tool_definition["name"]:
                        raw_mapping = block.input; tool_name = block.name
                        if USE_RICH:
                            # ... (código Rich para mostrar respuesta) ...
                             mapping_panel_content = Text(f"Tool Name: ", style="bold magenta") + Text(f"{tool_name}\n", style="magenta")
                             mapping_panel_content.append(Text("Parameters (Mapping):\n", style="bold green"))
                             mapping_panel_content.append(json.dumps(raw_mapping, indent=2))
                             console.print(Panel(mapping_panel_content, title="[bold green]Respuesta LLM: Uso de Herramienta Detectado[/bold green]", border_style="green", expand=False))
                        else: print_info(f"--- LLM usó herramienta: {tool_name} ---\n" + json.dumps(raw_mapping, indent=2) + "\n--- Fin Parámetros Herramienta ---")

                        print_success("Mapeo recibido. Validando..."); valid_mapping = {}; found_columns = set(); missing_concepts = []
                        # MODIFICACIÓN: Valida contra target_concepts del config
                        for concept in target_concepts.keys():
                            col_name = raw_mapping.get(concept)
                            if col_name is None or str(col_name).lower() == 'null' or col_name is False:
                                print_warning(f"  LLM no encontró columna para '{concept}'.")
                                missing_concepts.append(concept)
                            elif not isinstance(col_name, str) or col_name not in csv_headers:
                                print_warning(f"  LLM sugirió '{col_name}' para '{concept}', pero no es un nombre de columna válido/existente. Ignorando.")
                                missing_concepts.append(concept)
                            else:
                                valid_mapping[concept] = col_name; found_columns.add(col_name)

                        # MODIFICACIÓN: Usa required_concepts del config para validar
                        required_concepts = config.get('required_concepts', [])
                        critical_missing = [rc for rc in required_concepts if rc not in valid_mapping]
                        if critical_missing:
                             print_error(f"¡Error crítico! No se pudieron mapear conceptos requeridos: {critical_missing}."); return None

                        print_info(f"Mapeo validado final: {valid_mapping}"); print_info(f"Columnas originales a usar: {list(found_columns)}")
                        if missing_concepts: print_warning(f"Conceptos no mapeados o inválidos: {missing_concepts}")
                        return valid_mapping

            print_warning("El LLM no usó la herramienta como se esperaba o respuesta vacía.")
        except RateLimitError: print_warning(f"Rate Limit Error. Esperando {LLM_RETRY_DELAY}s...")
        except APIConnectionError as e: print_error(f"Error conexión Anthropic: {e}"); return None
        except APIError as e: print_error(f"Error API Anthropic: {e.status_code} - {e.message}"); return None
        except Exception as e: print_error(f"Error inesperado llamando a Anthropic: {e}"); import traceback; console.print(traceback.format_exc())

        retries += 1
        if retries <= LLM_MAX_RETRIES: print_info(f"Esperando {LLM_RETRY_DELAY}s antes reintento {retries}..."); time.sleep(LLM_RETRY_DELAY)

    print_error("Máximo reintentos LLM alcanzado."); return None
# --- Fin Mapeo LLM ---

# --- Función Principal de Procesamiento (MODIFICADA para usar config) ---
# Asegúrate de tener 'import os' y 'import pandas as pd', etc. al inicio del archivo

# ... (El resto de las funciones auxiliares como read_csv_header, define_column..., get_dynamic..., etc. permanecen igual que en v7) ...

# --- Función Principal de Procesamiento (CORREGIDA para v7.1) ---
def process_csv_and_impute(input_csv_path: str, output_json_path: str, config: Dict[str, Any], config_path: str, column_mapping: dict, detected_encoding: str):
    """
    Lee CSV, aplica limpieza/validación/imputación/procesamiento personalizado
    basado en el archivo de configuración, y guarda JSON limpio y trazable.
    (v7.1: Corrige bug de imputación condicional)
    """
    print_info(f"Iniciando procesamiento v7.1 (Config Driven): {input_csv_path}")
    # MODIFICACIÓN: Acceder a parámetros desde config (igual que v7)
    target_concepts = config.get('target_concepts', {})
    numeric_concepts = config.get('numeric_concepts', [])
    text_concepts = config.get('text_concepts', [])
    imputation_params = config.get('imputation_params', {})
    custom_processing_config = config.get('custom_processing', {})
    required_concepts = config.get('required_concepts', [])

    imputation_enabled = imputation_params.get('enabled', False)
    imputation_method = imputation_params.get('method', 'median')
    # MODIFICACIÓN: Asegurar que group_keys existan en el df procesado ANTES de usarlos
    imputation_group_keys_config = imputation_params.get('group_keys', [])
    skip_imputation_prefixes = imputation_params.get('skip_imputation_for_prefixes', [])

    if not column_mapping or not required_concepts or not any(rc in column_mapping for rc in required_concepts):
         raise ValueError(f"Mapeo de columnas dinámico inválido o faltan conceptos requeridos ({required_concepts}).")

    original_columns_to_read = list(set(val for val in column_mapping.values() if val is not None))
    rename_mapping = {v: k for k, v in column_mapping.items() if v is not None}
    initial_row_count = 0

    try:
        print_info(f"Leyendo CSV completo con {detected_encoding}, columnas mapeadas: {original_columns_to_read}")
        df = pd.read_csv(input_csv_path, encoding=detected_encoding, usecols=original_columns_to_read, dtype=str, low_memory=False)
        initial_row_count = len(df)
        print_info(f"CSV leído. {initial_row_count} filas iniciales.")

        df_processed = df.rename(columns=rename_mapping)
        renamed_cols_str = ", ".join([f"'{k}'->'{v}'" for k, v in rename_mapping.items()])
        print_info(f"Columnas renombradas a conceptos estándar: {renamed_cols_str}")

        # Corrección Encoding (igual que v7)
        print_info("Aplicando corrección de encoding (ftfy)...")
        text_columns_to_fix_now = [key for key in text_concepts if key in df_processed.columns]
        for col_concept in text_columns_to_fix_now:
             if not df_processed[col_concept].isnull().all():
                  original_col_str = df_processed[col_concept].astype(str)
                  df_processed[col_concept] = df_processed[col_concept].astype(str).apply(fix_double_encoding)
                  changes = (original_col_str != df_processed[col_concept].astype(str)).sum()
                  if changes > 0: print_info(f"  Se corrigieron {changes} valores en '{col_concept}'.")

        # Limpieza y Validación de Requeridos (igual que v7)
        essential_cols = [rc for rc in required_concepts if rc in df_processed.columns]
        if not essential_cols:
            print_warning(f"Ninguna columna requerida ({required_concepts}) encontrada tras mapeo. Se procesarán todas las filas.")
        else:
            print_info(f"Validando filas basadas en columnas requeridas: {essential_cols}")
            valid_row_mask = pd.Series(True, index=df_processed.index)
            for essential_col in essential_cols:
                 col_str = df_processed[essential_col].astype(str).str.strip().str.lower()
                 valid_row_mask &= ~col_str.isin(['', 'nan', 'na', 'n/a']) & df_processed[essential_col].notna()

            rows_before_dropping = len(df_processed)
            df_processed = df_processed[valid_row_mask].copy()
            rows_after_dropping = len(df_processed)
            rows_dropped = rows_before_dropping - rows_after_dropping
            if rows_dropped > 0: print_info(f"Se eliminaron {rows_dropped} filas por valores inválidos/faltantes en columnas requeridas ({essential_cols}).")

        # Validación, Conversión Numérica e Imputación SELECTIVA basada en Config
        print_info("Validando, convirtiendo y aplicando imputación/procesamiento según configuración...")
        data_quality_summary = []
        # MODIFICACIÓN: Verificar que las group keys existan en el DataFrame procesado
        imputation_group_keys = [k for k in imputation_group_keys_config if k in df_processed.columns]
        if len(imputation_group_keys) != len(imputation_group_keys_config):
             missing_group_keys = set(imputation_group_keys_config) - set(imputation_group_keys)
             print_warning(f"Claves de grupo para imputación ({missing_group_keys}) no encontradas en los datos mapeados. La imputación agrupada podría no funcionar.")

        grouping_key = imputation_group_keys[0] if imputation_group_keys else None # Usar la primera clave válida encontrada

        # MODIFICACIÓN: Itera sobre numeric_concepts del config
        for col_concept in numeric_concepts:
            stats = {'concept': col_concept, 'status': 'OK', 'missing_initial': 0, 'invalid_format': 0,
                     'imputed': 0, 'imputation_method': '-', 'missing_final': 0,
                     'mean': np.nan, 'median': np.nan, 'min': np.nan, 'max': np.nan}

            if col_concept not in df_processed.columns:
                stats['status'] = 'MISSING_COLUMN'
                data_quality_summary.append(stats)
                df_processed[col_concept] = np.nan
                df_processed[f"{col_concept}_status"] = 'missing_column'
                continue

            status_col_name = f"{col_concept}_status"
            df_processed[status_col_name] = 'original'

            col_as_str = df_processed[col_concept].astype(str).str.strip()
            original_nan_mask = df_processed[col_concept].isna() | col_as_str.isin(['', 'nan', 'na', 'n/a'])
            stats['missing_initial'] = int(original_nan_mask.sum())

            numeric_col = pd.to_numeric(df_processed[col_concept], errors='coerce')
            conversion_failed_mask = numeric_col.isna() & ~original_nan_mask
            stats['invalid_format'] = int(conversion_failed_mask.sum())
            if stats['invalid_format'] > 0:
                df_processed.loc[conversion_failed_mask, status_col_name] = 'invalid_format'
                stats['status'] = 'WARN'

            df_processed[col_concept] = numeric_col

            # --- CORRECCIÓN: Lógica de Imputación Condicional Revisada ---
            # Determinar si se debe *intentar* imputar esta columna específica
            should_skip_by_prefix = any(col_concept.startswith(prefix) for prefix in skip_imputation_prefixes)
            should_attempt_impute = imputation_enabled and not should_skip_by_prefix

            if should_attempt_impute:
                nan_to_impute_mask = df_processed[col_concept].isna()
                if nan_to_impute_mask.sum() > 0: # Solo proceder si hay NaNs que imputar
                    num_nans = nan_to_impute_mask.sum()
                    print_info(f"  Intentando imputar {num_nans} NaN en '{col_concept}' con {imputation_method.upper()}...")
                    imputation_method_used_text = None # Para la tabla resumen
                    imputed_values_series = df_processed[col_concept].copy() # Empezar con la columna actual
                    applied_mask = pd.Series(False, index=df_processed.index) # Mask para saber qué se imputó realmente

                    # Agrupada
                    current_grouping_key = grouping_key # Usar copia local por si falla
                    if imputation_method == 'median' and current_grouping_key and df_processed[current_grouping_key].nunique() > 1:
                        try:
                            group_values = df_processed.groupby(current_grouping_key, observed=True)[col_concept].transform('median')
                            # Aplicar solo donde el original es NaN y el valor agrupado NO es NaN
                            mask_to_apply_group = nan_to_impute_mask & group_values.notna()
                            if mask_to_apply_group.any():
                                imputed_values_series.loc[mask_to_apply_group] = group_values[mask_to_apply_group]
                                imputation_method_used_text = f'{imputation_method}_{current_grouping_key}'
                                applied_mask |= mask_to_apply_group # Marcar filas afectadas
                            else: current_grouping_key = None # Fallback
                        except Exception as e_grp:
                             print_warning(f"  Advertencia imputación agrupada para '{col_concept}': {e_grp}. Intentando global.")
                             current_grouping_key = None # Fallback

                    # Global (si agrupada falló o no aplica O para los restantes si la agrupada no cubrió todo)
                    nan_still_remaining_mask = imputed_values_series.isna() # Verificar NaNs restantes DESPUÉS del intento agrupado
                    if nan_still_remaining_mask.any(): # Solo si quedan NaNs
                        global_value = None
                        if imputation_method == 'median': global_value = df_processed[col_concept].median(skipna=True)
                        elif imputation_method == 'mean': global_value = df_processed[col_concept].mean(skipna=True)
                        # ... otros métodos ...

                        if pd.notna(global_value):
                             # Aplicar solo donde todavía es NaN
                             mask_to_apply_global = nan_still_remaining_mask
                             imputed_values_series.loc[mask_to_apply_global] = global_value
                             # Decidir qué método reportar si se usaron ambos
                             if imputation_method_used_text is None: # Si solo se usó global
                                 imputation_method_used_text = f'{imputation_method}_global'
                             else: # Si se usaron ambos (agrupado + global)
                                 imputation_method_used_text += '+global' # Indicar mixto
                             applied_mask |= mask_to_apply_global # Marcar filas afectadas
                        # Si no se pudo calcular global, los NaNs restantes permanecerán

                    # Actualizar DataFrame y Estadísticas *solo si algo cambió*
                    if applied_mask.any():
                        stats['imputed'] = int(applied_mask.sum())
                        df_processed.loc[applied_mask, status_col_name] = f"imputed_{imputation_method_used_text}" if imputation_method_used_text else "imputed_unknown"
                        stats['imputation_method'] = imputation_method_used_text if imputation_method_used_text else "-"
                        stats['status'] = 'IMPUTED' if stats['status'] == 'OK' else stats['status']
                        df_processed[col_concept] = imputed_values_series # Aplicar la serie con valores imputados

            # --- FIN CORRECCIÓN IMPUTACIÓN ---
            else: # Si no se intentó imputar
                if not imputation_enabled:
                    stats['imputation_method'] = 'Disabled (Global)'
                elif should_skip_by_prefix:
                    stats['imputation_method'] = 'Skipped (Prefix)'
                # stats['imputed'] permanece 0

            # Marcar NaN finales (igual que antes)
            final_nan_mask = df_processed[col_concept].isna()
            stats['missing_final'] = int(final_nan_mask.sum())
            if stats['missing_final'] > 0:
                 unimputed_mask = final_nan_mask & df_processed[status_col_name].isin(['original', 'invalid_format'])
                 df_processed.loc[unimputed_mask, status_col_name] = 'missing_unimputed'
                 # Solo marcar WARN si quedan NaNs y no es columna de composición recalculada (donde NaN es esperado si falla)
                 is_recalculated_other = (col_concept == custom_processing_config.get('recalculate_composition_other', {}).get('other_key', None) and
                                          custom_processing_config.get('recalculate_composition_other', {}).get('enabled', False))
                 if stats['status'] == 'OK' and not is_recalculated_other:
                      stats['status'] = 'WARN'
                 elif stats['status'] == 'IMPUTED' and not is_recalculated_other: # Si se imputó pero aún quedan NaNs (raro)
                      stats['status'] = 'WARN'


            # Calcular estadísticas finales (igual que antes)
            final_numeric_col = df_processed[col_concept].dropna()
            if not final_numeric_col.empty:
                stats['mean'] = final_numeric_col.mean()
                stats['median'] = final_numeric_col.median()
                stats['min'] = final_numeric_col.min()
                stats['max'] = final_numeric_col.max()

            data_quality_summary.append(stats)

        # --- Procesamiento Personalizado Condicional (igual que v7) ---
        recalc_config = custom_processing_config.get('recalculate_composition_other', {})
        if recalc_config.get('enabled', False):
            print_info("Aplicando procesamiento personalizado: Recalcular 'composition_other'...")
            specific_keys = recalc_config.get('specific_keys', [])
            other_key = recalc_config.get('other_key', 'composition_other')

            specific_cols_present = [col for col in specific_keys if col in df_processed.columns]

            if not specific_cols_present:
                print_warning(f"No se encontraron columnas específicas ({specific_keys}) para recalcular '{other_key}'.")
                df_processed['composition_calculation_status'] = 'error_no_specific_cols'
                if other_key not in df_processed.columns: df_processed[other_key] = np.nan
                if f"{other_key}_status" not in df_processed.columns: df_processed[f"{other_key}_status"] = 'missing_column'
            elif other_key not in df_processed.columns:
                 print_warning(f"La columna '{other_key}' definida en config no existe tras mapeo/procesamiento inicial.")
                 df_processed['composition_calculation_status'] = 'error_other_key_missing'
            else:
                # Suma y Recálculo
                df_processed['sum_specific_composition'] = df_processed[specific_cols_present].sum(axis=1, skipna=False)
                df_processed['recalculated_other'] = 100 - df_processed['sum_specific_composition']
                df_processed['recalculated_other'] = df_processed['recalculated_other'].clip(lower=0)

                recalc_failed_mask = df_processed['recalculated_other'].isna()
                df_processed['composition_calculation_status'] = np.where(recalc_failed_mask, 'failed_missing_specifics', 'recalculated')

                df_processed[other_key] = df_processed['recalculated_other'] # Actualizar la columna 'other'

                other_status_col = f"{other_key}_status"
                # Crear status col si no existe (puede pasar si 'other' no estaba en numeric_concepts)
                if other_status_col not in df_processed.columns:
                    df_processed[other_status_col] = 'original' # Default inicial

                # Actualizar estado basado en recálculo
                df_processed.loc[~recalc_failed_mask, other_status_col] = 'recalculated_other'
                df_processed.loc[recalc_failed_mask, other_status_col] = 'failed_missing_specifics'


                df_processed = df_processed.drop(columns=['sum_specific_composition', 'recalculated_other'])
                num_recalculated = (~recalc_failed_mask).sum(); num_failed = recalc_failed_mask.sum()
                print_success(f"  '{other_key}' recalculado para {num_recalculated} filas.")
                if num_failed > 0: print_warning(f"  Cálculo de '{other_key}' falló para {num_failed} filas.")
        # --- Fin Procesamiento Personalizado ---

        # --- CORRECCIÓN: Mostrar Tabla Resumen Mejorada ---
        if USE_RICH:
            input_filename = os.path.basename(input_csv_path)
            config_filename = os.path.basename(config_path)
            table_title = f"Resultados para {input_filename} (Config: {config_filename})"
            print_heading(f"Resumen de Calidad y Procesamiento ({'Imputación Habilitada' if imputation_enabled else 'Imputación Deshabilitada'})")

            summary_table = Table(title=table_title, show_header=True, header_style="bold magenta")
            summary_table.add_column("Métrica", style="dim", width=40)
            summary_table.add_column("Valor", justify="right")
            summary_table.add_row("Filas leídas inicialmente", f"{initial_row_count:,}")
            summary_table.add_row("Filas tras limpieza/validación", f"{len(df_processed):,}")
            summary_table.add_section()

            for stats in data_quality_summary:
                col = stats['concept']; status = stats['status']
                style = ""
                if status == 'MISSING_COLUMN': style = "bold red"
                elif status == 'WARN': style = "yellow"
                elif status == 'IMPUTED': style = "cyan"

                # Preparar texto de estado de imputación
                imputation_status_text = f"{stats['imputed']:,}"
                if stats['imputation_method'] != '-':
                     imputation_status_text += f" ({stats['imputation_method']})"

                summary_table.add_row(f"{col} - Estado General", Text(status, style=style))
                if status != 'MISSING_COLUMN':
                    summary_table.add_row(f"  └─ Originales", f"{len(df_processed) - stats['missing_initial'] - stats['invalid_format']:,}")
                    summary_table.add_row(f"  └─ Faltantes Iniciales (NaN)", f"{stats['missing_initial']:,}")
                    summary_table.add_row(f"  └─ Formato Inválido", f"{stats['invalid_format']:,}")
                    summary_table.add_row(f"  └─ Imputados", imputation_status_text) # Usar texto preparado
                    summary_table.add_row(f"  └─ Faltantes Finales", f"{stats['missing_final']:,}")
                    summary_table.add_row(f"  └─ Media Final", f"{stats['mean']:,.2f}" if pd.notna(stats['mean']) else "N/A")
                    summary_table.add_row(f"  └─ Mediana Final", f"{stats['median']:,.2f}" if pd.notna(stats['median']) else "N/A")
                    summary_table.add_row(f"  └─ Mínimo Final", f"{stats['min']:,.2f}" if pd.notna(stats['min']) else "N/A")
                    summary_table.add_row(f"  └─ Máximo Final", f"{stats['max']:,.2f}" if pd.notna(stats['max']) else "N/A")
                summary_table.add_section()

            # Añadir estado del recálculo si se hizo
            if 'composition_calculation_status' in df_processed.columns:
                 recalc_count = (df_processed['composition_calculation_status'] == 'recalculated').sum()
                 failed_count = (df_processed['composition_calculation_status'] == 'failed_missing_specifics').sum()
                 other_key_display = custom_processing_config.get('recalculate_composition_other', {}).get('other_key', 'Otros')
                 summary_table.add_row(f"[bold]Estado Recálculo '{other_key_display}'[/bold]", f"Éxito: {recalc_count}, Falló: {failed_count}")

            console.print(summary_table)
        # --- FIN CORRECCIÓN TABLA RESUMEN ---
        else:
            # (Salida simple sin Rich - no se actualiza aquí por brevedad)
            print("\n--- Resumen Calidad Datos (Config Driven) ---")
            # ...

        # Limpieza Final y Salida JSON (igual que v7)
        print_info(f"Procesamiento finalizado. {len(df_processed)} filas resultantes.")
        final_columns = list(rename_mapping.keys()) # Conceptos mapeados
        status_columns = [f"{key}_status" for key in list(target_concepts.keys()) if f"{key}_status" in df_processed.columns] # Usar target_concepts para asegurar todos los status
        custom_cols = [col for col in df_processed.columns if col not in final_columns and not col.endswith('_status')]

        output_columns = final_columns + status_columns + custom_cols
        df_output = df_processed[[col for col in output_columns if col in df_processed.columns]].copy()

        df_output = df_output.where(pd.notnull(df_output), None)
        output_data = df_output.to_dict(orient='records')

        output_dir = Path(output_json_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        print_info(f"Guardando datos procesados en: {output_json_path} (Encoding: UTF-8)")
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False, default=str)
        print_success(f"Archivo JSON limpio y trazable guardado con {len(output_data)} registros.")

    # ... (Manejo de excepciones igual que v7) ...
    except FileNotFoundError as e: print_error(f"Error archivo: {e}"); sys.exit(1)
    except ValueError as e: print_error(f"Error en datos/config: {e}"); sys.exit(1)
    except KeyError as e: print_error(f"Error columna/clave config: '{e}' no encontrada/procesable."); sys.exit(1)
    except Exception as e: print_error(f"Error inesperado: {e}"); import traceback; console.print(traceback.format_exc()); sys.exit(1)


def main():
    # MODIFICACIÓN: Añadir argumento --config-file (igual que v7)
    parser = argparse.ArgumentParser(description="SFA1 v7.1: Procesa CSV genérico usando archivo de configuración JSON.")
    parser.add_argument("--input-csv", required=True, help="Ruta al archivo CSV de entrada.")
    parser.add_argument("--output-json", required=True, help="Ruta para guardar el JSON de salida.")
    parser.add_argument("--config-file", required=True, help="Ruta al archivo de configuración JSON.")
    parser.add_argument("--api-key", help="Anthropic API Key (o var ANTHROPIC_API_KEY).")
    parser.add_argument("--llm-model", default=DEFAULT_LLM_MODEL, help=f"Modelo Anthropic (default: {DEFAULT_LLM_MODEL}).")
    args = parser.parse_args()

    api_key = args.api_key or os.getenv("ANTHROPIC_API_KEY")
    if not api_key: print_error("Falta API key Anthropic."); sys.exit(1)

    # Cargar archivo de configuración (igual que v7)
    try:
        print_info(f"Cargando configuración desde: {args.config_file}")
        with open(args.config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        print_success("Configuración cargada.")
        # Validación básica del config...
        if not isinstance(config.get('target_concepts'), dict) or not config['target_concepts']: raise ValueError("...")
        if not isinstance(config.get('numeric_concepts'), list): raise ValueError("...")
    except Exception as e: # Captura más genérica para errores de carga/validación
        print_error(f"Error cargando/validando configuración '{args.config_file}': {e}"); sys.exit(1)

    try:
        headers, data_rows, detected_encoding = read_csv_header(args.input_csv, MAX_HEADER_ROWS)
        if not detected_encoding: sys.exit(1)
        # Pasar config a las funciones (igual que v7)
        dynamic_mapping = get_dynamic_column_mapping(api_key, args.llm_model, config, headers, data_rows)
        if not dynamic_mapping: print_error("Fallo al obtener mapeo LLM."); sys.exit(1)
        # MODIFICACIÓN: Pasar config_path a process_csv para usar en título de tabla
        process_csv_and_impute(args.input_csv, args.output_json, config, args.config_file, dynamic_mapping, detected_encoding)

    # ... (Manejo de excepciones igual que v7) ...
    except FileNotFoundError as e: print_error(f"Error archivo: {e}"); sys.exit(1)
    except ValueError as e: print_error(f"Error valor: {e}"); sys.exit(1)
    except Exception as e: print_error(f"Error inesperado en main: {e}"); import traceback; console.print(traceback.format_exc()); sys.exit(1)

if __name__ == "__main__":
    main()