import pandas as pd
import re
import numpy as np
from pathlib import Path
import argparse
import unicodedata # Usado en fallback de unidecode
import csv # Importar módulo csv

# --- Funciones Auxiliares ---

# Fallback si unidecode no está instalado
try:
    from unidecode import unidecode
except ImportError:
    print("ADVERTENCIA: La librería 'unidecode' no está instalada. Usando fallback para normalizar nombres.")
    def unidecode(s: str) -> str:
        try:
            s = unicodedata.normalize('NFD', s)
            return "".join(c for c in s if unicodedata.category(c) != 'Mn')
        except TypeError: return str(s)

def parse_study_name(study_name):
    """
    Intenta extraer Ciudad, País y Año del campo study_name.
    Devuelve un diccionario con 'municipality', 'country', 'parsed_year'.
    (Sin cambios respecto a la versión anterior)
    """
    municipality, country, parsed_year = None, None, None
    if not isinstance(study_name, str):
        return {'municipality': None, 'country': None, 'parsed_year': None}
    study_name_clean = study_name.strip()
    year_match = re.search(r'[,\-\s](\d{4})$', study_name_clean)
    if year_match:
        try:
            parsed_year = int(year_match.group(1))
            if not (1980 <= parsed_year <= 2050): parsed_year = None
        except ValueError: parsed_year = None
        base_name = study_name_clean[:year_match.start()].strip(' ,-') if parsed_year else study_name_clean
    else:
        base_name = study_name_clean
    parts = re.split(r'\s*[,]\s*(?=[A-Z])|\s+-\s+(?=[A-Z])', base_name, maxsplit=1)
    if len(parts) == 2:
        municipality = parts[0].strip(' ,-')
        country = parts[1].strip(' ,-')
    elif len(parts) == 1:
        common_countries = sorted([
            "Bosnia and Herzegovina", "Dominican Republic", "Sierra Leone",
            "South Korea", "North Macedonia", "Sint Maarten", "The Bahamas", "The Gambia",
            "Montenegro", "Guatemala", "Bangladesh", "Philippines", "Honduras",
            "Colombia", "Ethiopia", "Vietnam", "Morocco", "Albania", "Mexico",
            "Brazil", "Greece", "Serbia", "Belize", "Kosovo", "India", "Ghana", "Yemen",
            "Egypt", "Iran"
        ], key=len, reverse=True)
        found_country = None
        for c in common_countries:
            if re.search(r'\b' + re.escape(c) + r'\s*$', base_name, re.IGNORECASE):
                possible_mun = base_name[:-len(c)].strip(' ,-')
                if possible_mun:
                    municipality = possible_mun
                    country = c
                    found_country = True
                    break
        if not found_country: municipality = base_name; country = None
    else: municipality = base_name; country = None
    if municipality: municipality = municipality.strip(' ,-')
    if country: country = country.strip(' ,-')
    if country and "Kosovo" in country: country = "Kosovo"
    return {'municipality': municipality, 'country': country, 'parsed_year': parsed_year}

# --- *** FUNCIÓN load_descriptions MODIFICADA *** ---
def load_descriptions(desc_file_path):
    """
    Carga el archivo de descripciones CSV usando el módulo csv para mayor robustez
    y crea un mapeo de variable_name -> {description, component, unit}.
    """
    mapping = {}
    expected_fields = 8 # Número de columnas esperado según la cabecera
    header = []
    processed_lines = 0
    skipped_lines = 0

    try:
        with open(desc_file_path, 'r', encoding='utf-8', newline='') as csvfile:
            # Usar csv.reader con manejo estricto de comillas
            reader = csv.reader(csvfile, delimiter=',', quotechar='"', skipinitialspace=True)

            # Leer cabecera
            try:
                header_raw = next(reader)
                # Limpiar cabecera
                header = [h.strip().lower().replace('[^a-z0-9_]+', '_') for h in header_raw]
                print(f"Cabecera leída del archivo de descripciones: {header}")
                if len(header) != expected_fields:
                     print(f"Advertencia: La cabecera tiene {len(header)} campos, se esperaban {expected_fields}.")
                     # Intentar encontrar las columnas clave por nombre
                     try:
                         var_name_idx = header.index("variable_name")
                         desc_idx = header.index("question_indicator_description")
                         comp_idx = header.index("component")
                         unit_idx = header.index("unit")
                     except ValueError as ve:
                          print(f"Error: No se encontraron todas las columnas clave en la cabecera: {ve}")
                          return None
                else:
                     # Asumir índices estándar si el número de campos coincide
                     var_name_idx = 2
                     desc_idx = 3
                     comp_idx = 4
                     unit_idx = 5

            except StopIteration:
                print("Error: Archivo de descripción vacío o sin cabecera.")
                return None

            # Leer filas de datos
            for i, row in enumerate(reader):
                line_num = i + 2 # +1 por índice base 0, +1 por cabecera
                processed_lines += 1
                # Intentar manejar filas con más campos (posiblemente por comillas/saltos de línea mal manejados)
                # Tomando solo los primeros 'expected_fields' si hay más.
                if len(row) != expected_fields:
                    print(f"Advertencia: Línea {line_num} tiene {len(row)} campos, se esperaban {expected_fields}. Se intentará procesar igualmente.")
                    # Intentar unir campos extra si parecen ser parte del último campo (Reliability guidance)
                    if len(row) > expected_fields and expected_fields > 0:
                         row = row[:expected_fields-1] + [','.join(row[expected_fields-1:])]


                # Asegurarse de tener suficientes campos después del ajuste
                if len(row) >= max(var_name_idx, desc_idx, comp_idx, unit_idx) + 1:
                    var_name = row[var_name_idx].strip()
                    if var_name: # Solo procesar si hay un variable_name
                        mapping[var_name] = {
                            'description': row[desc_idx].strip(),
                            'component': row[comp_idx].strip() if row[comp_idx].strip() else None,
                            'unit': row[unit_idx].strip() if row[unit_idx].strip() else None
                        }
                else:
                    print(f"Advertencia: Omitiendo línea {line_num} por tener muy pocos campos ({len(row)}) después del procesamiento.")
                    skipped_lines += 1


        print(f"Descripciones procesadas: {len(mapping)} mapeos creados de {processed_lines} líneas leídas ({skipped_lines} omitidas).")
        if skipped_lines > 0:
             print("Advertencia: Algunas líneas del archivo de descripciones fueron omitidas por problemas de formato.")
        return mapping

    except FileNotFoundError:
        print(f"Error: Archivo de descripción no encontrado en {desc_file_path}")
        return None
    except Exception as e:
        print(f"Error cargando o procesando el archivo de descripción: {e}")
        import traceback
        traceback.print_exc() # Imprimir traceback completo para depuración
        return None
# --- FIN FUNCIÓN load_descriptions MODIFICADA ---


def clean_percentage(value):
    """Convierte a float y maneja valores anómalos."""
    try:
        num = float(value)
        if num < 0 or num > 1000: return np.nan
        return num
    except (ValueError, TypeError): return np.nan

def clean_numeric(value):
    """Convierte a float."""
    try:
        if isinstance(value, str): value = value.replace(',', '')
        return float(value)
    except (ValueError, TypeError): return np.nan

def create_measurement_name(var_name, desc_map):
    """Crea un nombre descriptivo para la tabla de mediciones."""
    # (Sin cambios respecto a la versión anterior)
    if var_name not in desc_map: return var_name
    info = desc_map[var_name]
    desc = info.get('description', '')
    comp = info.get('component')
    name = desc.lower()
    if comp:
        if comp.lower() not in name: name += f"_{comp.lower()}"
    name = name.replace('%', 'pct').replace('/', '_').replace('&', 'and')
    name = re.sub(r'[^a-z0-9_]+', '_', name)
    name = re.sub(r'_+', '_', name).strip('_')
    if var_name.endswith('_meta'): name += "_source_methodology"
    elif var_name.endswith('_comments'): name += "_comments"
    elif var_name.endswith('_reliability'): name += "_reliability_score"
    elif info.get('unit') == 'Score 1-7': name += "_score_1_7"
    elif info.get('unit') == 'Score 0-1': name += "_score_0_1"
    elif info.get('unit') == 't/d': name += "_tonnes_per_day"
    elif info.get('unit') == 'kg/cap/day': name += "_kg_per_capita_per_day"
    elif info.get('unit') == '%': name += "_percent"
    name = re.sub(r'_+', '_', name).strip('_')
    return name[:100]

# --- Mapeo de País a ISO3 ---
COUNTRY_TO_ISO3_MAP = {
    "Montenegro": "MNE", "India": "IND", "Mexico": "MEX", "Brazil": "BRA",
    "Greece": "GRC", "Serbia": "SRB", "Morocco": "MAR", "Vietnam": "VNM",
    "Ethiopia": "ETH", "Philippines": "PHL", "Honduras": "HND", "Guatemala": "GTM",
    "Belize": "BLZ", "Dominican Republic": "DOM", "Ghana": "GHA", "Sierra Leone": "SLE",
    "Bangladesh": "BGD", "Albania": "ALB", "Kosovo": "UNK", "Colombia": "COL",
    "Bosnia And Herzegovina": "BIH",
}

# --- Script Principal ---
def main(wfd_csv_path, desc_csv_path, output_cities_csv, output_measurements_csv):
    print(f"Cargando descripciones desde: {desc_csv_path}")
    description_map = load_descriptions(desc_csv_path) # Usa la nueva función
    if description_map is None: return

    print(f"Cargando datos WFD desde: {wfd_csv_path}")
    try:
        df_wfd = pd.read_csv(wfd_csv_path, skiprows=1, dtype=str)
        print(f"Leídas {len(df_wfd)} filas del archivo WFD.")
    except FileNotFoundError:
        print(f"Error: Archivo de datos WFD no encontrado en {wfd_csv_path}"); return
    except Exception as e:
        print(f"Error cargando el archivo de datos WFD: {e}"); return

    cities_data_list = []
    measurements_data_list = []
    countries_not_mapped = set()

    print("Procesando filas, separando datos y añadiendo ISO3c...")
    for index, row in df_wfd.iterrows():
        study_info = parse_study_name(row.get('study_name'))
        municipality = study_info['municipality']
        country_parsed = study_info['country']
        parsed_year = study_info['parsed_year']
        year_val = row.get('study_year')
        try:
            if pd.notna(parsed_year): year = int(parsed_year)
            else: year = int(year_val)
        except (ValueError, TypeError): year = None

        if not municipality:
            print(f"Advertencia: No se pudo extraer nombre de municipio para fila {index+2} ('{row.get('study_name')}'). Omitiendo fila.")
            continue

        iso3c = "UNK"
        country_normalized_for_map = None
        if country_parsed:
            country_normalized_for_map = country_parsed.title()
            iso3c = COUNTRY_TO_ISO3_MAP.get(country_normalized_for_map, "UNK")
            if iso3c == "UNK": countries_not_mapped.add(country_parsed)

        # Preparar fila para cities_data
        city_record = {
            'municipality': municipality, 'country': country_parsed, 'iso3c': iso3c, 'year': year,
            'original_wfd_id': clean_numeric(row.get('ID')),
            'population_served': clean_numeric(row.get('q_1')),
            'waste_gen_rate_kg_cap_day': clean_numeric(row.get('q_2')),
            'comp_pct_food_organic': clean_percentage(row.get('q_3_1')),
            'comp_pct_paper_cardboard': clean_percentage(row.get('q_3_2')),
            'comp_pct_plastic': clean_percentage(row.get('q_3_3')),
            'comp_pct_glass': clean_percentage(row.get('q_3_4')),
            'comp_pct_metal': clean_percentage(row.get('q_3_5')),
            'comp_pct_other': clean_percentage(row.get('q_3_6')),
            'collection_pct_formal': clean_percentage(row.get('q_8_1')),
            'collection_pct_informal': clean_percentage(row.get('q_8_2')),
            'treatment_pct_recycling_composting': clean_percentage(row.get('q_9_1')),
            'treatment_pct_thermal': clean_percentage(row.get('q_9_2')),
            'treatment_pct_landfill': clean_percentage(row.get('q_9_3')),
        }
        cities_data_list.append(city_record)

        # Preparar filas para measurements_data
        for col_name, value in row.items():
            if col_name in city_record or col_name in ['study_name', 'study_year', 'study_id', 'set_id', 'deleted_at', 'updated_at'] or pd.isna(value) or value == '':
                continue

            if col_name in description_map:
                measurement_name = create_measurement_name(col_name, description_map)
                unit = description_map[col_name].get('unit')
                measurement_value = value
                if unit in ['Score 0-1', 'Score 1-7', 't/d', '%', 'Number']:
                    cleaned_value = clean_numeric(value)
                    if pd.isna(cleaned_value): continue
                    else: measurement_value = cleaned_value

                measurement_record = {
                    'municipality': municipality, 'country': country_parsed, 'iso3c': iso3c, 'year': year,
                    'measurement_name': measurement_name, 'measurement_value': measurement_value,
                    'measurement_unit': unit, 'original_variable_name': col_name
                }
                measurements_data_list.append(measurement_record)

    if countries_not_mapped:
        print(f"ADVERTENCIA: No se encontró mapeo ISO3 para los siguientes nombres de país extraídos (se asignó 'UNK'): {sorted(list(countries_not_mapped))}")
        print("Por favor, revisa la función 'parse_study_name' y el diccionario 'COUNTRY_TO_ISO3_MAP' en el script.")

    df_cities = pd.DataFrame(cities_data_list)
    df_measurements = pd.DataFrame(measurements_data_list)

    comp_cols = [c for c in df_cities.columns if c.startswith('comp_pct_')]
    if comp_cols:
        for col in comp_cols: df_cities[col] = pd.to_numeric(df_cities[col], errors='coerce')
        df_cities['composition_sum_pct'] = df_cities[comp_cols].sum(axis=1)
        df_cities['composition_sum_flag'] = df_cities['composition_sum_pct'].notna() & \
                                            ~df_cities['composition_sum_pct'].between(95, 105, inclusive='both')
        print(f"Se marcaron {df_cities['composition_sum_flag'].sum()} ciudades con suma de composición fuera del rango [95, 105]%.")
    else:
        print("Advertencia: No se encontraron columnas de composición ('comp_pct_*') para calcular la suma.")
        df_cities['composition_sum_pct'] = np.nan; df_cities['composition_sum_flag'] = False

    print(f"Guardando datos de ciudades limpias (con ISO3c) en: {output_cities_csv}")
    try:
        output_cities_path = Path(output_cities_csv)
        output_cities_path.parent.mkdir(parents=True, exist_ok=True)
        city_cols_order = ['municipality', 'country', 'iso3c', 'year', 'original_wfd_id',
                           'population_served', 'waste_gen_rate_kg_cap_day',
                           'comp_pct_food_organic', 'comp_pct_paper_cardboard', 'comp_pct_plastic',
                           'comp_pct_glass', 'comp_pct_metal', 'comp_pct_other',
                           'collection_pct_formal', 'collection_pct_informal',
                           'treatment_pct_recycling_composting', 'treatment_pct_thermal',
                           'treatment_pct_landfill', 'composition_sum_pct', 'composition_sum_flag']
        df_cities_output = df_cities[[col for col in city_cols_order if col in df_cities.columns]].copy()
        df_cities_output.to_csv(output_cities_path, index=False, encoding='utf-8')
        print("¡Archivo de ciudades limpias guardado exitosamente!")
    except Exception as e: print(f"Error al guardar el archivo CSV de ciudades: {e}")

    print(f"Guardando datos de mediciones limpias (con ISO3c) en: {output_measurements_csv}")
    try:
        output_measurements_path = Path(output_measurements_csv)
        output_measurements_path.parent.mkdir(parents=True, exist_ok=True)
        measurement_cols_order = ['municipality', 'country', 'iso3c', 'year',
                                  'measurement_name', 'measurement_value',
                                  'measurement_unit', 'original_variable_name']
        df_measurements_output = df_measurements[[col for col in measurement_cols_order if col in df_measurements.columns]].copy()
        df_measurements_output.to_csv(output_measurements_path, index=False, encoding='utf-8')
        print("¡Archivo de mediciones limpias guardado exitosamente!")
    except Exception as e: print(f"Error al guardar el archivo CSV de mediciones: {e}")

# --- Ejecución ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Limpia, separa y añade ISO3c al archivo CSV WFD Baselines (v2 - Lectura Descripciones Mejorada).")
    parser.add_argument("wfd_csv", help="Ruta al archivo CSV principal (wfd-baselines.csv).")
    parser.add_argument("desc_csv", help="Ruta al archivo CSV con las descripciones de columnas (corregido).")
    parser.add_argument("output_cities", help="Ruta donde guardar el CSV limpio de datos de ciudades (con ISO3c).")
    parser.add_argument("output_measurements", help="Ruta donde guardar el CSV limpio de mediciones/metadatos (con ISO3c).")
    args = parser.parse_args()
    main(args.wfd_csv, args.desc_csv, args.output_cities, args.output_measurements)
