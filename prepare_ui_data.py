#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# /// script
# dependencies = [
#   "rich>=13.7.0",
#   "unidecode>=1.0",
#   "pandas>=2.0.0"
# ]
# ///

"""
SFA: Preparador de Datos JSON para UI desde SQLite (v7.4 - Corrección Nombres de Archivo Perfil)

Lee la base de datos SQLite 'waste_data_clean.db', los perfiles SFA3.1 generados
y los datos de extrapolación para generar archivos JSON estructurados
para ser consumidos por una interfaz de usuario web.
Crea un índice general y archivos detallados por país y ciudad.
NUEVO v7.4: Asegura que la carga de perfiles SFA3 coincida con la convención de nombres
             usada por sfa_profile_generator_v2_granular.py.
"""

import argparse
import json
import sqlite3
import sys
import re
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd

# --- Dependencias y Configuración Visual ---
try:
    from rich.console import Console
    from rich.progress import track
    console = Console(stderr=True); USE_RICH = True
    def print_info(message): console.print(f"[cyan]ℹ️ {message}[/cyan]")
    def print_success(message): console.print(f"[green]✅ {message}[/green]")
    def print_warning(message): console.print(f"[yellow]⚠️ {message}[/yellow]")
    def print_error(message): console.print(f"[bold red]❌ ERROR:[/bold red] {message}")
except ImportError:
    USE_RICH = False
    def print_info(message): print(f"INFO: {message}", file=sys.stderr)
    def print_success(message): print(f"SUCCESS: {message}", file=sys.stderr)
    def print_warning(message): print(f"WARNING: {message}", file=sys.stderr)
    def print_error(message): print(f"ERROR: {message}", file=sys.stderr)
    def track(sequence, description="Procesando..."): yield from sequence

try:
    from unidecode import unidecode
except ImportError:
    print_warning("La librería 'unidecode' no está instalada. Se usará un fallback.")
    def unidecode_fallback(s: str) -> str:
        import unicodedata
        try: s = unicodedata.normalize('NFKD', s); return "".join([c for c in s if not unicodedata.combining(c)])
        except TypeError: return str(s)
    unidecode = unidecode_fallback
# --- Fin Dependencias ---

# --- Constantes y Configuraciones ---
PROFILES_DIR_DEFAULT = Path("output/profiles_sfa3_ultra_refined/")
EXTRAPOLATIONS_DIR_DEFAULT = Path("output/extrapolations/")
OUTPUT_DIR_DEFAULT = Path("output/ui_data/")
UI_DATA_VERSION = "v7.4_sprint5_final_profile_fix"

def sanitize_filename(name: str) -> str:
    if not isinstance(name, str): name = str(name)
    try: name = unidecode(name)
    except Exception as e: print_warning(f"Fallo unidecode en '{name}': {e}.")
    name = re.sub(r'[^\w-]', '_', name)
    name = re.sub(r'_+', '_', name).strip('_')
    name = name[:100]
    if not name: name = "unnamed"
    return name.lower()

def create_connection(db_file: Path) -> Optional[sqlite3.Connection]:
    conn = None
    try:
        print_info(f"Conectando a la base de datos: {db_file}")
        if not db_file.exists(): print_error(f"Archivo DB '{db_file}' no existe."); return None
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        print_success("Conexión establecida.")
        return conn
    except sqlite3.Error as e: print_error(f"Error DB connect: {e}")
    return None

def fetch_all_countries_for_index(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                id as country_db_id, country_name, country_code_iso3, region, income_group_wb,
                latitude, longitude, latitude_geo_status, longitude_geo_status,
                data_quality_score,
                gdp_per_capita_usd,
                CASE
                    WHEN population_total > 0 AND total_msw_generated_tons_year IS NOT NULL
                    THEN (total_msw_generated_tons_year * 1000.0) / (population_total * 365.0)
                    ELSE NULL
                END AS waste_per_capita_kg_day
            FROM countries
            WHERE country_name IS NOT NULL AND country_code_iso3 IS NOT NULL
            ORDER BY country_name
        """)
        return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        print_error(f"Error fetch países index: {e}")
        return []
    except Exception as ex:
        print_error(f"Error inesperado en fetch_all_countries_for_index: {ex}")
        return []

def fetch_unique_values(conn: sqlite3.Connection, column_name: str, table_name: str) -> List[str]:
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT DISTINCT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL ORDER BY {column_name}")
        return [row[0] for row in cursor.fetchall() if row[0]]
    except sqlite3.Error as e: print_error(f"Error fetch únicos {column_name}: {e}"); return []

def fetch_cities_by_country_for_index(conn: sqlite3.Connection) -> Dict[str, List[Dict[str, Any]]]:
    cities_by_country = {}
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, iso3c, municipality, latitude, longitude, latitude_geo_status, longitude_geo_status,
                   data_quality_score
            FROM cities
            WHERE iso3c IS NOT NULL AND municipality IS NOT NULL
            ORDER BY iso3c, municipality
        """)
        rows = cursor.fetchall()
        for row_sqlite in rows:
            row_dict = dict(row_sqlite)
            iso3 = row_dict['iso3c']
            city_data = {
                "id": row_dict['id'], # Este es el city_db_id
                "name": row_dict['municipality'],
                "latitude": row_dict.get('latitude'),
                "longitude": row_dict.get('longitude'),
                "latitude_geo_status": row_dict.get('latitude_geo_status'),
                "longitude_geo_status": row_dict.get('longitude_geo_status'),
                "data_quality_score": row_dict.get('data_quality_score'),
            }
            if iso3 not in cities_by_country: cities_by_country[iso3] = []
            cities_by_country[iso3].append(city_data)
        return cities_by_country
    except sqlite3.Error as e: print_error(f"Error fetch ciudades index: {e}"); return {}

def fetch_entity_details(conn: sqlite3.Connection, table_name: str, identifier_column: str, identifier_value: Any) -> Optional[Dict[str, Any]]:
    try:
        cursor = conn.cursor()
        if not re.match(r'^[a-zA-Z0-9_]+$', table_name) or not re.match(r'^[a-zA-Z0-9_]+$', identifier_column):
            print_error(f"Nombre tabla/columna inválido: {table_name}, {identifier_column}"); return None
        cursor.execute(f'SELECT * FROM "{table_name}" WHERE "{identifier_column}" = ?', (identifier_value,))
        row = cursor.fetchone()
        return dict(row) if row else None
    except sqlite3.Error as e: print_error(f"Error fetch detalles {identifier_column}={identifier_value} de {table_name}: {e}"); return None
    except Exception as e: print_error(f"Error inesperado fetch detalles: {e}"); return None

def fetch_measurements_for_entity(conn: sqlite3.Connection, measurements_table_name: str, fk_column_name: str, entity_identifier: Any) -> List[Dict[str, Any]]:
    try:
        cursor = conn.cursor()
        if not re.match(r'^[a-zA-Z0-9_]+$', measurements_table_name) or not re.match(r'^[a-zA-Z0-9_]+$', fk_column_name):
            print_error(f"Nombre tabla/columna medición inválido: {measurements_table_name}, {fk_column_name}"); return []
        query = f"""
            SELECT measurement, units, year, source, comments
            FROM "{measurements_table_name}"
            WHERE "{fk_column_name}" = ?
            ORDER BY measurement, year
        """
        cursor.execute(query, (entity_identifier,))
        return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e: print_error(f"Error fetch mediciones '{measurements_table_name}' para '{fk_column_name}'='{entity_identifier}': {e}"); return []
    except Exception as ex: print_error(f"Error inesperado fetch_measurements: {ex}"); return []

def load_sfa3_profile(entity_type: str, 
                      identifier: str, # ISO3 para país (ej. "ARG"), ID numérico como string para ciudad (ej. "13")
                      profiles_dir: Path) -> Optional[Dict[str, Any]]:
    """ Carga el perfil JSON SFA3 para una entidad. """
    profile_filename = None
    if entity_type == "country":
        # sfa_profile_generator guarda como: ARG_country_profile.json (ISO3 en mayúsculas)
        profile_filename = f"{identifier.upper()}_country_profile.json"
    elif entity_type == "city":
        # sfa_profile_generator guarda como: city_13_city_profile.json
        profile_filename = f"city_{identifier}_city_profile.json"
    else:
        print_warning(f"Tipo de entidad '{entity_type}' no soportado para cargar perfil IA.")
        return None

    profile_path = profiles_dir / profile_filename
    if profile_path.is_file():
        try:
            with open(profile_path, 'r', encoding='utf-8') as f:
                profile_data_full = json.load(f)
            # Devolver solo la parte 'waste_profile' para insertarla en la UI
            return profile_data_full.get("waste_profile")
        except json.JSONDecodeError:
            print_warning(f"Error decodificando perfil JSON SFA3: {profile_path}")
        except Exception as e:
            print_warning(f"Error cargando perfil JSON SFA3 {profile_path}: {e}")
    else:
        print_warning(f"Perfil SFA3 no encontrado: {profile_path}")
    return None

def load_extrapolations(extrapolations_dir: Path, entity_type_plural: str) -> Dict[str, Dict[str, Any]]:
    filename = f"{entity_type_plural}_level_extrapolations.json" # ej. countries_level_extrapolations.json
    file_path = extrapolations_dir / filename
    if file_path.is_file():
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            id_key = "country_code_iso3" if entity_type_plural == "countries" else "id"
            return {str(item[id_key]): item.get("extrapolated_data", {}) for item in data if id_key in item}
        except json.JSONDecodeError: print_warning(f"Error decodificando JSON de extrapolación: {file_path}")
        except Exception as e: print_warning(f"Error cargando JSON de extrapolación {file_path}: {e}")
    else:
        print_warning(f"Archivo de extrapolación no encontrado: {file_path}")
    return {}

def write_json(data: Any, output_path: Path):
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    except IOError as e: print_error(f"Error IO escribiendo JSON: {output_path}. Error: {e}")
    except Exception as e: print_error(f"Error inesperado escribiendo JSON {output_path}: {e}")

def clean_nan_values(data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if data is None: return None
    cleaned_data = {}
    for key, value in data.items():
        if isinstance(value, dict):
            cleaned_data[key] = clean_nan_values(value)
        elif isinstance(value, list):
            cleaned_list = []
            for item_in_list in value:
                if isinstance(item_in_list, dict):
                    cleaned_list.append(clean_nan_values(item_in_list))
                elif pd.isna(item_in_list) and isinstance(item_in_list, float):
                    cleaned_list.append(None)
                else:
                    cleaned_list.append(item_in_list)
            cleaned_data[key] = cleaned_list
        elif pd.isna(value) and isinstance(value, float):
            cleaned_data[key] = None
        else:
            cleaned_data[key] = value
    return cleaned_data

def main():
    parser = argparse.ArgumentParser(description=f"Prepara archivos JSON para la UI desde SQLite ({UI_DATA_VERSION}).")
    parser.add_argument("--db-file", default="output/db/waste_data_clean.db", help="Ruta DB SQLite.")
    parser.add_argument("--profiles-input-dir", default=str(PROFILES_DIR_DEFAULT), help=f"Directorio de entrada para perfiles SFA3 (default: {PROFILES_DIR_DEFAULT}).")
    parser.add_argument("--extrapolations-input-dir", default=str(EXTRAPOLATIONS_DIR_DEFAULT), help=f"Directorio de entrada para datos de extrapolación (default: {EXTRAPOLATIONS_DIR_DEFAULT}).")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR_DEFAULT), help=f"Directorio salida JSON UI (default: {OUTPUT_DIR_DEFAULT}).")
    
    args = parser.parse_args()

    DB_FILE = Path(args.db_file)
    PROFILES_DIR = Path(args.profiles_input_dir)
    EXTRAPOLATIONS_DIR = Path(args.extrapolations_input_dir)
    OUTPUT_DIR = Path(args.output_dir)
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    conn = create_connection(DB_FILE)
    if conn is None: sys.exit(1)

    print_info(f"Iniciando preparación de datos para la UI ({UI_DATA_VERSION})...")
    print_info(f"Leyendo perfiles SFA3 de: {PROFILES_DIR}")
    print_info(f"Leyendo extrapolaciones de: {EXTRAPOLATIONS_DIR}")

    print_info("Obteniendo datos índice (países, ciudades, regiones, ingresos, waste_per_capita, gdp_per_capita_usd)...")
    countries_for_index = fetch_all_countries_for_index(conn)
    cities_by_country_for_index_map = fetch_cities_by_country_for_index(conn)
    unique_regions = fetch_unique_values(conn, "region", "countries")
    unique_income_groups = fetch_unique_values(conn, "income_group_wb", "countries")

    if not countries_for_index: print_warning("No se encontraron países. index.json limitado.")

    index_data_structure = {
        "metadata": {
            "description": "Índice de datos de gestión de residuos para la UI.",
            "version": UI_DATA_VERSION,
            "generated_at": pd.Timestamp.now(tz='UTC').isoformat()
        },
        "regions": unique_regions,
        "income_groups": unique_income_groups,
        "countries": []
    }

    country_extrapolations = load_extrapolations(EXTRAPOLATIONS_DIR, "countries")
    city_extrapolations = load_extrapolations(EXTRAPOLATIONS_DIR, "cities")

    for country_summary_data_sqlite in countries_for_index:
        country_summary_data = dict(country_summary_data_sqlite)
        iso3 = country_summary_data.get('country_code_iso3')
        country_name = country_summary_data.get('country_name')
        if not iso3 or not country_name:
             print_warning(f"Registro país inválido omitido índice: {country_summary_data}")
             continue
        
        country_entry_for_index = {
            "country_name": country_name, "iso3": iso3,
            "region": country_summary_data.get('region'),
            "income": country_summary_data.get('income_group_wb'),
            "latitude": country_summary_data.get('latitude'),
            "longitude": country_summary_data.get('longitude'),
            "latitude_geo_status": country_summary_data.get('latitude_geo_status'),
            "longitude_geo_status": country_summary_data.get('longitude_geo_status'),
            "data_quality_score": country_summary_data.get('data_quality_score'),
            "waste_per_capita_kg_day": country_summary_data.get('waste_per_capita_kg_day'),
            "gdp_per_capita_usd": country_summary_data.get('gdp_per_capita_usd'),
            "cities": cities_by_country_for_index_map.get(iso3, [])
        }
        index_data_structure["countries"].append(country_entry_for_index)

    print_info(f"Generando JSON detallados para {len(countries_for_index)} países...")
    country_iterator = track(countries_for_index, description="Procesando países...") if USE_RICH else countries_for_index
    (OUTPUT_DIR / "countries").mkdir(parents=True, exist_ok=True)

    for country_summary_data_sqlite in country_iterator:
        country_summary_data = dict(country_summary_data_sqlite)
        iso3 = country_summary_data.get('country_code_iso3')
        country_db_id = country_summary_data.get('country_db_id') # Necesitamos el ID numérico para buscar mediciones
        if not iso3 or country_db_id is None: continue

        country_details_raw = fetch_entity_details(conn, "countries", "id", country_db_id) # Usar ID numérico
        if country_details_raw:
            country_details = dict(country_details_raw)
            # Usar country_code_iso3 para buscar mediciones de país, ya que así está en la tabla country_measurements
            country_measurements = fetch_measurements_for_entity(conn, "country_measurements", "country_iso3c", iso3)
            country_details["measurements_data"] = [clean_nan_values(m) for m in country_measurements]
            
            ai_profile = load_sfa3_profile(entity_type="country", 
                                           identifier=iso3, # Pasa ISO3 para la convención de nombres
                                           profiles_dir=PROFILES_DIR)
            country_details["ai_profile_data"] = ai_profile if ai_profile else {}

            extrapolated_data_for_country = country_extrapolations.get(iso3, {})
            if extrapolated_data_for_country:
                print_info(f"Aplicando extrapolaciones para país: {iso3}")
                for key, val_dict in extrapolated_data_for_country.items():
                    if key in country_details and pd.isna(country_details[key]):
                        country_details[key] = val_dict.get("value")
                        country_details[f"{key}_status"] = val_dict.get("status")
                        print_info(f"  Extrapolado {key} para {iso3} con valor {val_dict.get('value')}")

            cleaned_details = clean_nan_values(country_details)
            filename = f"{iso3.upper()}_country.json" # Guardar con ISO3 mayúsculas
            output_path = OUTPUT_DIR / "countries" / filename
            write_json(cleaned_details, output_path)
        else:
             print_warning(f"No se encontraron detalles para país ISO3: {iso3}")

    print_info("Generando JSON detallados para ciudades...")
    all_cities_summary_for_files = []
    try:
        cursor = conn.cursor()
        # Asegúrate de que la tabla 'cities' tiene 'iso3c'
        cursor.execute("SELECT id, municipality, iso3c FROM cities WHERE municipality IS NOT NULL AND iso3c IS NOT NULL")
        all_cities_summary_for_files = [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        print_error(f"Error obteniendo lista ciudades para detalles: {e}")

    city_iterator = track(all_cities_summary_for_files, description="Procesando ciudades...") if USE_RICH else all_cities_summary_for_files
    processed_cities_count = 0
    (OUTPUT_DIR / "cities").mkdir(parents=True, exist_ok=True)

    for city_summary in city_iterator:
        city_id = city_summary['id']
        city_name = city_summary['municipality']
        city_iso3 = city_summary['iso3c'] # ISO3 del país al que pertenece la ciudad

        city_details_raw = fetch_entity_details(conn, "cities", "id", city_id)
        if city_details_raw:
            city_details = dict(city_details_raw)
            city_measurements = fetch_measurements_for_entity(conn, "city_measurements", "city_id", city_id)
            city_details["measurements_data"] = [clean_nan_values(m) for m in city_measurements]

            ai_profile_city = load_sfa3_profile(entity_type="city", 
                                                identifier=str(city_id), # Pasa el ID de la ciudad como string
                                                profiles_dir=PROFILES_DIR)
            city_details["ai_profile_data"] = ai_profile_city if ai_profile_city else {}

            extrapolated_data_for_city = city_extrapolations.get(str(city_id), {})
            if extrapolated_data_for_city:
                print_info(f"Aplicando extrapolaciones para ciudad ID: {city_id} ({city_name})")
                for key, val_dict in extrapolated_data_for_city.items():
                    if key in city_details and pd.isna(city_details[key]):
                        city_details[key] = val_dict.get("value")
                        city_details[f"{key}_status"] = val_dict.get("status")
                        print_info(f"  Extrapolado {key} para ciudad {city_id} con valor {val_dict.get('value')}")

            cleaned_details = clean_nan_values(city_details)
            safe_city_name_for_file = sanitize_filename(city_name)
            # Usar el ID de la ciudad para el nombre del archivo para asegurar unicidad y consistencia
            filename = f"city_{city_id}_{safe_city_name_for_file}_{city_iso3.lower()}.json" 
            output_path = OUTPUT_DIR / "cities" / filename
            write_json(cleaned_details, output_path)
            processed_cities_count += 1
        else:
            print_warning(f"No se encontraron detalles para ciudad ID: {city_id} ({city_name})")

    print_info(f"Generados JSON para {processed_cities_count} ciudades.")

    print_info("Escribiendo archivo index.json...")
    index_output_path = OUTPUT_DIR / "index.json"
    cleaned_index_data = clean_nan_values(index_data_structure)
    write_json(cleaned_index_data, index_output_path)

    conn.close()
    print_success(f"Preparación de datos para UI ({UI_DATA_VERSION}) completada. Archivos en: {OUTPUT_DIR.resolve()}")

if __name__ == "__main__":
    main()
