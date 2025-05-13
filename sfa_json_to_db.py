# /// script
# dependencies = [
#   "rich>=13.7.0", # For visual feedback
# ]
# ///

"""
Single File Agent (SFA): JSON to SQLite Database Loader (v3.6 - UNIQUE constraint en city_measurements)

Lee los archivos JSON procesados (ciudades, países y codebooks)
y los inserta en tablas separadas dentro de una base de datos SQLite.
- CORRECCIÓN v2: Usa 'iso3c' como clave para el código de país en la tabla 'cities'.
- NUEVO v3: Añade soporte para cargar datos de codebooks de países y ciudades
           en nuevas tablas 'country_measurements' y 'city_measurements'.
- CORRECCIÓN v3.1: Importa 'Tuple' de 'typing' para la anotación de tipo en fetch_city_id_map.
- NUEVO v3.2: Añade impresiones de diagnóstico para verificar el estado de las tablas principales.
- NUEVO v3.3: Añade diagnóstico detallado dentro de fetch_city_id_map.
- NUEVO v3.4: Añade las columnas 'data_quality_score' y 'score_details_json'
              a las tablas 'countries' y 'cities'.
- NUEVO v3.5: Añade restricción UNIQUE(municipality, iso3c) a la tabla 'cities'.
- NUEVO v3.6: Añade restricción UNIQUE(city_id, measurement, year, source) a 'city_measurements'.
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Set # Añadido Set

# --- Dependencias y Configuración Visual ---
try:
    from rich.console import Console
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
# --- Fin Dependencias ---

def create_connection(db_file: Path) -> Optional[sqlite3.Connection]:
    """ Crea una conexión a la base de datos SQLite especificada por db_file """
    conn = None
    try:
        print_info(f"Conectando a la base de datos: {db_file}")
        db_file.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        print_success("Conexión establecida y claves foráneas habilitadas.")
        return conn
    except sqlite3.Error as e:
        print_error(f"Error al conectar a la base de datos: {e}")
    return None

def create_table(conn: sqlite3.Connection, create_table_sql: str):
    """ Crea una tabla usando la sentencia CREATE TABLE SQL proporcionada """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
        conn.commit()
    except sqlite3.Error as e:
        print_error(f"Error al crear tabla con SQL:\n{create_table_sql}\nError: {e}")

def insert_data(conn: sqlite3.Connection, table_name: str, data: List[Dict[str, Any]], column_names: List[str]):
    """ Inserta datos de una lista de diccionarios en la tabla especificada """
    if not data:
        print_warning(f"No hay datos para insertar en la tabla '{table_name}'.")
        return 0

    placeholders = ', '.join('?' * len(column_names))
    sql_column_names = ', '.join(f'"{col}"' for col in column_names) # Asegurar comillas

    # Usar INSERT OR IGNORE para manejar posibles violaciones de restricciones UNIQUE
    sql = f''' INSERT OR IGNORE INTO {table_name}({sql_column_names})
              VALUES({placeholders}) '''

    inserted_count = 0
    skipped_count = 0
    c = conn.cursor()
    print_info(f"Iniciando inserción en tabla '{table_name}'...")
    for record in data:
        # Crear tupla de valores en el orden correcto
        values_tuple = []
        for col_name in column_names:
            # Obtener valor, usar None si la clave no existe en el dict
            values_tuple.append(record.get(col_name))
        values = tuple(values_tuple)

        try:
            c.execute(sql, values)
            if c.rowcount > 0:
                inserted_count += 1
            else:
                # Si rowcount es 0 con INSERT OR IGNORE, significa que se violó una restricción UNIQUE (duplicado)
                skipped_count +=1
        except sqlite3.IntegrityError as e:
            # Aunque INSERT OR IGNORE debería manejar UNIQUE, otros errores de integridad podrían ocurrir
            print_warning(f"Error de integridad (no UNIQUE) insertando registro en '{table_name}': {e}. Registro: {record}")
            skipped_count += 1
        except sqlite3.Error as e:
            print_warning(f"Error general de DB insertando registro en '{table_name}': {e}. Registro: {record}")
            skipped_count += 1
        except Exception as e:
             print_error(f"Error inesperado durante inserción en '{table_name}': {e}. Registro: {record}")
             skipped_count += 1

    conn.commit()
    print_success(f"Inserción en tabla '{table_name}' completada.")
    print_info(f"Registros insertados: {inserted_count}")
    if skipped_count > 0:
        print_warning(f"Registros omitidos (probablemente duplicados por restricción UNIQUE): {skipped_count}")
    return inserted_count

def fetch_city_id_map(conn: sqlite3.Connection) -> Dict[Tuple[str, str], int]:
    """
    Crea un mapa de (nombre_ciudad_normalizado, iso3c_pais) -> city_id
    para una búsqueda eficiente de city_id.
    """
    print_info("Iniciando fetch_city_id_map...")
    city_map = {}
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM cities")
        total_cities_before_filter = cursor.fetchone()[0]
        print_info(f"DIAGNÓSTICO (fetch_city_id_map): Total de filas en 'cities' ANTES del filtro WHERE: {total_cities_before_filter}")

        query = "SELECT id, municipality, iso3c FROM cities WHERE municipality IS NOT NULL AND iso3c IS NOT NULL"
        print_info(f"DIAGNÓSTICO (fetch_city_id_map): Ejecutando consulta: {query}")
        cursor.execute(query)
        rows = cursor.fetchall()
        print_info(f"DIAGNÓSTICO (fetch_city_id_map): Número de filas DEVUELTAS por la consulta (DESPUÉS del filtro WHERE): {len(rows)}")

        if not rows and total_cities_before_filter > 0:
            print_warning(f"DIAGNÓSTICO (fetch_city_id_map): La consulta no devolvió filas, pero la tabla 'cities' tiene {total_cities_before_filter} filas. ¿Todas tienen municipality o iso3c NULL?")
            # ... (código de diagnóstico opcional) ...

        processed_count = 0
        for row in rows:
            # ... (código de diagnóstico opcional) ...

            city_name_raw = row['municipality']
            country_iso_raw = row['iso3c']

            if city_name_raw is None or country_iso_raw is None:
                # ... (código de diagnóstico opcional) ...
                continue

            # Normalización simple para la clave del mapa
            city_name_normalized = str(city_name_raw).lower().strip()
            country_iso_normalized = str(country_iso_raw).lower().strip()

            city_map[(city_name_normalized, country_iso_normalized)] = row['id']
            processed_count += 1
        print_success(f"Mapa de {len(city_map)} IDs de ciudades creado a partir de {processed_count} filas procesadas.")
        # ... (código de diagnóstico opcional) ...

    except sqlite3.Error as e:
        print_error(f"Error al crear el mapa de IDs de ciudades: {e}. ¿Está la tabla 'cities' poblada y con las columnas correctas?")
    except Exception as ex:
        print_error(f"Error inesperado en fetch_city_id_map: {ex}")
    return city_map

def main():
    parser = argparse.ArgumentParser(description="Carga datos JSON procesados a una base de datos SQLite.")
    parser.add_argument("--city-json", help="Ruta al archivo JSON procesado de ciudades.")
    parser.add_argument("--country-json", help="Ruta al archivo JSON procesado de países.")
    parser.add_argument("--country-codebook-json", help="Ruta al archivo JSON procesado del codebook de países.")
    parser.add_argument("--city-codebook-json", help="Ruta al archivo JSON procesado del codebook de ciudades.")
    parser.add_argument("--db-file", default="output/waste_data.db", help="Ruta al archivo de la base de datos SQLite.")
    parser.add_argument("--drop-tables", action="store_true", help="Eliminar tablas existentes. ¡PRECAUCIÓN!")
    # NUEVO: Argumento para indicar la fuente/metodología de los archivos cargados
    parser.add_argument("--source-methodology", help="Identificador de la metodología/fuente (ej. 'WB', 'WFD') para los datos de CIUDAD que se están cargando.")


    args = parser.parse_args()
    db_path = Path(args.db_file)

    # --- Definiciones SQL de las Tablas ---
    # (Incluye la nueva columna data_source_methodology en cities)
    sql_create_cities_table = """ CREATE TABLE IF NOT EXISTS cities (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT, municipality TEXT, iso3c TEXT, country TEXT,
                                        population REAL, total_waste_tons_year REAL, income_level TEXT,
                                        recycling_rate_percent REAL, collection_coverage_population_percent REAL,
                                        primary_collection_mode TEXT, composition_food_organic REAL, composition_glass REAL,
                                        composition_metal REAL, composition_paper_cardboard REAL, composition_plastic REAL,
                                        composition_rubber_leather REAL, composition_wood REAL,
                                        composition_yard_garden_green REAL, composition_other REAL,
                                        composition_calculation_status TEXT, municipality_status TEXT, country_status TEXT,
                                        iso3c_status TEXT, population_status TEXT, total_waste_tons_year_status TEXT,
                                        income_level_status TEXT, recycling_rate_percent_status TEXT,
                                        collection_coverage_population_percent_status TEXT,
                                        primary_collection_mode_status TEXT, composition_food_organic_status TEXT,
                                        composition_glass_status TEXT, composition_metal_status TEXT,
                                        composition_paper_cardboard_status TEXT, composition_plastic_status TEXT,
                                        composition_rubber_leather_status TEXT, composition_wood_status TEXT,
                                        composition_yard_garden_green_status TEXT, composition_other_status TEXT,
                                        latitude REAL, longitude REAL, latitude_geo_status TEXT, longitude_geo_status TEXT,
                                        data_quality_score REAL, score_details_json TEXT,
                                        -- Campos específicos WFD (pueden ser NULL para otras fuentes)
                                        original_wfd_id REAL, waste_gen_rate_kg_cap_day REAL,
                                        collection_pct_formal REAL, collection_pct_informal REAL,
                                        treatment_pct_recycling_composting REAL, treatment_pct_thermal REAL,
                                        treatment_pct_landfill REAL, composition_sum_pct REAL, composition_sum_flag TEXT,
                                        -- Columna para identificar la fuente principal
                                        data_source_methodology TEXT,
                                        -- Restricciones
                                        UNIQUE (municipality, iso3c)
                                    ); """
    # (countries y country_measurements sin cambios respecto a v3.5)
    sql_create_countries_table = """ CREATE TABLE IF NOT EXISTS countries (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT, country_code_iso3 TEXT UNIQUE,
                                        country_name TEXT, region TEXT, income_group_wb TEXT, gdp_per_capita_usd REAL,
                                        population_total REAL, total_msw_generated_tons_year REAL,
                                        waste_collection_coverage_total_percent_of_population REAL,
                                        waste_treatment_recycling_percent REAL, waste_treatment_compost_percent REAL,
                                        waste_treatment_incineration_percent REAL,
                                        waste_treatment_landfill_unspecified_percent REAL,
                                        waste_treatment_open_dump_percent REAL,
                                        waste_treatment_sanitary_landfill_percent REAL,
                                        waste_treatment_controlled_landfill_percent REAL, national_law_exists TEXT,
                                        national_agency_exists TEXT, e_waste_tons_year REAL,
                                        hazardous_waste_tons_year REAL, composition_food_organic REAL,
                                        composition_glass REAL, composition_metal REAL, composition_paper_cardboard REAL,
                                        composition_plastic REAL, composition_rubber_leather REAL, composition_wood REAL,
                                        composition_yard_garden_green REAL, composition_other REAL,
                                        country_name_status TEXT, country_code_iso3_status TEXT, region_status TEXT,
                                        income_group_wb_status TEXT, gdp_per_capita_usd_status TEXT,
                                        population_total_status TEXT, total_msw_generated_tons_year_status TEXT,
                                        waste_collection_coverage_total_percent_of_population_status TEXT,
                                        waste_treatment_recycling_percent_status TEXT,
                                        waste_treatment_compost_percent_status TEXT,
                                        waste_treatment_incineration_percent_status TEXT,
                                        waste_treatment_landfill_unspecified_percent_status TEXT,
                                        waste_treatment_open_dump_percent_status TEXT,
                                        waste_treatment_sanitary_landfill_percent_status TEXT,
                                        waste_treatment_controlled_landfill_percent_status TEXT,
                                        national_law_exists_status TEXT, national_agency_exists_status TEXT,
                                        e_waste_tons_year_status TEXT, hazardous_waste_tons_year_status TEXT,
                                        composition_food_organic_status TEXT, composition_glass_status TEXT,
                                        composition_metal_status TEXT, composition_paper_cardboard_status TEXT,
                                        composition_plastic_status TEXT, composition_rubber_leather_status TEXT,
                                        composition_wood_status TEXT, composition_yard_garden_green_status TEXT,
                                        composition_other_status TEXT, latitude REAL, longitude REAL,
                                        latitude_geo_status TEXT, longitude_geo_status TEXT,
                                        data_quality_score REAL, score_details_json TEXT
                                    ); """
    sql_create_country_measurements_table = """
    CREATE TABLE IF NOT EXISTS country_measurements (
      id INTEGER PRIMARY KEY AUTOINCREMENT, country_iso3c TEXT NOT NULL, measurement TEXT NOT NULL,
      units TEXT, year INTEGER, source TEXT, comments TEXT,
      FOREIGN KEY (country_iso3c) REFERENCES countries (country_code_iso3) ON DELETE CASCADE ON UPDATE CASCADE
    );"""
    sql_create_idx_country_measurements = "CREATE INDEX IF NOT EXISTS idx_country_measurements_iso3c_measurement ON country_measurements (country_iso3c, measurement);"

    # --- MODIFICACIÓN: Añadir UNIQUE constraint a city_measurements ---
    sql_create_city_measurements_table = """
    CREATE TABLE IF NOT EXISTS city_measurements (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      city_id INTEGER NOT NULL,
      measurement TEXT NOT NULL,
      units TEXT,
      year INTEGER,
      source TEXT,
      comments TEXT,
      FOREIGN KEY (city_id) REFERENCES cities (id) ON DELETE CASCADE ON UPDATE CASCADE,
      -- Añadir restricción UNIQUE para evitar duplicados exactos
      UNIQUE (city_id, measurement, year, source)
    );"""
    # --- FIN MODIFICACIÓN ---
    sql_create_idx_city_measurements = "CREATE INDEX IF NOT EXISTS idx_city_measurements_city_id_measurement ON city_measurements (city_id, measurement);"

    conn = create_connection(db_path)

    if conn is not None:
        if args.drop_tables:
            print_warning("Opción --drop-tables activada. Eliminando tablas existentes...")
            cursor = conn.cursor()
            # Asegurarse de eliminar en orden inverso a las dependencias FK si es necesario
            tables_to_drop = ["city_measurements", "country_measurements", "cities", "countries"]
            for table in tables_to_drop:
                print_warning(f"Eliminando tabla '{table}'...")
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
            conn.commit()
            print_success("Tablas eliminadas.")

        print_info("Creando/Verificando tablas...")
        create_table(conn, sql_create_countries_table)
        create_table(conn, sql_create_cities_table) # Incluye nueva columna y UNIQUE
        create_table(conn, sql_create_country_measurements_table)
        create_table(conn, sql_create_idx_country_measurements)
        create_table(conn, sql_create_city_measurements_table) # Incluye nuevo UNIQUE
        create_table(conn, sql_create_idx_city_measurements)
        print_success("Tablas verificadas/creadas.")

        # --- Carga de Países (sin cambios significativos) ---
        countries_loaded_count = 0
        if args.country_json:
            # ... (código de carga de países existente) ...
            country_json_path = Path(args.country_json)
            if country_json_path.is_file():
                try:
                    print_info(f"Leyendo datos de países desde: {country_json_path}")
                    with open(country_json_path, 'r', encoding='utf-8') as f:
                        country_data = json.load(f)
                    print_success(f"Leídos {len(country_data)} registros de países del JSON.")

                    if country_data:
                        # Obtener columnas de la DB (excluir las autogeneradas o manejadas por otros SFAs)
                        db_country_cols_info = conn.execute("PRAGMA table_info(countries)").fetchall()
                        db_country_cols = {col['name'] for col in db_country_cols_info
                                           if col['name'] not in ['id', 'data_quality_score', 'score_details_json']} # Usar set para búsqueda rápida
                        
                        # Determinar columnas a insertar (las que están en el JSON y en la DB)
                        first_record_keys = set(country_data[0].keys())
                        country_columns_to_insert = sorted(list(first_record_keys.intersection(db_country_cols)))

                        # Advertir si hay claves en JSON que no están en la tabla DB (excepto las excluidas)
                        missing_cols_in_db = first_record_keys - db_country_cols - {'id', 'data_quality_score', 'score_details_json'}
                        if missing_cols_in_db:
                             print_warning(f"JSON de países tiene claves no mapeadas a la tabla 'countries': {missing_cols_in_db}. Se ignorarán.")

                        countries_loaded_count = insert_data(conn, "countries", country_data, country_columns_to_insert)
                    else:
                        print_warning(f"El archivo JSON de países '{country_json_path}' está vacío.")
                except json.JSONDecodeError:
                    print_error(f"Error decodificando JSON en: {country_json_path}.")
                except Exception as e:
                    print_error(f"Error procesando archivo de países '{country_json_path}': {e}")
            else:
                print_warning(f"Archivo JSON de países no encontrado: {country_json_path}")


        # --- Carga de Ciudades (modificado para añadir source_methodology) ---
        cities_loaded_count = 0
        if args.city_json:
            city_json_path = Path(args.city_json)
            if city_json_path.is_file():
                try:
                    print_info(f"Leyendo datos de ciudades desde: {city_json_path}")
                    with open(city_json_path, 'r', encoding='utf-8') as f:
                        city_data_raw = json.load(f)
                    print_success(f"Leídos {len(city_data_raw)} registros de ciudades del JSON.")

                    if city_data_raw:
                        # Añadir la metodología a cada registro si se proporcionó
                        city_data_processed = []
                        if args.source_methodology:
                            print_info(f"Añadiendo 'data_source_methodology' = '{args.source_methodology}' a los datos de ciudad.")
                            for record in city_data_raw:
                                record['data_source_methodology'] = args.source_methodology
                                city_data_processed.append(record)
                        else:
                            print_warning("No se especificó --source-methodology. La columna 'data_source_methodology' quedará NULL para estos registros.")
                            city_data_processed = city_data_raw # Usar datos como están

                        # Obtener columnas de la DB (excluir las autogeneradas o manejadas por otros SFAs)
                        db_city_cols_info = conn.execute("PRAGMA table_info(cities)").fetchall()
                        db_city_cols = {col['name'] for col in db_city_cols_info
                                        if col['name'] not in ['id', 'data_quality_score', 'score_details_json']} # Usar set

                        # Determinar columnas a insertar
                        first_record_keys = set(city_data_processed[0].keys())
                        city_columns_to_insert = sorted(list(first_record_keys.intersection(db_city_cols)))

                         # Asegurarse de que 'data_source_methodology' esté en la lista si existe en la DB
                        if 'data_source_methodology' in db_city_cols and 'data_source_methodology' not in city_columns_to_insert:
                             city_columns_to_insert.append('data_source_methodology')
                             city_columns_to_insert.sort()


                        # Advertir si hay claves en JSON que no están en la tabla DB
                        missing_cols_in_db = first_record_keys - db_city_cols - {'id', 'data_quality_score', 'score_details_json'}
                        if missing_cols_in_db:
                             print_warning(f"JSON de ciudades tiene claves no mapeadas a la tabla 'cities': {missing_cols_in_db}. Se ignorarán.")

                        cities_loaded_count = insert_data(conn, "cities", city_data_processed, city_columns_to_insert)
                    else:
                        print_warning(f"El archivo JSON de ciudades '{city_json_path}' está vacío.")
                except json.JSONDecodeError:
                    print_error(f"Error decodificando JSON en: {city_json_path}.")
                except Exception as e:
                    print_error(f"Error procesando archivo de ciudades '{city_json_path}': {e}")
            else:
                print_warning(f"Archivo JSON de ciudades no encontrado: {city_json_path}")


        # --- Carga de Codebooks (sin cambios significativos, pero se beneficiará del UNIQUE en city_measurements) ---
        # Carga Country Codebook
        if args.country_codebook_json:
            # ... (código de carga de country_measurements existente) ...
             country_codebook_json_path = Path(args.country_codebook_json)
             if country_codebook_json_path.is_file():
                 try:
                     print_info(f"Leyendo datos del codebook de países desde: {country_codebook_json_path}")
                     with open(country_codebook_json_path, 'r', encoding='utf-8') as f:
                         country_codebook_data = json.load(f)
                     print_success(f"Leídos {len(country_codebook_data)} registros del codebook de países JSON.")
                     if country_codebook_data:
                         # Definir explícitamente las columnas esperadas del JSON y que coinciden con la tabla
                         country_measurement_columns_to_insert = ["country_iso3c", "measurement", "units", "year", "source", "comments"]
                         # Validar que el primer registro tenga al menos estas columnas (o las que existan)
                         if country_codebook_data and not all(col in country_codebook_data[0] for col in country_measurement_columns_to_insert if col in country_codebook_data[0]):
                             print_warning(f"Columnas esperadas vs encontradas en codebook país JSON: Esperadas (subconjunto): {country_measurement_columns_to_insert}. Encontradas: {list(country_codebook_data[0].keys()) if country_codebook_data else 'N/A'}")
                         # Usar solo las columnas definidas para la inserción
                         insert_data(conn, "country_measurements", country_codebook_data, country_measurement_columns_to_insert)
                 except json.JSONDecodeError:
                      print_error(f"Error decodificando JSON en: {country_codebook_json_path}.")
                 except Exception as e:
                     print_error(f"Error procesando archivo de codebook de países '{country_codebook_json_path}': {e}")
             else:
                 print_warning(f"Archivo JSON de codebook de países no encontrado: {country_codebook_json_path}")

        # Carga City Codebook
        if args.city_codebook_json:
            # ... (código de carga de city_measurements existente, ahora usará INSERT OR IGNORE con UNIQUE) ...
            city_codebook_json_path = Path(args.city_codebook_json)
            if city_codebook_json_path.is_file():
                city_id_lookup = fetch_city_id_map(conn) # Necesario para obtener city_id
                if not city_id_lookup:
                     print_warning("El mapa de IDs de ciudades está vacío. La carga de 'city_measurements' fallará las FK o no encontrará IDs.")

                try:
                    print_info(f"Leyendo datos del codebook de ciudades desde: {city_codebook_json_path}")
                    with open(city_codebook_json_path, 'r', encoding='utf-8') as f:
                        city_codebook_data_raw = json.load(f)
                    print_success(f"Leídos {len(city_codebook_data_raw)} registros del codebook de ciudades JSON.")

                    if city_codebook_data_raw:
                        city_measurements_to_insert = []
                        processed_keys_for_city_id = set() # Para evitar buscar ID repetidamente si falla
                        skipped_city_measurements_no_id = 0

                        for record_raw in city_codebook_data_raw:
                            # Asumir que el JSON del codebook tiene claves para identificar la ciudad
                            city_name_cb = record_raw.get("city_name_codebook") # Ajusta estas claves si son diferentes en tu JSON
                            country_iso_cb = record_raw.get("country_iso3c_codebook") # Ajusta estas claves si son diferentes en tu JSON

                            if not city_name_cb or not country_iso_cb:
                                skipped_city_measurements_no_id += 1
                                continue # No se puede buscar city_id

                            # Normalizar para buscar en el mapa
                            city_name_norm = str(city_name_cb).lower().strip()
                            country_iso_norm = str(country_iso_cb).lower().strip()
                            lookup_key = (city_name_norm, country_iso_norm)

                            city_id = city_id_lookup.get(lookup_key)

                            if city_id is not None:
                                # Crear el registro para insertar en city_measurements
                                measurement_record = {
                                    "city_id": city_id,
                                    "measurement": record_raw.get("measurement"),
                                    "units": record_raw.get("units"),
                                    "year": record_raw.get("year"),
                                    "source": record_raw.get("source"),
                                    "comments": record_raw.get("comments")
                                }
                                city_measurements_to_insert.append(measurement_record)
                            else:
                                if lookup_key not in processed_keys_for_city_id: # Solo advertir una vez por ciudad no encontrada
                                     print_warning(f"  DIAGNÓSTICO (city_codebook_load): No se encontró ID para ciudad '{city_name_cb}' (País: {country_iso_cb}). Clave de búsqueda: {lookup_key}")
                                     processed_keys_for_city_id.add(lookup_key)
                                skipped_city_measurements_no_id += 1

                        if skipped_city_measurements_no_id > 0:
                             print_warning(f"Total de {skipped_city_measurements_no_id} registros de codebook de ciudades omitidos (falta nombre/ISO en JSON o ID de ciudad no encontrado en BD).")

                        if city_measurements_to_insert:
                            city_measurement_columns_to_insert = ["city_id", "measurement", "units", "year", "source", "comments"]
                            # La función insert_data con INSERT OR IGNORE manejará los duplicados gracias al UNIQUE constraint
                            insert_data(conn, "city_measurements", city_measurements_to_insert, city_measurement_columns_to_insert)
                    else:
                        print_warning(f"El archivo JSON de codebook de ciudades '{city_codebook_json_path}' está vacío.")

                except json.JSONDecodeError:
                    print_error(f"Error decodificando JSON en: {city_codebook_json_path}.")
                except Exception as e:
                    print_error(f"Error procesando archivo de codebook de ciudades '{city_codebook_json_path}': {e}")
            else:
                print_warning(f"Archivo JSON de codebook de ciudades no encontrado: {city_codebook_json_path}")


        conn.close()
        print_info(f"Conexión a la base de datos '{db_path}' cerrada.")
    else:
        print_error("No se pudo establecer conexión con la base de datos. Abortando.")
        sys.exit(1)

if __name__ == "__main__":
    main()
