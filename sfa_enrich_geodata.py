# /// script
# dependencies = [
#   "rich>=13.7.0",
#   "requests>=2.25.1"
# ]
# ///

"""
SFA: Enrich Geodata (v3 - Improved Country Geocoding)
Este script enriquece la base de datos waste_data.db con datos de geolocalización
(latitud, longitud) para países y ciudades utilizando la API Nominatim de OpenStreetMap.
Incluye mejoras para la geocodificación de países.
"""

import argparse
import sqlite3
import time
import requests
import json
from pathlib import Path
import sys
from typing import Any, Dict 

# --- Configuración Visual (Rich) ---
try:
    from rich.console import Console
    from rich.progress import track
    from rich.table import Table
    console = Console(stderr=True)
    USE_RICH = True
    def print_info(message): console.print(f"[cyan]ℹ️ {message}[/cyan]")
    def print_success(message): console.print(f"[green]✅ {message}[/green]")
    def print_warning(message): console.print(f"[yellow]⚠️ {message}[/yellow]")
    def print_error(message): console.print(f"[bold red]❌ ERROR:[/bold red] {message}")
    def print_status(message): console.print(f"[blue]⚙️ {message}[/blue]")
except ImportError:
    USE_RICH = False
    def print_info(message): print(f"INFO: {message}", file=sys.stderr)
    def print_success(message): print(f"SUCCESS: {message}", file=sys.stderr)
    def print_warning(message): print(f"WARNING: {message}", file=sys.stderr)
    def print_error(message): print(f"ERROR: {message}", file=sys.stderr)
    def print_status(message): print(f"STATUS: {message}", file=sys.stderr)
    def track(sequence, description="Procesando..."):
        yield from sequence
# --- Fin Configuración Visual ---

# --- Constantes ---
DB_FILE_DEFAULT = "waste_data.db"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "WasteDataProject/1.0 (contact@example.com)" # ¡CAMBIAR ESTO!
REQUEST_DELAY_SECONDS = 1.1 

GEO_COLUMNS = {
    "latitude": "REAL",
    "longitude": "REAL",
}
STATUS_SUFFIX = "_geo_status" 

# Mapeo para nombres de países problemáticos o ambiguos
PROBLEM_COUNTRY_NAMES_MAP = {
    "Congo, Dem. Rep.": "Democratic Republic of the Congo",
    "Congo, Rep.": "Republic of the Congo",
    "Egypt, Arab Rep.": "Egypt",
    "Iran, Islamic Rep.": "Iran",
    "Korea, Rep.": "South Korea", # O "Republic of Korea"
    "Macedonia, FYR": "North Macedonia", # Nombre cambiado oficialmente
    "Sint Maarten (Dutch part)": "Sint Maarten", # Es un país constituyente del Reino de los Países Bajos
    "Yemen, Rep.": "Yemen",
    "Micronesia, Fed. Sts.": "Federated States of Micronesia",
    "Venezuela, RB": "Venezuela", # RB probablemente es "República Bolivariana"
    "Bahamas, The": "The Bahamas",
    "Gambia, The": "The Gambia",
    # Nombres que coinciden con lugares en otros países (ej. estados de EE.UU.)
    # Añadir ", country" en la consulta es una estrategia general para estos.
    # "Georgia": "Georgia country", # Se manejará con is_country_query
    # "Jordan": "Jordan country",
    # "Lebanon": "Lebanon country",
}

def create_connection(db_file: Path) -> sqlite3.Connection | None:
    """ Crea una conexión a la base de datos SQLite. """
    conn = None
    try:
        print_status(f"Conectando a la base de datos: {db_file}")
        if not db_file.exists():
            print_error(f"El archivo de base de datos '{db_file}' no existe.")
            return None
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        print_success("Conexión establecida.")
        return conn
    except sqlite3.Error as e:
        print_error(f"Error al conectar a la base de datos: {e}")
    return conn

def add_geo_columns_if_not_exist(conn: sqlite3.Connection, table_name: str):
    """Añade columnas de geodatos y sus estados a la tabla si no existen."""
    print_status(f"Verificando/Añadiendo columnas geográficas en tabla '{table_name}'...")
    cursor = conn.cursor()
    try:
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing_columns = [row['name'] for row in cursor.fetchall()]

        for col_name, col_type in GEO_COLUMNS.items():
            if col_name not in existing_columns:
                print_info(f"Añadiendo columna '{col_name}' ({col_type}) a '{table_name}'...")
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")
            status_col_name = f"{col_name}{STATUS_SUFFIX}"
            if status_col_name not in existing_columns:
                print_info(f"Añadiendo columna '{status_col_name}' (TEXT) a '{table_name}'...")
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {status_col_name} TEXT")
        conn.commit()
        print_success(f"Columnas geográficas verificadas/añadidas en '{table_name}'.")
    except sqlite3.Error as e:
        print_error(f"Error al añadir columnas a '{table_name}': {e}")
        conn.rollback()


def geocode_location_nominatim(query: str, 
                               country_for_city_query: str | None = None, 
                               is_country_query: bool = False) -> Dict[str, Any]:
    """
    Geocodifica una ubicación usando Nominatim.
    - query: El nombre del lugar a buscar.
    - country_for_city_query: Nombre del país para refinar búsquedas de ciudades.
    - is_country_query: Flag para indicar si la consulta es para un país, para aplicar lógicas especiales.
    """
    params = {
        'format': 'jsonv2', # jsonv2 suele dar más detalles como category y type
        'addressdetails': 1,
        'limit': 1 
    }
    
    actual_query_for_nominatim = query
    if is_country_query and country_for_city_query is None:
        # Para consultas de países, añadir ", country" puede ayudar a Nominatim a desambiguar.
        # Evitar añadirlo si ya parece ser parte de un nombre formal o específico.
        query_lower = query.lower()
        if not (", country" in query_lower or "republic" in query_lower or 
                "kingdom" in query_lower or "states" in query_lower or "emirates" in query_lower):
            actual_query_for_nominatim = f"{query}, country"
    elif country_for_city_query: # Es una consulta de ciudad
        actual_query_for_nominatim = f"{query}, {country_for_city_query}"
    
    params['q'] = actual_query_for_nominatim
    headers = {'User-Agent': USER_AGENT}
    print_status(f"Geocodificando: '{params['q']}' (Original: '{query}')")

    try:
        response = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=10)
        response.raise_for_status() 
        data = response.json()

        if data and len(data) > 0:
            location = data[0]
            lat = float(location.get('lat', 0.0))
            lon = float(location.get('lon', 0.0))

            # Verificación adicional para consultas de PAÍSES
            if is_country_query and country_for_city_query is None:
                loc_category = location.get('category', '').lower()
                loc_type = location.get('type', '').lower() # OSM type e.g. administrative, city
                loc_admin_level = str(location.get('admin_level', ''))
                
                # Un país usualmente es una frontera ('boundary') administrativa de nivel 2
                is_osm_country_type = (
                    loc_category == 'boundary' and 
                    loc_type == 'administrative' and 
                    loc_admin_level == '2'
                )
                # Nominatim a veces usa type='country' directamente en el resultado principal
                is_direct_country_type = (loc_type == 'country')


                if not (is_osm_country_type or is_direct_country_type):
                    address_details = location.get('address', {})
                    returned_country_name_from_address = address_details.get('country', '')
                    
                    # Si el tipo no es claramente un país Y el país en los detalles de la dirección
                    # no coincide razonablemente con nuestra consulta original, es sospechoso.
                    # (Ej: buscamos "Georgia, country", obtenemos un pub en "Reino Unido")
                    if returned_country_name_from_address and not query.lower() in returned_country_name_from_address.lower():
                         print_warning(f"Consulta de país '{actual_query_for_nominatim}' resultó en '{location.get('display_name')}' (Tipo OSM: {loc_category}/{loc_type}, Nivel Adm: {loc_admin_level}). El país en la dirección es '{returned_country_name_from_address}', que no coincide con la consulta '{query}'. Marcando como 'error_mismatch'.")
                         return {"status": "error_mismatch_country_type"}

            print_success(f"Encontrado para '{actual_query_for_nominatim}': {location.get('display_name')} ({lat}, {lon})")
            return {
                "latitude": lat,
                "longitude": lon,
                "status": "found",
                "geocoded_query": actual_query_for_nominatim,
                "returned_display_name": location.get('display_name'), # CORREGIDO AQUÍ
                "returned_osm_category": location.get('category'),
                "returned_osm_type": location.get('type')
            }
        else:
            print_warning(f"No se encontraron resultados para: '{params['q']}'")
            return {"status": "not_found", "geocoded_query": actual_query_for_nominatim}
    except requests.exceptions.RequestException as e:
        print_error(f"Error de red/HTTP geocodificando '{params['q']}': {e}")
        return {"status": "error_request", "geocoded_query": actual_query_for_nominatim}
    except json.JSONDecodeError:
        print_error(f"Error decodificando JSON de Nominatim para '{params['q']}'")
        return {"status": "error_json", "geocoded_query": actual_query_for_nominatim}
    except Exception as e:
        print_error(f"Error inesperado geocodificando '{params['q']}': {e}")
        return {"status": "error_unknown", "geocoded_query": actual_query_for_nominatim}
    finally:
        time.sleep(REQUEST_DELAY_SECONDS) 


def update_record_geodata(conn: sqlite3.Connection, table_name: str, primary_key_name: str, primary_key_value: Any, geo_results: Dict[str, Any]):
    """Actualiza un registro en la BD con los resultados de geocodificación."""
    cursor = conn.cursor()
    
    update_fields = []
    update_values = []

    for col_name in GEO_COLUMNS.keys(): # latitude, longitude
        update_fields.append(f"{col_name} = ?")
        update_fields.append(f"{col_name}{STATUS_SUFFIX} = ?")
        
        if geo_results['status'] == 'found':
            update_values.append(geo_results.get(col_name))
            update_values.append(geo_results['status'])
        else: 
            update_values.append(None) 
            update_values.append(geo_results['status']) 
            
    update_values.append(primary_key_value) 

    sql = f"""UPDATE {table_name}
              SET {', '.join(update_fields)}
              WHERE {primary_key_name} = ?"""
    try:
        cursor.execute(sql, tuple(update_values))
        conn.commit()
        if cursor.rowcount > 0:
            print_info(f"Registro {primary_key_value} en '{table_name}' actualizado con geodatos ({geo_results['status']}). Query: '{geo_results.get('geocoded_query','N/A')}'.")
        else:
            # Esto puede pasar si el WHERE no coincide o si los valores son los mismos (improbable con status)
            print_warning(f"No se actualizó el registro {primary_key_value} en '{table_name}' (quizás no existe o no se requería cambio). Estado: {geo_results['status']}.")
    except sqlite3.Error as e:
        print_error(f"Error al actualizar registro {primary_key_value} en '{table_name}': {e}")
        conn.rollback()

def process_countries(conn: sqlite3.Connection):
    """Obtiene geodatos para países que aún no los tienen o tuvieron errores."""
    print_info("\n--- Procesando Países ---")
    cursor = conn.cursor()
    status_check_col = f"latitude{STATUS_SUFFIX}"
    # Procesar si no hay estado, o si el estado indica un error previo o no encontrado
    # 'not_found_permanent' podría ser un estado para evitar reintentos si se confirma que no existe.
    # 'error_mismatch_country_type' es el nuevo estado para cuando el tipo no es país.
    # 'error_invalid_name' para nombres como 'nan'.
    cursor.execute(f"""
        SELECT country_code_iso3, country_name
        FROM countries
        WHERE {status_check_col} IS NULL OR 
              {status_check_col} IN ('not_found', 'error_request', 'error_json', 'error_unknown', 'error_mismatch_country_type')
    """) 
    
    countries_to_process = cursor.fetchall()
    if not countries_to_process:
        print_info("No hay países que necesiten nueva geocodificación o reintento.")
        return

    print_info(f"Encontrados {len(countries_to_process)} países para geocodificar/reintentar.")
    
    processed_countries_summary = []
    for country_row in track(countries_to_process, description="Geocodificando países..."):
        iso3 = country_row['country_code_iso3']
        original_country_name = country_row['country_name']
        
        # Manejar nombres inválidos como 'nan'
        if not original_country_name or str(original_country_name).strip().lower() == 'nan':
            print_warning(f"País con ISO3 '{iso3}' tiene nombre inválido: '{original_country_name}'. Marcando como 'error_invalid_name'.")
            update_record_geodata(conn, "countries", "country_code_iso3", iso3, {"status": "error_invalid_name"})
            processed_countries_summary.append({
                "iso3": iso3, "name": str(original_country_name), "latitude": None, "longitude": None, 
                "status": "error_invalid_name", "geocoded_query": "N/A", 
                "returned_display_name": "N/A", "returned_osm_category": "N/A", "returned_osm_type": "N/A"
            })
            continue
        
        # Usar nombre del mapeo si existe, si no, el original
        country_name_for_query = PROBLEM_COUNTRY_NAMES_MAP.get(original_country_name, original_country_name)
        
        geo_results = geocode_location_nominatim(country_name_for_query, is_country_query=True)
        
        # Si el primer intento (mapeado o directo) falla con not_found y el nombre fue mapeado,
        # intentar con el nombre original (por si el mapeo fue el problema o para aplicar ", country" al original)
        if geo_results["status"] == "not_found" and country_name_for_query != original_country_name:
            print_info(f"Intento con nombre '{country_name_for_query}' falló. Reintentando con original '{original_country_name}' (con posible sufijo ', country')...")
            time.sleep(REQUEST_DELAY_SECONDS) # Respetar delay
            geo_results = geocode_location_nominatim(original_country_name, is_country_query=True)

        update_record_geodata(conn, "countries", "country_code_iso3", iso3, geo_results)
        processed_countries_summary.append({
            "iso3": iso3,
            "name": original_country_name, # Mostrar nombre original en resumen
            "latitude": geo_results.get("latitude"),
            "longitude": geo_results.get("longitude"),
            "status": geo_results["status"],
            "geocoded_query": geo_results.get("geocoded_query"),
            "returned_display_name": geo_results.get("returned_display_name"),
            "returned_osm_category": geo_results.get("returned_osm_category"),
            "returned_osm_type": geo_results.get("returned_osm_type")
        })
    
    if USE_RICH and processed_countries_summary:
        table = Table(title="Resumen Geocodificación Países (Resultados de esta ejecución)")
        table.add_column("ISO3", style="dim")
        table.add_column("Nombre DB")
        table.add_column("Query Enviada")
        table.add_column("Lat", justify="right")
        table.add_column("Lon", justify="right")
        table.add_column("Estado Geo", justify="right")
        table.add_column("Respuesta Nominatim", overflow="fold")
        table.add_column("Tipo OSM", overflow="fold")

        for pc in processed_countries_summary:
            status_style = "green" if pc["status"] == "found" else "red" if "error" in pc["status"] else "yellow"
            lat_str = f"{pc['latitude']:.4f}" if pc.get("latitude") is not None else "N/A"
            lon_str = f"{pc['longitude']:.4f}" if pc.get("longitude") is not None else "N/A"
            table.add_row(
                pc["iso3"], pc["name"], pc.get("geocoded_query", "N/A"),
                lat_str, lon_str,
                f"[{status_style}]{pc['status']}[/{status_style}]",
                pc.get("returned_display_name", "N/A"),
                f"{pc.get('returned_osm_category', 'N/A')}/{pc.get('returned_osm_type', 'N/A')}"
            )
        console.print(table)


def process_cities(conn: sqlite3.Connection):
    """Obtiene geodatos para ciudades que aún no los tienen o tuvieron errores."""
    print_info("\n--- Procesando Ciudades ---")
    cursor = conn.cursor()
    status_check_col = f"latitude{STATUS_SUFFIX}"
    cursor.execute(f"""
        SELECT c.id AS city_id, c.municipality, COALESCE(co.country_name, c.country) AS country_context
        FROM cities c
        LEFT JOIN countries co ON c.iso3c = co.country_code_iso3
        WHERE c.{status_check_col} IS NULL OR 
              c.{status_check_col} IN ('not_found', 'error_request', 'error_json', 'error_unknown')
    """)
    
    cities_to_process = cursor.fetchall()
    if not cities_to_process:
        print_info("No hay ciudades que necesiten nueva geocodificación o reintento.")
        return

    print_info(f"Encontradas {len(cities_to_process)} ciudades para geocodificar/reintentar.")

    processed_cities_summary = []
    for city_row in track(cities_to_process, description="Geocodificando ciudades..."):
        city_id = city_row['city_id'] 
        city_name = city_row['municipality']
        country_context = city_row['country_context'] 

        if not city_name or str(city_name).strip().lower() == 'nan':
            print_warning(f"Ciudad con ID '{city_id}' tiene nombre inválido: '{city_name}'. Marcando como 'error_invalid_name'.")
            update_record_geodata(conn, "cities", "id", city_id, {"status": "error_invalid_name"})
            processed_cities_summary.append({
                "id": city_id, "name": str(city_name), "country": country_context, 
                "latitude": None, "longitude": None, "status": "error_invalid_name",
                "geocoded_query": "N/A", "returned_display_name": "N/A"
            })
            continue
        
        if not country_context: 
             print_warning(f"Ciudad '{city_name}' (ID: {city_id}) no tiene contexto de país para refinar búsqueda. La geocodificación puede ser menos precisa.")
        
        # Para ciudades, is_country_query es False (o no se pasa)
        geo_results = geocode_location_nominatim(city_name, country_for_city_query=country_context)
        update_record_geodata(conn, "cities", "id", city_id, geo_results)
        processed_cities_summary.append({
            "id": city_id,
            "name": city_name,
            "country": country_context,
            "latitude": geo_results.get("latitude"),
            "longitude": geo_results.get("longitude"),
            "status": geo_results["status"],
            "geocoded_query": geo_results.get("geocoded_query"),
            "returned_display_name": geo_results.get("returned_display_name")
        })

    if USE_RICH and processed_cities_summary:
        table = Table(title="Resumen Geocodificación Ciudades (Resultados de esta ejecución)")
        table.add_column("ID DB", style="dim")
        table.add_column("Nombre Ciudad")
        table.add_column("País Contexto")
        table.add_column("Query Enviada")
        table.add_column("Lat", justify="right")
        table.add_column("Lon", justify="right")
        table.add_column("Estado Geo", justify="right")
        table.add_column("Respuesta Nominatim", overflow="fold")

        for pc in processed_cities_summary:
            status_style = "green" if pc["status"] == "found" else "red" if "error" in pc["status"] else "yellow"
            lat_str = f"{pc['latitude']:.4f}" if pc.get("latitude") is not None else "N/A"
            lon_str = f"{pc['longitude']:.4f}" if pc.get("longitude") is not None else "N/A"
            table.add_row(
                str(pc["id"]), pc["name"], pc.get("country", "N/A"),
                pc.get("geocoded_query", "N/A"),
                lat_str, lon_str,
                f"[{status_style}]{pc['status']}[/{status_style}]",
                pc.get("returned_display_name", "N/A")
            )
        console.print(table)


def main():
    global USER_AGENT 

    parser = argparse.ArgumentParser(description="Enriquece la base de datos con geodatos de OpenStreetMap (v3).")
    parser.add_argument("--db-file", default=DB_FILE_DEFAULT, help=f"Ruta al archivo de la base de datos SQLite (default: {DB_FILE_DEFAULT}).")
    parser.add_argument("--user-agent", default=USER_AGENT, help="User-Agent para las solicitudes a Nominatim. ¡Es importante cambiar el default!")
    parser.add_argument("--skip-countries", action="store_true", help="Omitir el procesamiento de países.")
    parser.add_argument("--skip-cities", action="store_true", help="Omitir el procesamiento de ciudades.")

    args = parser.parse_args()

    if args.user_agent != USER_AGENT and args.user_agent != "WasteDataProject/1.0 (contact@example.com)": 
        USER_AGENT = args.user_agent 
        print_warning(f"Usando User-Agent personalizado: {USER_AGENT}")
    elif USER_AGENT == "WasteDataProject/1.0 (contact@example.com)": 
        print_error("¡IMPORTANTE! Debes cambiar el User-Agent por defecto. Edita el script o usa --user-agent.")
        print_error("Ejemplo: --user-agent \"MiAppDeMapas/1.0 (miemail@dominio.com)\"")
        sys.exit(1)

    db_path = Path(args.db_file)
    conn = create_connection(db_path)

    if conn:
        add_geo_columns_if_not_exist(conn, "countries")
        add_geo_columns_if_not_exist(conn, "cities")

        if not args.skip_countries:
            process_countries(conn)
        else:
            print_info("Procesamiento de países omitido por argumento --skip-countries.")

        if not args.skip_cities:
            process_cities(conn)
        else:
            print_info("Procesamiento de ciudades omitido por argumento --skip-cities.")

        conn.close()
        print_info(f"\nConexión a la base de datos '{db_path}' cerrada.")
        print_success("Proceso de enriquecimiento de geodatos (v3) completado.")
    else:
        print_error("No se pudo establecer conexión con la base de datos. Abortando.")
        sys.exit(1)

if __name__ == "__main__":
    main()
