#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# /// script
# dependencies = [
#   "pandas>=2.0.0",
#   "rich>=13.7.0",
#   "numpy>=1.20.0"
# ]
# ///

"""
SFA: Cluster Comparative Analysis

Carga los resultados de la clusterización (clusters.json) y los datos detallados
de la base de datos (waste_data.db) para realizar un análisis comparativo
de las entidades dentro de clústeres específicos.
Calcula y muestra estadísticas descriptivas para características clave.
"""

import json
import pandas as pd
import numpy as np
import sqlite3
from pathlib import Path
import argparse
from collections import Counter
from typing import Tuple, List, Dict, Any # <--- IMPORTACIÓN CORREGIDA/AÑADIDA

# --- Configuración Visual (Rich) ---
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    from rich.padding import Padding
    console = Console(stderr=True)
    USE_RICH = True
    def print_info(message): console.print(f"[cyan]ℹ️ {message}[/cyan]")
    def print_success(message): console.print(f"[green]✅ {message}[/green]")
    def print_warning(message): console.print(f"[yellow]⚠️ {message}[/yellow]")
    def print_error(message): console.print(Panel(Text(str(message), style="bold red"), title="[bold red]ERROR[/bold red]", border_style="red"))
    def print_heading(title, level=1):
        if level == 1:
            console.print(Padding(Text(title, style="bold underline magenta"), (1, 0, 1, 0)))
        elif level == 2:
            console.print(Padding(Text(title, style="bold blue"), (1, 0, 0, 0)))
        else:
            console.print(Text(title, style="bold"))
except ImportError:
    USE_RICH = False
    # Funciones print básicas
    def print_info(message): print(f"INFO: {message}", file=sys.stderr)
    def print_success(message): print(f"SUCCESS: {message}", file=sys.stderr)
    def print_warning(message): print(f"WARNING: {message}", file=sys.stderr)
    def print_error(message): print(f"ERROR: {message}", file=sys.stderr)
    def print_heading(title, level=1):
        if level == 1: print(f"\n--- {title.upper()} ---\n", file=sys.stderr)
        elif level == 2: print(f"\n-- {title} --\n", file=sys.stderr)
        else: print(f"{title}", file=sys.stderr)
    class Table:
        def __init__(self, title="", show_header=True, header_style=""): self.title = title; self.headers = []; self.rows = []; self.show_header=show_header
        def add_column(self, header, style="", justify="left", min_width=None, overflow=None): self.headers.append(header)
        def add_row(self, *args): self.rows.append(list(args))
        def add_section(self): pass
        def __str__(self):
            output = []
            if self.title: output.append(self.title)
            if self.show_header and self.headers: output.append(" | ".join(map(str, self.headers)))
            for row in self.rows: output.append(" | ".join(map(str, row)))
            return "\n".join(output)
    def console_print_table(table_obj): print(str(table_obj))
# --- Fin Configuración Visual ---

# --- Constantes ---
DEFAULT_DB_PATH = "output/waste_data.db"
DEFAULT_CLUSTERS_JSON = "output/quality_analysis/entity_clusters.json"

# Características para análisis descriptivo y sus nombres en DB
COUNTRY_ANALYSIS_FEATURES = {
    "data_quality_score": "data_quality_score",
    "population_total": "population_total",
    "total_msw_generated_tons_year": "total_msw_generated_tons_year",
    "waste_per_capita_kg_day": "waste_per_capita_kg_day", 
    "waste_collection_coverage_total_percent_of_population": "waste_collection_coverage_total_percent_of_population",
    "waste_treatment_recycling_percent": "waste_treatment_recycling_percent",
    "composition_food_organic": "composition_food_organic"
}

CITY_ANALYSIS_FEATURES = {
    "data_quality_score": "data_quality_score",
    "population": "population",
    "total_waste_tons_year": "total_waste_tons_year",
    "waste_per_capita_kg_day": "waste_per_capita_kg_day", 
    "collection_coverage_population_percent": "collection_coverage_population_percent",
    "recycling_rate_percent": "recycling_rate_percent",
    "composition_food_organic": "composition_food_organic"
}

def create_db_connection(db_path_str: str) -> sqlite3.Connection:
    db_path = Path(db_path_str)
    print_info(f"Conectando a la base de datos: {db_path}")
    if not db_path.exists():
        print_error(f"Archivo de base de datos no encontrado: {db_path}")
        sys.exit(1)
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        print_success("Conexión a la base de datos establecida.")
        return conn
    except sqlite3.Error as e:
        print_error(f"Error conectando a la base de datos: {e}")
        sys.exit(1)

def load_cluster_data(json_file_path: str) -> dict:
    print_info(f"Cargando datos de clústeres desde: {json_file_path}")
    path = Path(json_file_path)
    if not path.is_file():
        print_error(f"El archivo JSON de clústeres no se encontró en: {json_file_path}")
        sys.exit(1)
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print_success(f"Datos de clústeres cargados: {len(data.get('country_clusters',[]))} clústeres de país, {len(data.get('city_clusters',[]))} clústeres de ciudad.")
        return data
    except json.JSONDecodeError as e:
        print_error(f"Error al decodificar JSON de clústeres: {e}")
        sys.exit(1)
    except Exception as e:
        print_error(f"Error inesperado al cargar datos de clústeres: {e}")
        sys.exit(1)

def load_entity_data_from_db(conn: sqlite3.Connection) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Carga datos detallados de países y ciudades desde la BD."""
    print_info("Cargando datos detallados de entidades desde la base de datos...")
    try:
        df_countries_db = pd.read_sql_query("SELECT * FROM countries", conn)
        df_cities_db = pd.read_sql_query("SELECT * FROM cities", conn)
        print_success(f"Cargados {len(df_countries_db)} registros de países y {len(df_cities_db)} registros de ciudades de la BD.")

        if 'population_total' in df_countries_db.columns and 'total_msw_generated_tons_year' in df_countries_db.columns:
            if 'waste_per_capita_kg_day' not in df_countries_db.columns:
                df_countries_db['waste_per_capita_kg_day'] = np.nan
            
            pop = pd.to_numeric(df_countries_db['population_total'], errors='coerce')
            waste = pd.to_numeric(df_countries_db['total_msw_generated_tons_year'], errors='coerce')
            
            mask_calc_needed = df_countries_db['waste_per_capita_kg_day'].isna()
            mask_calc_possible = (pop > 0) & pop.notna() & waste.notna()
            final_mask = mask_calc_needed & mask_calc_possible

            df_countries_db.loc[final_mask, 'waste_per_capita_kg_day'] = \
                (waste[final_mask] * 1000) / (pop[final_mask] * 365)
        
        if 'population' in df_cities_db.columns and 'total_waste_tons_year' in df_cities_db.columns:
            if 'waste_per_capita_kg_day' not in df_cities_db.columns:
                 df_cities_db['waste_per_capita_kg_day'] = np.nan
            
            pop_city = pd.to_numeric(df_cities_db['population'], errors='coerce')
            waste_city = pd.to_numeric(df_cities_db['total_waste_tons_year'], errors='coerce')

            mask_calc_needed_city = df_cities_db['waste_per_capita_kg_day'].isna()
            mask_calc_possible_city = (pop_city > 0) & pop_city.notna() & waste_city.notna()
            final_mask_city = mask_calc_needed_city & mask_calc_possible_city
            
            df_cities_db.loc[final_mask_city, 'waste_per_capita_kg_day'] = \
                (waste_city[final_mask_city] * 1000) / (pop_city[final_mask_city] * 365)
            
            if 'waste_gen_rate_kg_cap_day' in df_cities_db.columns:
                mask_still_nan = df_cities_db['waste_per_capita_kg_day'].isna()
                df_cities_db.loc[mask_still_nan, 'waste_per_capita_kg_day'] = pd.to_numeric(df_cities_db.loc[mask_still_nan, 'waste_gen_rate_kg_cap_day'], errors='coerce')

        return df_countries_db, df_cities_db
    except pd.io.sql.DatabaseError as e:
        print_error(f"Error de base de datos al cargar datos de entidades: {e}")
        sys.exit(1)
    except Exception as e:
        print_error(f"Error inesperado al cargar datos de entidades: {e}")
        sys.exit(1)


def analyze_clusters_descriptively(
    clustered_entities_df: pd.DataFrame, 
    entity_data_df: pd.DataFrame, 
    entity_type: str, 
    analysis_features_config: Dict[str, str],
    cluster_ids_to_analyze: List[str]
):
    """Calcula y muestra estadísticas descriptivas para entidades dentro de clústeres específicos."""
    
    if clustered_entities_df.empty:
        print_warning(f"No hay datos de clústeres para {entity_type} para analizar.")
        return

    print_heading(f"Análisis Descriptivo para Clústeres Seleccionados de {entity_type.capitalize()}", level=2)

    join_col_clusters = 'entity_id' 
    if entity_type == 'country':
        join_col_db = 'country_code_iso3' 
    elif entity_type == 'city':
        join_col_db = 'id' 
    else:
        print_error(f"Tipo de entidad desconocido: {entity_type}")
        return

    if join_col_db in entity_data_df.columns:
        if clustered_entities_df[join_col_clusters].dtype != entity_data_df[join_col_db].dtype:
            try:
                if clustered_entities_df[join_col_clusters].dtype == 'object' and pd.api.types.is_numeric_dtype(entity_data_df[join_col_db]):
                     clustered_entities_df[join_col_clusters] = pd.to_numeric(clustered_entities_df[join_col_clusters], errors='coerce')
                elif pd.api.types.is_numeric_dtype(clustered_entities_df[join_col_clusters]) and entity_data_df[join_col_db].dtype == 'object':
                     entity_data_df[join_col_db] = pd.to_numeric(entity_data_df[join_col_db], errors='coerce')
            except Exception as e:
                print_warning(f"No se pudo alinear los tipos de las columnas de unión ('{join_col_clusters}', '{join_col_db}'): {e}")

    merged_df = pd.merge(
        clustered_entities_df,
        entity_data_df,
        left_on=join_col_clusters,
        right_on=join_col_db,
        how='left'
    )
    
    if merged_df.empty:
        print_warning(f"La unión de datos de clúster y de entidad resultó vacía para {entity_type}.")
        return

    for cluster_id in cluster_ids_to_analyze:
        print_heading(f"Análisis para Cluster ID: {cluster_id} ({entity_type.capitalize()})", level=3)
        
        cluster_subset_df = merged_df[merged_df['cluster_id'] == cluster_id]

        if cluster_subset_df.empty:
            print_warning(f"No se encontraron entidades para el Cluster ID: {cluster_id}")
            continue

        cluster_name_display = cluster_subset_df['cluster_name'].iloc[0] if not cluster_subset_df.empty else "Nombre Desconocido"
        print_info(f"Nombre del Clúster: {cluster_name_display}")
        print_info(f"Número de entidades en este clúster: {len(cluster_subset_df)}")

        stats_table = Table(title=f"Estadísticas Descriptivas para Cluster: {cluster_id}", show_header=True, header_style="bold cyan")
        stats_table.add_column("Característica", style="dim", min_width=30)
        stats_table.add_column("Media", justify="right")
        stats_table.add_column("Mediana", justify="right")
        stats_table.add_column("Mín.", justify="right")
        stats_table.add_column("Máx.", justify="right")
        stats_table.add_column("Desv. Est.", justify="right")
        stats_table.add_column("Nº Válidos", justify="right")

        for feature_label, db_col_name in analysis_features_config.items():
            if db_col_name in cluster_subset_df.columns:
                feature_series = pd.to_numeric(cluster_subset_df[db_col_name], errors='coerce')
                if not feature_series.isna().all():
                    desc_stats = feature_series.describe()
                    stats_table.add_row(
                        feature_label,
                        f"{desc_stats.get('mean', np.nan):.2f}" if pd.notna(desc_stats.get('mean')) else "N/A",
                        f"{desc_stats.get('50%', np.nan):.2f}" if pd.notna(desc_stats.get('50%')) else "N/A", 
                        f"{desc_stats.get('min', np.nan):.2f}" if pd.notna(desc_stats.get('min')) else "N/A",
                        f"{desc_stats.get('max', np.nan):.2f}" if pd.notna(desc_stats.get('max')) else "N/A",
                        f"{desc_stats.get('std', np.nan):.2f}" if pd.notna(desc_stats.get('std')) else "N/A",
                        str(int(desc_stats.get('count', 0)))
                    )
                else:
                    stats_table.add_row(feature_label, "N/A", "N/A", "N/A", "N/A", "N/A", "0")
            else:
                print_warning(f"  Característica '{db_col_name}' no encontrada en los datos detallados para el clúster {cluster_id}.")
                stats_table.add_row(feature_label, "Columna Ausente", "-", "-", "-", "-", "-")
        
        if USE_RICH:
            console.print(stats_table)
        else:
            console_print_table(stats_table)


def main():
    parser = argparse.ArgumentParser(description="SFA: Realiza análisis comparativo de clústeres de entidades.")
    parser.add_argument("--clusters-json", default=DEFAULT_CLUSTERS_JSON, help=f"Ruta al archivo JSON de clústeres (default: {DEFAULT_CLUSTERS_JSON}).")
    parser.add_argument("--db-file", default=DEFAULT_DB_PATH, help=f"Ruta al archivo de la base de datos SQLite (default: {DEFAULT_DB_PATH}).")
    parser.add_argument("--num-top-clusters", type=int, default=1, help="Número de los clústeres más grandes a analizar para cada tipo de entidad (país/ciudad).")
    parser.add_argument("--country-cluster-ids", nargs='*', help="Lista opcional de IDs de clústeres de países específicos a analizar.")
    parser.add_argument("--city-cluster-ids", nargs='*', help="Lista opcional de IDs de clústeres de ciudades específicos a analizar.")

    args = parser.parse_args()

    print_heading("SFA: Análisis Comparativo de Clústeres")

    cluster_data_json = load_cluster_data(args.clusters_json)
    
    conn = create_db_connection(args.db_file)
    df_countries_db, df_cities_db = load_entity_data_from_db(conn)
    conn.close()

    df_country_clusters = pd.DataFrame(cluster_data_json.get('country_clusters', []))
    df_city_clusters = pd.DataFrame(cluster_data_json.get('city_clusters', []))

    country_clusters_to_analyze = args.country_cluster_ids
    if not country_clusters_to_analyze and not df_country_clusters.empty:
        top_country_clusters = df_country_clusters['cluster_id'].value_counts().nlargest(args.num_top_clusters).index.tolist()
        country_clusters_to_analyze = top_country_clusters
        print_info(f"Analizando los {args.num_top_clusters} clústeres de países más grandes: {top_country_clusters}")

    city_clusters_to_analyze = args.city_cluster_ids
    if not city_clusters_to_analyze and not df_city_clusters.empty:
        top_city_clusters = df_city_clusters['cluster_id'].value_counts().nlargest(args.num_top_clusters).index.tolist()
        city_clusters_to_analyze = top_city_clusters
        print_info(f"Analizando los {args.num_top_clusters} clústeres de ciudades más grandes: {top_city_clusters}")

    if country_clusters_to_analyze:
        analyze_clusters_descriptively(df_country_clusters, df_countries_db, "country", COUNTRY_ANALYSIS_FEATURES, country_clusters_to_analyze)
    else:
        print_warning("No se analizarán clústeres de países (no especificados o no encontrados).")

    if city_clusters_to_analyze:
        analyze_clusters_descriptively(df_city_clusters, df_cities_db, "city", CITY_ANALYSIS_FEATURES, city_clusters_to_analyze)
    else:
        print_warning("No se analizarán clústeres de ciudades (no especificados o no encontrados).")

    print_success("Análisis comparativo de clústeres completado.")

if __name__ == "__main__":
    main()
