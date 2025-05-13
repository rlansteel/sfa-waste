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
SFA: Data Enhancer by Cluster (sfa_data_enhancer_by_cluster.py) - v3 (Depuración Mejorada)

Este script mejora la completitud de la base de datos 'waste_data_clean.db'
extrapolando valores faltantes. Utiliza asignaciones de clústeres de entidades
(países y ciudades) y calcula valores de extrapolación (mediana) basados
exclusivamente en datos con '_status = original' de otros miembros del mismo clúster.
"""

import argparse
import json
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
import sys
from typing import Dict, Any, List, Optional, Tuple

# --- Configuración Visual (Rich) ---
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.progress import track
    from rich.text import Text 
    from rich.padding import Padding 
    console = Console(stderr=True)
    USE_RICH = True
    def print_info(message): console.print(f"[cyan]ℹ️ {message}[/cyan]")
    def print_success(message): console.print(f"[green]✅ {message}[/green]")
    def print_warning(message): console.print(f"[yellow]⚠️ {message}[/yellow]")
    def print_error(message): console.print(Panel(f"[bold red]❌ ERROR:[/bold red]\n{message}", border_style="red"))
    def print_debug(message): console.print(f"[dim blue]DEBUG: {message}[/dim blue]") # Nueva función de depuración
    def print_heading(title, level=1):
        if USE_RICH:
            if level == 1:
                console.print(Padding(Text(title, style="bold underline magenta"), (1, 0, 1, 0)))
            elif level == 2:
                console.print(Padding(Text(title, style="bold blue"), (1, 0, 0, 0)))
            else:
                console.print(Text(title, style="bold"))
        else:
            if level == 1: print(f"\n--- {title.upper()} ---\n", file=sys.stderr)
            elif level == 2: print(f"\n-- {title} --\n", file=sys.stderr)
            else: print(f"{title}", file=sys.stderr)

except ImportError:
    USE_RICH = False
    def print_info(message): print(f"INFO: {message}", file=sys.stderr)
    def print_success(message): print(f"SUCCESS: {message}", file=sys.stderr)
    def print_warning(message): print(f"WARNING: {message}", file=sys.stderr)
    def print_error(message): print(f"ERROR: {message}", file=sys.stderr)
    def print_debug(message): print(f"DEBUG: {message}", file=sys.stderr) # Fallback para depuración
    def print_heading(title, level=1):
        if level == 1: print(f"\n--- {title.upper()} ---\n", file=sys.stderr)
        elif level == 2: print(f"\n-- {title} --\n", file=sys.stderr)
        else: print(f"{title}", file=sys.stderr)
    def track(sequence, description="Procesando..."): yield from sequence
# --- Fin Configuración Visual ---

# --- CONFIGURACIÓN DE EXTRAPOLACIÓN ---
DEFAULT_DB_PATH = "output/db/waste_data_clean.db"
DEFAULT_CLUSTERS_JSON_PATH = "output/quality_analysis_clean/entity_clusters_clean.json"
MIN_ORIGINAL_SAMPLES_THRESHOLD = 3 
EXTRAPOLATION_METHOD = "median" 
EXTRAPOLATED_STATUS_LABEL = "extrapolated_by_sfa5"

TARGET_FEATURES_CONFIG = {
    "countries": {
        "table_name": "countries",
        "pk_column": "country_code_iso3", 
        "entity_id_col_in_clusters_json": "entity_id", 
        "features": [
            "total_msw_generated_tons_year",
            "waste_collection_coverage_total_percent_of_population",
            "waste_treatment_recycling_percent",
            "waste_treatment_compost_percent",
            "waste_treatment_incineration_percent",
            "waste_treatment_sanitary_landfill_percent",
            "waste_treatment_controlled_landfill_percent",
            "waste_treatment_landfill_unspecified_percent",
            "waste_treatment_open_dump_percent",
            "composition_food_organic",
            "composition_glass",
            "composition_metal",
            "composition_paper_cardboard",
            "composition_plastic",
            "composition_rubber_leather",
            "composition_wood",
            "composition_yard_garden_green",
            "composition_other",
            "e_waste_tons_year",
            "hazardous_waste_tons_year",
            "gdp_per_capita_usd",
            "population_total"
        ]
    },
    "cities": {
        "table_name": "cities",
        "pk_column": "id", 
        "entity_id_col_in_clusters_json": "entity_id", 
        "features": [
            "population",
            "total_waste_tons_year",
            "recycling_rate_percent",
            "collection_coverage_population_percent",
            "composition_food_organic",
            "composition_glass",
            "composition_metal",
            "composition_paper_cardboard",
            "composition_plastic",
            "composition_rubber_leather",
            "composition_wood",
            "composition_yard_garden_green",
            "composition_other",
            "waste_gen_rate_kg_cap_day",
            "collection_pct_formal",
            "collection_pct_informal",
            "treatment_pct_recycling_composting",
            "treatment_pct_thermal",
            "treatment_pct_landfill"
        ]
    }
}
# --- FIN CONFIGURACIÓN ---

def connect_db(db_path_str: str) -> Optional[sqlite3.Connection]:
    db_path = Path(db_path_str)
    print_info(f"Conectando a la base de datos: {db_path}")
    if not db_path.exists():
        print_error(f"El archivo de base de datos '{db_path}' no existe.")
        return None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row 
        print_success("Conexión a la base de datos establecida.")
        return conn
    except sqlite3.Error as e:
        print_error(f"Error conectando a la base de datos: {e}")
        return None

def load_clusters_data(clusters_json_path_str: str) -> Dict[str, pd.DataFrame]:
    clusters_path = Path(clusters_json_path_str)
    print_info(f"Cargando datos de clústeres desde: {clusters_path}")
    if not clusters_path.is_file():
        print_error(f"El archivo JSON de clústeres no se encontró en: {clusters_path}")
        return {"countries": pd.DataFrame(), "cities": pd.DataFrame()}
    try:
        with open(clusters_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        df_country_clusters = pd.DataFrame(data.get('country_clusters', []))
        df_city_clusters = pd.DataFrame(data.get('city_clusters', []))
        
        # Asegurar que la columna de ID de clúster sea del tipo correcto
        if not df_country_clusters.empty and 'entity_id' in df_country_clusters.columns:
            df_country_clusters['entity_id'] = df_country_clusters['entity_id'].astype(str) # ISO3 es string
        if not df_city_clusters.empty and 'entity_id' in df_city_clusters.columns:
            # Primero convertir a numérico (puede ser float si hay NaNs), luego a Int64 para permitir NaNs enteros
            df_city_clusters['entity_id'] = pd.to_numeric(df_city_clusters['entity_id'], errors='coerce').astype('Int64') 

        print_success(f"Datos de clústeres cargados: {len(df_country_clusters)} países, {len(df_city_clusters)} ciudades.")
        return {"countries": df_country_clusters, "cities": df_city_clusters}
    except json.JSONDecodeError as e:
        print_error(f"Error al decodificar JSON de clústeres: {e}")
    except Exception as e:
        print_error(f"Error inesperado al cargar datos de clústeres: {e}")
    return {"countries": pd.DataFrame(), "cities": pd.DataFrame()}

def get_entity_data_for_extrapolation(conn: sqlite3.Connection, entity_type_config: Dict, target_feature: str) -> pd.DataFrame:
    table_name = entity_type_config["table_name"]
    pk_column = entity_type_config["pk_column"]
    status_column = f"{target_feature}_status"
    query = f'SELECT "{pk_column}", "{target_feature}", "{status_column}" FROM "{table_name}";'
    try:
        df = pd.read_sql_query(query, conn)
        if target_feature in df.columns:
            df[target_feature] = pd.to_numeric(df[target_feature], errors='coerce')
        
        if table_name == "countries" and pk_column == "country_code_iso3":
            if pk_column in df.columns:
                df[pk_column] = df[pk_column].astype(str)
        elif table_name == "cities" and pk_column == "id":
            if pk_column in df.columns:
                 # Convertir a numérico primero, luego a Int64 para manejar posibles NaNs de la conversión
                df[pk_column] = pd.to_numeric(df[pk_column], errors='coerce').astype('Int64')
            
        return df
    except Exception as e:
        print_error(f"Error obteniendo datos para extrapolación de '{target_feature}' en tabla '{table_name}': {e}")
        return pd.DataFrame()

def get_cluster_member_original_values(
    conn: sqlite3.Connection,
    table_name: str,
    pk_column_db: str,
    member_entity_ids: List[Any],
    target_feature: str
) -> List[float]:
    if not member_entity_ids:
        return []
    status_col_name = f"{target_feature}_status"
    
    # Filtrar NaNs de member_entity_ids antes de crear placeholders
    valid_member_entity_ids = [mid for mid in member_entity_ids if pd.notna(mid)]
    if not valid_member_entity_ids:
        return []

    placeholders = ', '.join('?' * len(valid_member_entity_ids))
    query = f"""
        SELECT "{target_feature}"
        FROM "{table_name}"
        WHERE "{pk_column_db}" IN ({placeholders})
          AND "{status_col_name}" = 'original';
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, valid_member_entity_ids)
        rows = cursor.fetchall()
        original_values = [row[target_feature] for row in rows if pd.notna(row[target_feature])]
        return [float(v) for v in original_values] 
    except sqlite3.Error as e:
        print_error(f"Error DB obteniendo valores originales para '{target_feature}' en '{table_name}': {e}")
    except Exception as e_gen:
        print_error(f"Error general obteniendo valores originales para '{target_feature}' en '{table_name}': {e_gen}")
    return []

def update_db_with_extrapolated_value(
    conn: sqlite3.Connection,
    table_name: str,
    pk_column_db: str,
    entity_pk_value: Any,
    target_feature: str,
    extrapolated_value: float
):
    status_col_name = f"{target_feature}_status"
    query = f"""
        UPDATE "{table_name}"
        SET "{target_feature}" = ?,
            "{status_col_name}" = ?
        WHERE "{pk_column_db}" = ?;
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, (extrapolated_value, EXTRAPOLATED_STATUS_LABEL, entity_pk_value))
        conn.commit() 
        if cursor.rowcount > 0:
            print_success(f"  Extrapolado: {table_name}.{target_feature} para {pk_column_db}={entity_pk_value} con valor {extrapolated_value:.2f} (cluster).")
            return True
        else:
            print_warning(f"  No se actualizó {table_name}.{target_feature} para {pk_column_db}={entity_pk_value}. ¿Registro no encontrado o valor ya existente?")
            return False
    except sqlite3.Error as e:
        print_error(f"Error DB actualizando valor extrapolado para '{target_feature}' en {pk_column_db}={entity_pk_value}: {e}")
        conn.rollback() 
        return False

def main():
    parser = argparse.ArgumentParser(description="SFA: Data Enhancer by Cluster - Extrapola datos faltantes usando clústeres.")
    parser.add_argument("--db-file", default=DEFAULT_DB_PATH,
                        help=f"Ruta al archivo de la base de datos SQLite (default: {DEFAULT_DB_PATH}).")
    parser.add_argument("--clusters-json", default=DEFAULT_CLUSTERS_JSON_PATH,
                        help=f"Ruta al archivo JSON con los clústeres de entidades (default: {DEFAULT_CLUSTERS_JSON_PATH}).")
    parser.add_argument("--min-samples", type=int, default=MIN_ORIGINAL_SAMPLES_THRESHOLD,
                        help=f"Umbral mínimo de muestras originales en un clúster para extrapolar (default: {MIN_ORIGINAL_SAMPLES_THRESHOLD}).")
    parser.add_argument("--method", choices=["median", "mean"], default=EXTRAPOLATION_METHOD,
                        help=f"Método de extrapolación (default: {EXTRAPOLATION_METHOD}).")

    args = parser.parse_args()

    print_heading(f"SFA: Data Enhancer by Cluster (DB: {args.db_file}, Clusters: {args.clusters_json})")

    conn = connect_db(args.db_file)
    if conn is None:
        sys.exit(1)

    all_clusters_data = load_clusters_data(args.clusters_json)
    if all_clusters_data["countries"].empty and all_clusters_data["cities"].empty:
        print_warning("No se cargaron datos de clústeres. No se puede proceder con la extrapolación.")
        conn.close()
        sys.exit(0)

    total_extrapolations_done = 0

    for entity_type_label, config in TARGET_FEATURES_CONFIG.items(): 
        print_heading(f"Procesando Entidades: {entity_type_label.capitalize()}", level=2)
        
        table_name = config["table_name"]
        pk_column_db = config["pk_column"] 
        entity_id_col_clusters = config["entity_id_col_in_clusters_json"] 
        features_to_extrapolate = config["features"]
        
        df_clusters_for_type = all_clusters_data.get(entity_type_label)
        if df_clusters_for_type is None or df_clusters_for_type.empty:
            print_info(f"No hay datos de clústeres para {entity_type_label}. Omitiendo.")
            continue
        
        # Limpiar df_clusters_for_type de filas donde entity_id_col_clusters es NaN después de la conversión
        df_clusters_for_type.dropna(subset=[entity_id_col_clusters], inplace=True)


        for target_feature in features_to_extrapolate:
            print_info(f"Extrapolando para característica: '{target_feature}' en '{table_name}'")
            
            df_entities_feature_data = get_entity_data_for_extrapolation(conn, config, target_feature)
            if df_entities_feature_data.empty or target_feature not in df_entities_feature_data.columns:
                print_warning(f"No se pudo obtener datos o la columna '{target_feature}' no existe para {table_name}. Omitiendo característica.")
                continue
            
            # Limpiar df_entities_feature_data de filas donde pk_column_db es NaN después de la conversión
            df_entities_feature_data.dropna(subset=[pk_column_db], inplace=True)


            entities_needing_extrapolation = df_entities_feature_data[df_entities_feature_data[target_feature].isna()]
            
            if entities_needing_extrapolation.empty:
                print_info(f"No hay entidades que necesiten extrapolación para '{target_feature}' en '{table_name}'.")
                continue
            
            print_info(f"Encontradas {len(entities_needing_extrapolation)} entidades con '{target_feature}' faltante en '{table_name}'.")

            iterator = track(entities_needing_extrapolation.iterrows(), description=f"Extrapolando {target_feature} ({entity_type_label})...", total=len(entities_needing_extrapolation)) if USE_RICH else entities_needing_extrapolation.iterrows()

            for _, entity_row in iterator:
                entity_pk_value = entity_row[pk_column_db] 
                
                # Verificar si entity_pk_value es NaN después de obtenerlo de entity_row
                if pd.isna(entity_pk_value):
                    print_debug(f"  Valor PK nulo para una entidad en la tabla {table_name} para {target_feature}. Omitiendo.")
                    continue
                print_debug(f"Procesando entidad {pk_column_db}={entity_pk_value} para {target_feature}")

                # Comparar tipos antes de la búsqueda en df_clusters_for_type
                # print_debug(f"  Tipo de entity_pk_value: {type(entity_pk_value)}")
                # if not df_clusters_for_type.empty:
                #     print_debug(f"  Tipo de df_clusters_for_type[{entity_id_col_clusters}].iloc[0]: {type(df_clusters_for_type[entity_id_col_clusters].iloc[0])}")


                cluster_info_list = df_clusters_for_type[df_clusters_for_type[entity_id_col_clusters] == entity_pk_value]
                
                if cluster_info_list.empty:
                    print_debug(f"  No se encontró clúster para {pk_column_db}={entity_pk_value} (tipo: {type(entity_pk_value)}). Omitiendo extrapolación para {target_feature}.")
                    continue
                
                current_cluster_id = cluster_info_list["cluster_id"].iloc[0]
                print_debug(f"  Entidad {pk_column_db}={entity_pk_value} pertenece al clúster ID: {current_cluster_id}")
                
                member_entity_ids_series = df_clusters_for_type[df_clusters_for_type["cluster_id"] == current_cluster_id][entity_id_col_clusters]
                # Excluir la entidad actual y cualquier NaN que pudiera haber quedado
                member_entity_ids_for_calc = member_entity_ids_series[(member_entity_ids_series != entity_pk_value) & (pd.notna(member_entity_ids_series))].tolist()


                if not member_entity_ids_for_calc:
                    print_debug(f"  No hay otros miembros en el clúster {current_cluster_id} para {pk_column_db}={entity_pk_value} para calcular {target_feature}.")
                    continue
                print_debug(f"  Miembros del clúster {current_cluster_id} para cálculo ({len(member_entity_ids_for_calc)}): {member_entity_ids_for_calc[:5]}...") 
                    
                original_values = get_cluster_member_original_values(
                    conn, table_name, pk_column_db, member_entity_ids_for_calc, target_feature
                )
                print_debug(f"  Valores originales encontrados para {target_feature} en clúster {current_cluster_id}: {original_values}")
                
                if len(original_values) < args.min_samples:
                    print_debug(f"  Insuficientes muestras originales ({len(original_values)} < {args.min_samples}) para {target_feature} en clúster {current_cluster_id}. No se extrapola para {pk_column_db}={entity_pk_value}.")
                    continue
                    
                extrapolated_value = None
                if args.method == "median":
                    extrapolated_value = np.median(original_values)
                elif args.method == "mean":
                    extrapolated_value = np.mean(original_values)
                
                print_debug(f"  Valor extrapolado calculado para {target_feature} ({args.method}): {extrapolated_value}")

                if pd.notna(extrapolated_value):
                    if update_db_with_extrapolated_value(conn, table_name, pk_column_db, entity_pk_value, target_feature, extrapolated_value):
                        total_extrapolations_done += 1
                else:
                    print_warning(f"  Cálculo de extrapolación para '{target_feature}' en {pk_column_db}={entity_pk_value} resultó en NaN (Cluster: {current_cluster_id}).")

    print_heading("Resumen de Extrapolación", level=2) 
    print_success(f"Total de valores extrapolados y actualizados en la base de datos: {total_extrapolations_done}")

    if conn:
        conn.close()
        print_info("Conexión a la base de datos cerrada.")

if __name__ == "__main__":
    main()
