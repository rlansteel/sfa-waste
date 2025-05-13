#!/usr/bin/env python3

# /// script
# dependencies = [
#   "rich>=13.7.0",
#   "pandas>=2.0.0",
#   "numpy>=1.20.0"
# ]
# ///

"""
SFA: Data Quality Analyzer (v3 - Calculation Summary Included)

Analiza los datos en la base de datos SQLite 'waste_data.db', calcula un puntaje
de calidad para cada país y ciudad, actualiza las tablas con este puntaje y
genera archivos JSON con los rankings.

Mejoras v3:
- Incluye un resumen legible del cálculo del puntaje en 'score_details_json'.
- Cuenta mediciones únicas para el puntaje.
- Es consciente de la metodología ('WB', 'WFD') para ajustar expectativas de campos.
- Ajusta pesos para dar más relevancia a indicadores clave agregados.
- Considera 'composition_calculation_status'.
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple, Set

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
    def print_heading(title): console.print(f"\n[bold underline magenta]{title}[/bold underline magenta]\n")
except ImportError:
    USE_RICH = False
    # Funciones print básicas
    def print_info(message): print(f"INFO: {message}", file=sys.stderr)
    def print_success(message): print(f"SUCCESS: {message}", file=sys.stderr)
    def print_warning(message): print(f"WARNING: {message}", file=sys.stderr)
    def print_error(message): print(f"ERROR: {message}", file=sys.stderr)
    def print_heading(title): print(f"\n--- {title} ---\n", file=sys.stderr)
    def track(sequence, description="Procesando..."): yield from sequence
# --- Fin Configuración Visual ---

# --- Constantes de Puntuación y Métricas (Ajustadas v2) ---
# (Sin cambios respecto a v2)
WEIGHTS = {
    "completeness_core": 2.5,
    "completeness_secondary": 1.0,
    "status_original": 1.5,
    "status_imputed": 0.6,
    "status_recalculated": 1.0,
    "status_extrapolated": 0.8,
    "geocoding_found": 1.0,
    "composition_consistency": 3.5,
    "measurements_availability_unique": 0.8,
    "measurements_recency": 0.1,
    "measurements_details": 0.2
}
POINTS = {
    "field_present_original": 10,
    "field_present_imputed": 6,
    "field_present_recalculated": 8,
    "field_present_extrapolated": 7,
    "geocoding_found": 10,
    "composition_consistent_strict": 15,
    "composition_consistent_tolerant": 10,
    "measurement_available_unique": 5,
    "measurement_has_year": 1,
    "measurement_recent_year_bonus": 3,
    "measurement_has_source": 2,
    "measurement_has_comments": 1
}
MAX_POSSIBLE_SCORE_PER_FIELD_ORIGINAL = POINTS["field_present_original"] * WEIGHTS["status_original"]
MAX_POSSIBLE_SCORE_GEO = POINTS["geocoding_found"] * WEIGHTS["geocoding_found"]
MAX_POSSIBLE_SCORE_COMPOSITION = POINTS["composition_consistent_strict"] * WEIGHTS["composition_consistency"]
MAX_EXPECTED_UNIQUE_MEASUREMENTS = 7
MAX_POSSIBLE_SCORE_MEASUREMENTS_AVAIL = MAX_EXPECTED_UNIQUE_MEASUREMENTS * POINTS["measurement_available_unique"] * WEIGHTS["measurements_availability_unique"]
MAX_POSSIBLE_SCORE_MEASUREMENT_DETAILS = MAX_EXPECTED_UNIQUE_MEASUREMENTS * (
    (POINTS["measurement_has_year"] + POINTS["measurement_recent_year_bonus"]) * WEIGHTS["measurements_recency"] +
    POINTS["measurement_has_source"] * WEIGHTS["measurements_details"] +
    POINTS["measurement_has_comments"] * WEIGHTS["measurements_details"]
)
COUNTRY_CORE_FIELDS_WB = [
    'country_name', 'country_code_iso3', 'region', 'income_group_wb',
    'population_total', 'total_msw_generated_tons_year',
    'waste_collection_coverage_total_percent_of_population',
    'waste_treatment_recycling_percent'
]
CITY_CORE_FIELDS_WB = [
    'municipality', 'iso3c', 'country', 'population', 'total_waste_tons_year',
    'recycling_rate_percent',
    'collection_coverage_population_percent'
]
CITY_CORE_FIELDS_WFD = [
    'municipality', 'iso3c', 'country', 'population',
    'waste_gen_rate_kg_cap_day',
    'collection_pct_formal',
]
COMPOSITION_FIELDS = [
    'composition_food_organic', 'composition_glass', 'composition_metal',
    'composition_paper_cardboard', 'composition_plastic', 'composition_rubber_leather',
    'composition_wood', 'composition_yard_garden_green', 'composition_other'
]
COMPOSITION_SUM_TOLERANCE_MIN = 95.0
COMPOSITION_SUM_TOLERANCE_MAX = 105.0
CURRENT_YEAR = pd.Timestamp.now().year
RECENCY_THRESHOLD_YEARS = 5
# --- Fin Constantes ---

# --- Funciones Auxiliares ---
def create_connection(db_file: Path) -> Optional[sqlite3.Connection]:
    # (Sin cambios)
    conn = None
    try:
        print_info(f"Conectando a la base de datos: {db_file}")
        if not db_file.exists():
            print_error(f"El archivo de base de datos '{db_file}' no existe.")
            return None
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        print_success("Conexión establecida.")
        return conn
    except sqlite3.Error as e:
        print_error(f"Error al conectar a la base de datos: {e}")
    return None

def add_quality_score_column(conn: sqlite3.Connection, table_name: str):
    # (Sin cambios)
    try:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [row['name'] for row in cursor.fetchall()]
        if 'data_quality_score' not in columns:
            print_info(f"Añadiendo columna 'data_quality_score' a la tabla '{table_name}'...")
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN data_quality_score REAL")
            print_success(f"Columna 'data_quality_score' añadida a '{table_name}'.")
        else: print_info(f"Columna 'data_quality_score' ya existe en '{table_name}'.")
        if 'score_details_json' not in columns:
            print_info(f"Añadiendo columna 'score_details_json' a la tabla '{table_name}'...")
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN score_details_json TEXT")
            print_success(f"Columna 'score_details_json' añadida a '{table_name}'.")
        else: print_info(f"Columna 'score_details_json' ya existe en '{table_name}'.")
        conn.commit()
    except sqlite3.Error as e:
        print_error(f"Error al añadir columnas de calidad a '{table_name}': {e}")
        conn.rollback()

def score_field_status(value: Any, status: Optional[str]) -> Tuple[float, str]:
    # (Sin cambios)
    score = 0.0; weight_status = 0.0; status_description = "missing"
    if pd.isna(value) or (isinstance(value, str) and not value.strip()): return 0.0, "missing"
    status_description = "original"; score = POINTS["field_present_original"]; weight_status = WEIGHTS["status_original"]
    if status:
        status_lower = status.lower()
        if status_lower == 'original': pass
        elif 'imputed' in status_lower: score = POINTS["field_present_imputed"]; weight_status = WEIGHTS["status_imputed"]; status_description = "imputed"
        elif 'recalculated' in status_lower: score = POINTS["field_present_recalculated"]; weight_status = WEIGHTS["status_recalculated"]; status_description = "recalculated"
        elif 'extrapolated' in status_lower: score = POINTS["field_present_extrapolated"]; weight_status = WEIGHTS["status_extrapolated"]; status_description = "extrapolated"
        elif status_lower in ['missing_unimputed', 'invalid_format', 'missing_column', 'failed_missing_specifics']: return 0.0, status_lower
        else: score = POINTS["field_present_imputed"] * 0.5; weight_status = WEIGHTS["status_imputed"] * 0.5; status_description = "present_unknown_status"
    return score * weight_status, status_description

def calculate_country_quality_score(country_row: sqlite3.Row, conn: sqlite3.Connection) -> Tuple[float, Dict]:
    """Calcula el puntaje de calidad para un país (asumiendo metodología WB)."""
    total_score = 0.0
    max_potential_score = 0.01
    score_details = {"methodology": "WB"}
    summary_lines = ["Resumen Cálculo Puntaje (País):"]

    # 1. Completitud y Estado de Campos Core (WB)
    current_max_field_score_core = 0
    completeness_score_sum = 0
    core_fields_summary = []
    for field in COUNTRY_CORE_FIELDS_WB:
        max_field_score = MAX_POSSIBLE_SCORE_PER_FIELD_ORIGINAL * WEIGHTS["completeness_core"]
        current_max_field_score_core += max_field_score
        status_field = f"{field}_status"
        value = country_row[field] if field in country_row.keys() else None
        status = country_row[status_field] if status_field in country_row.keys() else None
        field_score, field_status_desc = score_field_status(value, status)
        weighted_field_score = field_score * WEIGHTS["completeness_core"]
        total_score += weighted_field_score
        completeness_score_sum += weighted_field_score # Sumar para el resumen
        score_details[f"{field}_score"] = round(weighted_field_score, 2)
        score_details[f"{field}_status"] = field_status_desc
        if field_status_desc != 'original':
             core_fields_summary.append(f"{field}: {field_status_desc}")
    max_potential_score += current_max_field_score_core
    summary_lines.append(f"- Completitud Core ({len(COUNTRY_CORE_FIELDS_WB)} campos): {round(completeness_score_sum, 1)} pts.")
    if core_fields_summary:
        summary_lines.append(f"  (Detalles: {'; '.join(core_fields_summary)})")


    # 2. Calidad de Geolocalización
    max_potential_score += MAX_POSSIBLE_SCORE_GEO * 2
    geo_lat_score = 0; geo_lon_score = 0; geo_status_detail = "missing"
    if 'latitude_geo_status' in country_row.keys() and country_row['latitude_geo_status'] and country_row['latitude_geo_status'].lower() == 'found':
        geo_lat_score = POINTS["geocoding_found"] * WEIGHTS["geocoding_found"]; geo_status_detail = "found"
    if 'longitude_geo_status' in country_row.keys() and country_row['longitude_geo_status'] and country_row['longitude_geo_status'].lower() == 'found':
        geo_lon_score = POINTS["geocoding_found"] * WEIGHTS["geocoding_found"]; geo_status_detail = "found"
    geocoding_total_score = geo_lat_score + geo_lon_score
    total_score += geocoding_total_score
    score_details["geocoding_score"] = round(geocoding_total_score, 2)
    score_details["geocoding_status"] = geo_status_detail
    summary_lines.append(f"- Geolocalización ({geo_status_detail}): {round(geocoding_total_score, 1)} pts.")

    # 3. Consistencia de Composición
    max_potential_score += MAX_POSSIBLE_SCORE_COMPOSITION
    composition_sum = 0; valid_numeric_composition_fields = 0; composition_fields_present_count = 0
    comp_score_value = 0; comp_status_detail = "incomplete"
    for field in COMPOSITION_FIELDS:
        if field in country_row.keys() and pd.notna(country_row[field]):
            composition_fields_present_count += 1
            try: composition_sum += float(country_row[field]); valid_numeric_composition_fields +=1
            except (ValueError, TypeError): pass
    if valid_numeric_composition_fields == len(COMPOSITION_FIELDS):
        if COMPOSITION_SUM_TOLERANCE_MIN <= composition_sum <= COMPOSITION_SUM_TOLERANCE_MAX:
            comp_score_value = POINTS["composition_consistent_tolerant"]; comp_status_detail = "consistent_tolerant"
            if abs(composition_sum - 100.0) < 0.1: comp_score_value = POINTS["composition_consistent_strict"]; comp_status_detail = "consistent_strict"
        else: comp_status_detail = "inconsistent_sum"
    weighted_comp_score = comp_score_value * WEIGHTS["composition_consistency"]
    total_score += weighted_comp_score
    score_details["composition_score"] = round(weighted_comp_score, 2)
    score_details["composition_status"] = comp_status_detail
    score_details["composition_sum_actual"] = round(composition_sum, 2) if valid_numeric_composition_fields > 0 else "N/A"
    score_details["composition_fields_present"] = f"{valid_numeric_composition_fields}/{len(COMPOSITION_FIELDS)}"
    summary_lines.append(f"- Composición ({comp_status_detail}, Suma: {score_details['composition_sum_actual']}, Presentes: {score_details['composition_fields_present']}): {round(weighted_comp_score, 1)} pts.")

    # 4. Mediciones (country_measurements) - Contando únicas
    max_potential_score += MAX_POSSIBLE_SCORE_MEASUREMENTS_AVAIL
    max_potential_score += MAX_POSSIBLE_SCORE_MEASUREMENT_DETAILS
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT measurement, year, source, comments FROM country_measurements WHERE country_iso3c = ?", (country_row['country_code_iso3'],))
    unique_measurements = cursor.fetchall()
    num_distinct_measurements = len(unique_measurements)
    measurement_availability_score = min(num_distinct_measurements, MAX_EXPECTED_UNIQUE_MEASUREMENTS) * POINTS["measurement_available_unique"] * WEIGHTS["measurements_availability_unique"]
    total_score += measurement_availability_score
    score_details["measurements_unique_count"] = num_distinct_measurements
    score_details["measurements_availability_score"] = round(measurement_availability_score, 2)
    measurement_details_score = 0; recency_bonus_applied_this_entity = False
    num_with_year = 0; num_with_source = 0; num_with_comments = 0
    for m_row in unique_measurements:
        if m_row['year'] and pd.notna(m_row['year']):
            measurement_details_score += POINTS["measurement_has_year"] * WEIGHTS["measurements_recency"]; num_with_year += 1
            try:
                year_val = int(m_row['year'])
                if not recency_bonus_applied_this_entity and (CURRENT_YEAR - year_val) <= RECENCY_THRESHOLD_YEARS:
                    measurement_details_score += POINTS["measurement_recent_year_bonus"] * WEIGHTS["measurements_recency"]; recency_bonus_applied_this_entity = True
            except (ValueError, TypeError): pass
        if m_row['source'] and str(m_row['source']).strip():
            measurement_details_score += POINTS["measurement_has_source"] * WEIGHTS["measurements_details"]; num_with_source += 1
        if m_row['comments'] and str(m_row['comments']).strip():
            measurement_details_score += POINTS["measurement_has_comments"] * WEIGHTS["measurements_details"]; num_with_comments += 1
    total_score += measurement_details_score
    score_details["measurements_details_score"] = round(measurement_details_score, 2)
    score_details["measurements_recency_bonus_applied"] = recency_bonus_applied_this_entity
    score_details["measurements_with_year"] = num_with_year
    score_details["measurements_with_source"] = num_with_source
    score_details["measurements_with_comments"] = num_with_comments
    summary_lines.append(f"- Mediciones ({num_distinct_measurements} únicas): Disp={round(measurement_availability_score,1)}, Detalle/Rec={round(measurement_details_score,1)} pts.")

    # Normalización y Finalización
    normalized_score = (total_score / max_potential_score) * 100 if max_potential_score > 0 else 0
    final_score = round(min(max(normalized_score, 0), 100), 2)
    score_details["raw_score"] = round(total_score, 2)
    score_details["max_potential_score"] = round(max_potential_score, 2)
    score_details["final_normalized_score"] = final_score
    summary_lines.append(f"-> Puntaje Bruto: {score_details['raw_score']} / {score_details['max_potential_score']} => Normalizado: {final_score}")
    # --- INICIO: Añadir resumen a score_details ---
    score_details["calculation_summary"] = "\n".join(summary_lines)
    # --- FIN: Añadir resumen a score_details ---

    return final_score, score_details


def calculate_city_quality_score(city_row: sqlite3.Row, conn: sqlite3.Connection) -> Tuple[float, Dict]:
    """Calcula el puntaje de calidad para una ciudad, considerando la metodología."""
    total_score = 0.0
    max_potential_score = 0.01
    score_details = {}
    summary_lines = ["Resumen Cálculo Puntaje (Ciudad):"]

    methodology = city_row['data_source_methodology'] if 'data_source_methodology' in city_row.keys() else 'Unknown'
    score_details["methodology"] = methodology
    summary_lines.append(f" Metodología Detectada: {methodology}")

    if methodology == 'WFD': core_fields = CITY_CORE_FIELDS_WFD
    elif methodology == 'WB': core_fields = CITY_CORE_FIELDS_WB
    else: core_fields = CITY_CORE_FIELDS_WB; score_details["methodology"] = 'WB_or_Unknown'

    # 1. Completitud y Estado de Campos Core
    current_max_field_score_core = 0
    completeness_score_sum = 0
    core_fields_summary = []
    for field in core_fields:
        max_field_score = MAX_POSSIBLE_SCORE_PER_FIELD_ORIGINAL * WEIGHTS["completeness_core"]
        current_max_field_score_core += max_field_score
        status_field = f"{field}_status"
        value = city_row[field] if field in city_row.keys() else None
        status = city_row[status_field] if status_field in city_row.keys() else None
        field_score, field_status_desc = score_field_status(value, status)
        weighted_field_score = field_score * WEIGHTS["completeness_core"]
        total_score += weighted_field_score
        completeness_score_sum += weighted_field_score
        score_details[f"{field}_score"] = round(weighted_field_score, 2)
        score_details[f"{field}_status"] = field_status_desc
        if field_status_desc != 'original':
            core_fields_summary.append(f"{field}: {field_status_desc}")
    max_potential_score += current_max_field_score_core
    summary_lines.append(f"- Completitud Core ({len(core_fields)} campos, {methodology}): {round(completeness_score_sum, 1)} pts.")
    if core_fields_summary:
        summary_lines.append(f"  (Detalles: {'; '.join(core_fields_summary)})")

    # 2. Calidad de Geolocalización
    max_potential_score += MAX_POSSIBLE_SCORE_GEO * 2
    geo_lat_score = 0; geo_lon_score = 0; geo_status_detail = "missing"
    if 'latitude_geo_status' in city_row.keys() and city_row['latitude_geo_status'] and city_row['latitude_geo_status'].lower() == 'found':
        geo_lat_score = POINTS["geocoding_found"] * WEIGHTS["geocoding_found"]; geo_status_detail = "found"
    if 'longitude_geo_status' in city_row.keys() and city_row['longitude_geo_status'] and city_row['longitude_geo_status'].lower() == 'found':
        geo_lon_score = POINTS["geocoding_found"] * WEIGHTS["geocoding_found"]; geo_status_detail = "found"
    geocoding_total_score = geo_lat_score + geo_lon_score
    total_score += geocoding_total_score
    score_details["geocoding_score"] = round(geocoding_total_score, 2)
    score_details["geocoding_status"] = geo_status_detail
    summary_lines.append(f"- Geolocalización ({geo_status_detail}): {round(geocoding_total_score, 1)} pts.")

    # 3. Consistencia de Composición
    max_potential_score += MAX_POSSIBLE_SCORE_COMPOSITION
    composition_sum = 0; valid_numeric_composition_fields = 0; composition_fields_present_count = 0
    comp_score_value = 0; comp_status_detail = "incomplete"
    comp_calc_status = city_row['composition_calculation_status'] if 'composition_calculation_status' in city_row.keys() else None
    score_details["composition_calc_status_db"] = comp_calc_status if comp_calc_status else "N/A"
    for field in COMPOSITION_FIELDS:
        if field in city_row.keys() and pd.notna(city_row[field]):
            composition_fields_present_count +=1
            try: composition_sum += float(city_row[field]); valid_numeric_composition_fields +=1
            except (ValueError, TypeError): pass
    if comp_calc_status and comp_calc_status.lower() == 'recalculated':
        comp_score_value = POINTS["composition_consistent_strict"]; comp_status_detail = "recalculated_consistent"
    elif comp_calc_status and comp_calc_status.lower() == 'failed_missing_specifics':
        comp_score_value = 0; comp_status_detail = "recalculation_failed"
    elif valid_numeric_composition_fields == len(COMPOSITION_FIELDS):
        if COMPOSITION_SUM_TOLERANCE_MIN <= composition_sum <= COMPOSITION_SUM_TOLERANCE_MAX:
            comp_score_value = POINTS["composition_consistent_tolerant"]; comp_status_detail = "consistent_tolerant"
            if abs(composition_sum - 100.0) < 0.1: comp_score_value = POINTS["composition_consistent_strict"]; comp_status_detail = "consistent_strict"
        else: comp_status_detail = "inconsistent_sum"
    weighted_comp_score = comp_score_value * WEIGHTS["composition_consistency"]
    total_score += weighted_comp_score
    score_details["composition_score"] = round(weighted_comp_score, 2)
    score_details["composition_status"] = comp_status_detail
    score_details["composition_sum_actual"] = round(composition_sum, 2) if valid_numeric_composition_fields > 0 else "N/A"
    score_details["composition_fields_present"] = f"{valid_numeric_composition_fields}/{len(COMPOSITION_FIELDS)}"
    summary_lines.append(f"- Composición ({comp_status_detail}, Suma: {score_details['composition_sum_actual']}, Presentes: {score_details['composition_fields_present']}, CalcStat: {score_details['composition_calc_status_db']}): {round(weighted_comp_score, 1)} pts.")

    # 4. Mediciones (city_measurements) - Contando únicas
    max_potential_score += MAX_POSSIBLE_SCORE_MEASUREMENTS_AVAIL
    max_potential_score += MAX_POSSIBLE_SCORE_MEASUREMENT_DETAILS
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT measurement, year, source, comments FROM city_measurements WHERE city_id = ?", (city_row['id'],))
    unique_measurements = cursor.fetchall()
    num_distinct_measurements = len(unique_measurements)
    core_completeness_ratio = (completeness_score_sum / current_max_field_score_core) if current_max_field_score_core > 0 else 0
    availability_weight_factor = max(0.2, core_completeness_ratio)
    measurement_availability_score = min(num_distinct_measurements, MAX_EXPECTED_UNIQUE_MEASUREMENTS) * POINTS["measurement_available_unique"] * WEIGHTS["measurements_availability_unique"] * availability_weight_factor
    total_score += measurement_availability_score
    score_details["measurements_unique_count"] = num_distinct_measurements
    score_details["measurements_availability_score"] = round(measurement_availability_score, 2)
    score_details["measurements_availability_weight_factor"] = round(availability_weight_factor, 2)
    measurement_details_score = 0; recency_bonus_applied_this_entity = False
    num_with_year = 0; num_with_source = 0; num_with_comments = 0
    for m_row in unique_measurements:
        if m_row['year'] and pd.notna(m_row['year']):
            measurement_details_score += POINTS["measurement_has_year"] * WEIGHTS["measurements_recency"]; num_with_year += 1
            try:
                year_val = int(m_row['year'])
                if not recency_bonus_applied_this_entity and (CURRENT_YEAR - year_val) <= RECENCY_THRESHOLD_YEARS:
                    measurement_details_score += POINTS["measurement_recent_year_bonus"] * WEIGHTS["measurements_recency"]; recency_bonus_applied_this_entity = True
            except (ValueError, TypeError): pass
        if m_row['source'] and str(m_row['source']).strip():
            measurement_details_score += POINTS["measurement_has_source"] * WEIGHTS["measurements_details"]; num_with_source += 1
        if m_row['comments'] and str(m_row['comments']).strip():
            measurement_details_score += POINTS["measurement_has_comments"] * WEIGHTS["measurements_details"]; num_with_comments += 1
    total_score += measurement_details_score
    score_details["measurements_details_score"] = round(measurement_details_score, 2)
    score_details["measurements_recency_bonus_applied"] = recency_bonus_applied_this_entity
    score_details["measurements_with_year"] = num_with_year
    score_details["measurements_with_source"] = num_with_source
    score_details["measurements_with_comments"] = num_with_comments
    summary_lines.append(f"- Mediciones ({num_distinct_measurements} únicas): Disp={round(measurement_availability_score,1)} (WFactor:{availability_weight_factor:.2f}), Detalle/Rec={round(measurement_details_score,1)} pts.")

    # Normalización y Finalización
    normalized_score = (total_score / max_potential_score) * 100 if max_potential_score > 0 else 0
    final_score = round(min(max(normalized_score, 0), 100), 2)
    score_details["raw_score"] = round(total_score, 2)
    score_details["max_potential_score"] = round(max_potential_score, 2)
    score_details["final_normalized_score"] = final_score
    summary_lines.append(f"-> Puntaje Bruto: {score_details['raw_score']} / {score_details['max_potential_score']} => Normalizado: {final_score}")
    # --- INICIO: Añadir resumen a score_details ---
    score_details["calculation_summary"] = "\n".join(summary_lines)
    # --- FIN: Añadir resumen a score_details ---

    return final_score, score_details

def update_scores_in_db(conn: sqlite3.Connection, table_name: str, pk_column: str, scores_data: List[Dict]):
    # (Sin cambios)
    print_info(f"Actualizando puntajes de calidad en la tabla '{table_name}'...")
    try:
        cursor = conn.cursor()
        updates = []
        for item in scores_data:
            updates.append((item['score'], item['details_json'], item[pk_column]))
        cursor.executemany(f"UPDATE {table_name} SET data_quality_score = ?, score_details_json = ? WHERE {pk_column} = ?", updates)
        conn.commit()
        print_success(f"{cursor.rowcount} registros actualizados en '{table_name}'.")
    except sqlite3.Error as e:
        print_error(f"Error al actualizar puntajes en '{table_name}': {e}")
        conn.rollback()

def save_rankings_to_json(data: List[Dict], output_path: Path):
    """Guarda los datos rankeados en un archivo JSON, incluyendo el score_details_json completo."""
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        # Decodificar el JSON de detalles para guardarlo como objeto, no como string
        ranked_data_with_details = []
        for item in data:
            new_item = item.copy() # Copiar para no modificar el original
            try:
                # Intentar decodificar el JSON string de detalles
                new_item['score_details'] = json.loads(item['details_json'])
            except (json.JSONDecodeError, TypeError):
                 # Si falla, guardar el string original o un mensaje de error
                 new_item['score_details'] = {"error": "Could not decode details JSON", "original_string": item.get('details_json')}
            del new_item['details_json'] # Eliminar la versión string
            ranked_data_with_details.append(new_item)

        with open(output_path, 'w', encoding='utf-8') as f:
            # Guardar la lista completa con los detalles decodificados
            json.dump(ranked_data_with_details, f, indent=2, ensure_ascii=False)
        print_success(f"Ranking guardado (con detalles) en: {output_path}")
    except IOError as e:
        print_error(f"No se pudo escribir el archivo JSON de ranking: {output_path}. Error: {e}")
    except Exception as e:
        print_error(f"Error inesperado guardando ranking JSON: {e}")


def main():
    parser = argparse.ArgumentParser(description="SFA: Analizador de Calidad de Datos y Generador de Rankings (v3 - con Resumen).")
    parser.add_argument("--db-file", default="output/waste_data.db", help="Ruta al archivo de la base de datos SQLite.")
    parser.add_argument("--output-dir", default="quality_analysis", help="Directorio para guardar los rankings y reportes de calidad.")
    args = parser.parse_args()

    db_path = Path(args.db_file)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = create_connection(db_path)
    if conn is None: sys.exit(1)

    add_quality_score_column(conn, "countries")
    add_quality_score_column(conn, "cities")

    # --- Procesar Países ---
    print_heading("Procesando Calidad de Datos para Países")
    cursor = conn.cursor()
    try: cursor.execute("SELECT * FROM countries") ; countries_data = cursor.fetchall()
    except sqlite3.Error as e: print_error(f"Error al leer 'countries': {e}"); conn.close(); sys.exit(1)
    country_scores = []
    country_iterator = track(countries_data, description="Analizando países...") if USE_RICH else countries_data
    for country_row in country_iterator:
        score, details = calculate_country_quality_score(country_row, conn)
        country_scores.append({
            "country_code_iso3": country_row['country_code_iso3'],
            "country_name": country_row['country_name'],
            "score": score,
            "details_json": json.dumps(details) # Guardar detalles completos (incluye resumen)
        })
    if country_scores:
        update_scores_in_db(conn, "countries", "country_code_iso3", country_scores)
        ranked_countries = sorted(country_scores, key=lambda x: x['score'], reverse=True)
        save_rankings_to_json(ranked_countries, output_dir / "ranked_countries_quality.json")
    else: print_warning("No se procesaron puntajes para países.")

    # --- Procesar Ciudades ---
    print_heading("Procesando Calidad de Datos para Ciudades")
    try: cursor.execute("SELECT * FROM cities"); cities_data = cursor.fetchall()
    except sqlite3.Error as e: print_error(f"Error al leer 'cities': {e}"); conn.close(); sys.exit(1)
    city_scores = []
    city_iterator = track(cities_data, description="Analizando ciudades...") if USE_RICH else cities_data
    for city_row in city_iterator:
        score, details = calculate_city_quality_score(city_row, conn)
        city_scores.append({
            "id": city_row['id'],
            "municipality": city_row['municipality'],
            "iso3c": city_row['iso3c'],
            "methodology": details.get("methodology", "Unknown"),
            "score": score,
            "details_json": json.dumps(details) # Guardar detalles completos (incluye resumen)
        })
    if city_scores:
        update_scores_in_db(conn, "cities", "id", city_scores)
        ranked_cities = sorted(city_scores, key=lambda x: x['score'], reverse=True)
        save_rankings_to_json(ranked_cities, output_dir / "ranked_cities_quality.json")
    else: print_warning("No se procesaron puntajes para ciudades.")

    conn.close()
    print_info(f"Análisis de calidad (v3) completado. Resultados en: {output_dir.resolve()}")

if __name__ == "__main__":
    main()
