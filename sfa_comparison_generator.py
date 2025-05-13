#!/usr/bin/env python3

# /// script
# dependencies = [
#   "pandas>=2.0.0",
#   "rich>=13.7.0",
#   "numpy>=1.20.0"
# ]
# ///

"""
Single File Agent (SFA) 5: Comparison Generator (v2 - Quantile-based Clustering)

Reads data from the waste_data.db SQLite database, performs quantile-based clustering
on countries and cities based on multiple descriptive features, and outputs the
results to a JSON file.
"""

import pandas as pd
import numpy as np
import json
import sqlite3
from pathlib import Path
import argparse
import sys
from datetime import datetime, timezone
from typing import Union, Dict, Any, List, Tuple

# --- Rich Console Setup ---
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    console = Console(stderr=True)
    USE_RICH = True
    def print_info(message): console.print(f"[cyan]ℹ️ {message}[/cyan]")
    def print_success(message): console.print(f"[green]✅ {message}[/green]")
    def print_warning(message): console.print(f"[yellow]⚠️ {message}[/yellow]")
    def print_error(message): console.print(Panel(f"[bold red]❌ ERROR:[/bold red]\n{message}", border_style="red"))
    def print_heading(title): console.print(f"\n[bold underline magenta]{title}[/bold underline magenta]\n")
except ImportError:
    USE_RICH = False
    def print_info(message): print(f"INFO: {message}", file=sys.stderr)
    def print_success(message): print(f"SUCCESS: {message}", file=sys.stderr)
    def print_warning(message): print(f"WARNING: {message}", file=sys.stderr)
    def print_error(message): print(f"ERROR: {message}", file=sys.stderr)
    def print_heading(title): print(f"\n--- {title} ---\n", file=sys.stderr)
# --- End Rich Console Setup ---

# --- Configuration ---
DEFAULT_DB_PATH = "output/waste_data.db" # Default path to the SQLite database
DEFAULT_OUTPUT_JSON = "output/quality_analysis/entity_clusters.json" # Default output file path

# Features for clustering and their corresponding column names in the database
# For countries
COUNTRY_FEATURES_CONFIG = {
    "data_quality_score": {"db_col": "data_quality_score", "prefix": "DQ"},
    "population": {"db_col": "population_total", "prefix": "Pop"},
    "total_waste_tons_year": {"db_col": "total_msw_generated_tons_year", "prefix": "TotWaste"},
    "waste_per_capita_kg_day": {"db_col": "waste_per_capita_kg_day", "prefix": "WPC"}, # Calculated
    "collection_coverage_percent": {"db_col": "waste_collection_coverage_total_percent_of_population", "prefix": "CollCover"},
    "recycling_rate_percent": {"db_col": "waste_treatment_recycling_percent", "prefix": "RecRate"},
    "composition_food_organic_percent": {"db_col": "composition_food_organic", "prefix": "OrgComp"}
}

# For cities
CITY_FEATURES_CONFIG = {
    "data_quality_score": {"db_col": "data_quality_score", "prefix": "DQ"},
    "population": {"db_col": "population", "prefix": "Pop"},
    "total_waste_tons_year": {"db_col": "total_waste_tons_year", "prefix": "TotWaste"},
    "waste_per_capita_kg_day": {"db_col": "waste_per_capita_kg_day", "prefix": "WPC"}, # Calculated
    "collection_coverage_percent": {"db_col": "collection_coverage_population_percent", "prefix": "CollCover"},
    "recycling_rate_percent": {"db_col": "recycling_rate_percent", "prefix": "RecRate"},
    "composition_food_organic_percent": {"db_col": "composition_food_organic", "prefix": "OrgComp"}
}

# Quantile definitions: 5 groups (quintiles)
QUANTILES = [0, 0.2, 0.4, 0.6, 0.8, 1.0]
QUANTILE_LABELS = ["Very Low", "Low", "Medium", "High", "Very High"]
# Short labels for cluster name generation
QUANTILE_SHORT_LABELS = ["VL", "L", "M", "H", "VH"]


def create_db_connection(db_path_str: str) -> sqlite3.Connection:
    """Creates a connection to the SQLite database."""
    db_path = Path(db_path_str)
    print_info(f"Connecting to database: {db_path}")
    if not db_path.exists():
        print_error(f"Database file not found: {db_path}")
        sys.exit(1)
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row # Access columns by name
        print_success("Successfully connected to the database.")
        return conn
    except sqlite3.Error as e:
        print_error(f"Error connecting to database: {e}")
        sys.exit(1)

def load_data_from_db(conn: sqlite3.Connection) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Loads country and city data from the database and calculates per capita waste."""
    print_info("Loading data from database...")
    try:
        df_countries = pd.read_sql_query("SELECT * FROM countries", conn)
        df_cities = pd.read_sql_query("SELECT * FROM cities", conn)
        print_success(f"Loaded {len(df_countries)} countries and {len(df_cities)} cities.")

        # Calculate waste_per_capita_kg_day for countries
        if 'population_total' in df_countries.columns and 'total_msw_generated_tons_year' in df_countries.columns:
            pop = pd.to_numeric(df_countries['population_total'], errors='coerce')
            waste = pd.to_numeric(df_countries['total_msw_generated_tons_year'], errors='coerce')
            valid_mask = (pop > 0) & pop.notna() & waste.notna()
            df_countries['waste_per_capita_kg_day'] = np.nan
            df_countries.loc[valid_mask, 'waste_per_capita_kg_day'] = (waste[valid_mask] * 1000) / (pop[valid_mask] * 365)
        else:
            df_countries['waste_per_capita_kg_day'] = np.nan
            print_warning("Could not calculate waste_per_capita_kg_day for countries due to missing columns.")

        # Calculate waste_per_capita_kg_day for cities
        if 'population' in df_cities.columns and 'total_waste_tons_year' in df_cities.columns:
            pop_city = pd.to_numeric(df_cities['population'], errors='coerce')
            waste_city = pd.to_numeric(df_cities['total_waste_tons_year'], errors='coerce')
            valid_mask_city = (pop_city > 0) & pop_city.notna() & waste_city.notna()
            df_cities['waste_per_capita_kg_day'] = np.nan
            df_cities.loc[valid_mask_city, 'waste_per_capita_kg_day'] = (waste_city[valid_mask_city] * 1000) / (pop_city[valid_mask_city] * 365)
        elif 'waste_gen_rate_kg_cap_day' in df_cities.columns: # Fallback for WFD-like data
            df_cities['waste_per_capita_kg_day'] = pd.to_numeric(df_cities['waste_gen_rate_kg_cap_day'], errors='coerce')
            print_info("Used 'waste_gen_rate_kg_cap_day' for city waste per capita.")
        else:
            df_cities['waste_per_capita_kg_day'] = np.nan
            print_warning("Could not calculate waste_per_capita_kg_day for cities due to missing columns.")

        return df_countries, df_cities

    except pd.io.sql.DatabaseError as e:
        print_error(f"Database error loading data: {e}. Check table names and columns.")
        sys.exit(1)
    except Exception as e:
        print_error(f"An unexpected error occurred while loading data: {e}")
        sys.exit(1)

def assign_quantile_clusters(df: pd.DataFrame, features_config: Dict[str, Dict[str, str]], entity_type_label: str) -> pd.DataFrame:
    """
    Assigns entities to clusters based on quantiles of specified features.
    """
    print_info(f"Assigning quantile-based clusters for {entity_type_label}...")
    if df.empty:
        print_warning(f"DataFrame for {entity_type_label} is empty. Skipping clustering.")
        return df

    df_clustered = df.copy()
    cluster_name_parts_series_list = [] # Changed variable name for clarity
    feature_quantile_cols = {}

    for feature_key, config in features_config.items():
        db_col = config["db_col"]
        prefix = config["prefix"]
        quantile_col_name = f"{feature_key}_quantile_label"
        feature_quantile_cols[feature_key] = quantile_col_name # Store for later output

        if db_col not in df_clustered.columns:
            print_warning(f"Feature column '{db_col}' for '{feature_key}' not found in {entity_type_label} data. Skipping this feature.")
            df_clustered[quantile_col_name] = "N/A" # Mark as Not Available
            # Add a series of "prefix:NA" for this feature
            cluster_name_parts_series_list.append(pd.Series([f"{prefix}:NA"] * len(df_clustered), index=df_clustered.index))
            continue

        # Ensure column is numeric, coercing errors to NaN
        numeric_series = pd.to_numeric(df_clustered[db_col], errors='coerce')
        
        # Handle cases where all values are NaN after coercion
        if numeric_series.isna().all():
            print_warning(f"All values for feature '{db_col}' are NaN or non-numeric. Assigning 'N/A' for quantiles.")
            df_clustered[quantile_col_name] = "N/A"
            cluster_name_parts_series_list.append(pd.Series([f"{prefix}:NA"] * len(df_clustered), index=df_clustered.index))
            continue

        try:
            # Use pd.qcut for quantile-based discretization.
            df_clustered[quantile_col_name] = pd.qcut(
                numeric_series.rank(method='first'), # Rank to handle duplicates better
                q=QUANTILES,
                labels=QUANTILE_LABELS,
                duplicates='drop'
            )
            df_clustered[quantile_col_name] = df_clustered[quantile_col_name].cat.add_categories("N/A").fillna("N/A")

            label_to_short_label_map = dict(zip(QUANTILE_LABELS, QUANTILE_SHORT_LABELS))
            label_to_short_label_map["N/A"] = "NA" 

            short_labels_series = df_clustered[quantile_col_name].map(label_to_short_label_map)
            cluster_name_parts_series_list.append(short_labels_series.apply(lambda x: f"{prefix}:{x}"))

        except ValueError as e:
            print_warning(f"Could not assign quantiles for '{db_col}' due to data distribution: {e}. Assigning 'N/A'.")
            df_clustered[quantile_col_name] = "N/A"
            cluster_name_parts_series_list.append(pd.Series([f"{prefix}:NA"] * len(df_clustered), index=df_clustered.index))
        except Exception as e_gen:
            print_error(f"Unexpected error assigning quantiles for '{db_col}': {e_gen}")
            df_clustered[quantile_col_name] = "N/A"
            cluster_name_parts_series_list.append(pd.Series([f"{prefix}:NA"] * len(df_clustered), index=df_clustered.index))


    # Combine parts into a single cluster name
    if cluster_name_parts_series_list:
        # Concatenate the list of Series into a DataFrame
        cluster_parts_df = pd.concat(cluster_name_parts_series_list, axis=1)
        # Aggregate row-wise by joining strings
        df_clustered['cluster_name'] = cluster_parts_df.agg('_'.join, axis=1)
    else:
        df_clustered['cluster_name'] = "NoFeaturesClustered"

    unique_cluster_names = df_clustered['cluster_name'].unique()
    cluster_id_map = {name: f"{entity_type_label.lower()}_cluster_{i+1}" for i, name in enumerate(unique_cluster_names)}
    df_clustered['cluster_id'] = df_clustered['cluster_name'].map(cluster_id_map)

    df_clustered['_feature_quantile_cols_map'] = pd.Series([feature_quantile_cols] * len(df_clustered), index=df_clustered.index)


    print_success(f"Clustering for {entity_type_label} completed.")
    return df_clustered


def main():
    parser = argparse.ArgumentParser(description="SFA5 v2: Generates entity clusters based on waste management characteristics using quantile analysis.")
    parser.add_argument("--db-file", default=DEFAULT_DB_PATH, help=f"Path to the SQLite database file (default: {DEFAULT_DB_PATH}).")
    parser.add_argument("--output-json", default=DEFAULT_OUTPUT_JSON, help=f"Path to save the output JSON file (default: {DEFAULT_OUTPUT_JSON}).")
    args = parser.parse_args()

    print_heading("SFA5: Advanced Comparison & Grouping - Quantile Clustering (v2)")

    conn = create_db_connection(args.db_file)
    df_countries, df_cities = load_data_from_db(conn)
    conn.close() 

    df_countries_clustered = assign_quantile_clusters(df_countries, COUNTRY_FEATURES_CONFIG, "Country")
    df_cities_clustered = assign_quantile_clusters(df_cities, CITY_FEATURES_CONFIG, "City")

    output_data = {
        "metadata": {
            "description": "Entity clusters based on quantile analysis of waste management characteristics.",
            "sfa5_version": "2.0_quantile_based_fix", # Updated version
            "generation_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "country_features_used": list(COUNTRY_FEATURES_CONFIG.keys()),
            "city_features_used": list(CITY_FEATURES_CONFIG.keys()),
            "quantile_method": f"{len(QUANTILE_LABELS)}-group quantiles ({', '.join(QUANTILE_LABELS)})"
        },
        "country_clusters": [],
        "city_clusters": []
    }

    if not df_countries_clustered.empty:
        # Ensure the _feature_quantile_cols_map is correctly accessed if it was added as a single Series of dicts
        # If it was added as a column where each row has the same dict, then direct access is fine.
        # The current implementation adds it as a Series where each element is the same dict.
        # So, row.get('_feature_quantile_cols_map') should correctly fetch that dict.

        for _, row in df_countries_clustered.iterrows():
            # Get the map for the current row (it's the same map for all rows of this entity type)
            # If _feature_quantile_cols_map was not created (e.g. df_clustered was empty initially), provide a default
            feature_quantiles_map_for_row = row.get('_feature_quantile_cols_map', COUNTRY_FEATURES_CONFIG)


            output_data["country_clusters"].append({
                "entity_id": row.get("country_code_iso3"),
                "entity_name": row.get("country_name"),
                "entity_type": "country",
                "cluster_id": row.get("cluster_id"),
                "cluster_name": row.get("cluster_name"),
                "feature_quantiles": {
                    feature_key: row.get(quantile_col_name, "N/A")
                    # Use the stored map to get the correct quantile_col_name for each feature_key
                    for feature_key, quantile_col_name in feature_quantiles_map_for_row.items()
                }
            })

    if not df_cities_clustered.empty:
        for _, row in df_cities_clustered.iterrows():
            feature_quantiles_map_for_row = row.get('_feature_quantile_cols_map', CITY_FEATURES_CONFIG)

            output_data["city_clusters"].append({
                "entity_id": row.get("id"), 
                "entity_name": row.get("municipality"),
                "country_iso3": row.get("iso3c"),
                "entity_type": "city",
                "cluster_id": row.get("cluster_id"),
                "cluster_name": row.get("cluster_name"),
                "feature_quantiles": {
                    feature_key: row.get(quantile_col_name, "N/A")
                    for feature_key, quantile_col_name in feature_quantiles_map_for_row.items()
                }
            })

    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        print_success(f"Clustering results saved to: {output_path}")
    except IOError as e:
        print_error(f"Error writing output JSON to {output_path}: {e}")
    except Exception as e:
        print_error(f"An unexpected error occurred while saving JSON: {e}")

    print_heading("SFA5 Processing Complete.")

if __name__ == "__main__":
    main()
