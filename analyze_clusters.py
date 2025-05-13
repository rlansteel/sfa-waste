#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# /// script
# dependencies = [
#   "pandas>=2.0.0",
#   "rich>=13.7.0"
# ]
# ///

"""
Script para analizar el output de sfa_comparison_generator.py (clusters.json).
Realiza las siguientes tareas:
1. Analiza la distribución del tamaño de los clústeres.
2. Identifica e interpreta los cluster_name más comunes.
3. Identifica qué características resultan más frecuentemente en "N/A".
"""

import json
import pandas as pd
from pathlib import Path
import argparse
from collections import Counter

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
    # Fallback para Table si Rich no está
    class Table:
        def __init__(self, title="", show_header=True, header_style=""): self.title = title; self.headers = []; self.rows = []; self.show_header=show_header
        def add_column(self, header, style="", justify="left", width=None): self.headers.append(header)
        def add_row(self, *args): self.rows.append(list(args))
        def add_section(self): pass # No-op
        def __str__(self):
            output = []
            if self.title: output.append(self.title)
            if self.show_header and self.headers: output.append(" | ".join(map(str, self.headers)))
            for row in self.rows: output.append(" | ".join(map(str, row)))
            return "\n".join(output)
    def console_print_table(table_obj): print(str(table_obj)) # Redefinir para que use el __str__ de la clase Table mock
# --- Fin Configuración Visual ---

def load_cluster_data(json_file_path: str) -> dict:
    """Carga los datos del archivo JSON de clústeres."""
    print_info(f"Cargando datos de clústeres desde: {json_file_path}")
    path = Path(json_file_path)
    if not path.is_file():
        print_error(f"El archivo JSON no se encontró en: {json_file_path}")
        sys.exit(1)
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print_success(f"Datos de clústeres cargados exitosamente. {len(data.get('country_clusters',[]))} países, {len(data.get('city_clusters',[]))} ciudades.")
        return data
    except json.JSONDecodeError as e:
        print_error(f"Error al decodificar JSON: {e}")
        sys.exit(1)
    except Exception as e:
        print_error(f"Error inesperado al cargar los datos: {e}")
        sys.exit(1)

def analyze_cluster_size_distribution(cluster_data: list, entity_type: str):
    """Analiza y muestra la distribución del tamaño de los clústeres."""
    print_heading(f"Distribución del Tamaño de Clústeres para: {entity_type.capitalize()}", level=2)
    if not cluster_data:
        print_warning(f"No hay datos de clústeres para {entity_type}.")
        return

    cluster_names = [item.get('cluster_name', 'N/A_ClusterName') for item in cluster_data]
    cluster_counts = Counter(cluster_names)

    if USE_RICH:
        table = Table(title=f"Tamaño de Clústeres ({entity_type.capitalize()})", show_header=True, header_style="bold magenta")
        table.add_column("Cluster Name", style="cyan", min_width=50, overflow="fold")
        table.add_column("Número de Entidades", style="green", justify="right")

        # Ordenar por número de entidades, descendente
        for name, count in cluster_counts.most_common():
            table.add_row(name, str(count))
        console.print(table)
    else:
        print(f"\nTamaño de Clústeres ({entity_type.capitalize()}):")
        for name, count in cluster_counts.most_common():
            print(f"  {name}: {count} entidades")
    
    return cluster_counts

def interpret_common_clusters(cluster_counts: Counter, entity_type: str, top_n: int = 5):
    """Muestra los cluster_name más comunes para interpretación."""
    print_heading(f"Clústeres Más Comunes ({entity_type.capitalize()}) - Top {top_n}", level=2)
    if not cluster_counts:
        print_warning(f"No hay conteos de clústeres para interpretar para {entity_type}.")
        return

    if USE_RICH:
        table = Table(title=f"Top {top_n} Clústeres Más Comunes ({entity_type.capitalize()})", show_header=True, header_style="bold magenta")
        table.add_column("Ranking", style="dim", justify="right")
        table.add_column("Cluster Name", style="cyan", min_width=50, overflow="fold")
        table.add_column("Número de Entidades", style="green", justify="right")

        for i, (name, count) in enumerate(cluster_counts.most_common(top_n)):
            table.add_row(str(i + 1), name, str(count))
        console.print(table)
        print_info(f"Interpretar estos 'Cluster Name' implica analizar las etiquetas de cuantiles (ej. DQ:H, Pop:M) que los componen.")
    else:
        print(f"\nTop {top_n} Clústeres Más Comunes ({entity_type.capitalize()}):")
        for i, (name, count) in enumerate(cluster_counts.most_common(top_n)):
            print(f"  {i+1}. {name}: {count} entidades")
        print(f"Interpretar estos 'Cluster Name' implica analizar las etiquetas de cuantiles (ej. DQ:H, Pop:M) que los componen.")


def analyze_na_features(cluster_data: list, features_used: list, entity_type: str):
    """Identifica qué características resultan más frecuentemente en N/A."""
    print_heading(f"Frecuencia de 'N/A' por Característica para: {entity_type.capitalize()}", level=2)
    if not cluster_data:
        print_warning(f"No hay datos de clústeres para analizar N/A para {entity_type}.")
        return

    na_counts = Counter()
    total_entities = len(cluster_data)

    for item in cluster_data:
        feature_quantiles = item.get('feature_quantiles', {})
        for feature in features_used:
            if feature_quantiles.get(feature) == "N/A":
                na_counts[feature] += 1
    
    if USE_RICH:
        table = Table(title=f"Características con 'N/A' ({entity_type.capitalize()})", show_header=True, header_style="bold magenta")
        table.add_column("Característica", style="cyan")
        table.add_column("Número de 'N/A'", style="yellow", justify="right")
        table.add_column("Porcentaje de 'N/A'", style="yellow", justify="right")

        # Ordenar por número de N/A, descendente
        for feature, count in na_counts.most_common():
            percentage = (count / total_entities) * 100 if total_entities > 0 else 0
            table.add_row(feature, str(count), f"{percentage:.1f}%")
        console.print(table)
    else:
        print(f"\nCaracterísticas con 'N/A' ({entity_type.capitalize()}):")
        for feature, count in na_counts.most_common():
            percentage = (count / total_entities) * 100 if total_entities > 0 else 0
            print(f"  {feature}: {count} veces N/A ({percentage:.1f}%)")


def main():
    parser = argparse.ArgumentParser(description="Analiza el archivo JSON de clústeres (clusters.json).")
    parser.add_argument("--input-json", required=True, help="Ruta al archivo JSON de clústeres (ej. output/quality_analysis/entity_clusters.json).")
    parser.add_argument("--top-n-clusters", type=int, default=5, help="Número de clústeres más comunes a mostrar.")
    args = parser.parse_args()

    print_heading("Análisis de Clústeres de Entidades")

    data = load_cluster_data(args.input_json)
    metadata = data.get('metadata', {})
    country_clusters_data = data.get('country_clusters', [])
    city_clusters_data = data.get('city_clusters', [])

    country_features = metadata.get('country_features_used', [])
    city_features = metadata.get('city_features_used', [])

    # --- Análisis para Países ---
    if country_clusters_data:
        print_heading("--- Análisis de Clústeres de Países ---", level=1)
        country_cluster_counts = analyze_cluster_size_distribution(country_clusters_data, "Países")
        if country_cluster_counts:
             interpret_common_clusters(country_cluster_counts, "Países", args.top_n_clusters)
        analyze_na_features(country_clusters_data, country_features, "Países")
    else:
        print_warning("No se encontraron datos de clústeres de países para analizar.")

    # --- Análisis para Ciudades ---
    if city_clusters_data:
        print_heading("--- Análisis de Clústeres de Ciudades ---", level=1)
        city_cluster_counts = analyze_cluster_size_distribution(city_clusters_data, "Ciudades")
        if city_cluster_counts:
            interpret_common_clusters(city_cluster_counts, "Ciudades", args.top_n_clusters)
        analyze_na_features(city_clusters_data, city_features, "Ciudades")
    else:
        print_warning("No se encontraron datos de clústeres de ciudades para analizar.")
    
    print_success("Análisis de clústeres completado.")

if __name__ == "__main__":
    main()
