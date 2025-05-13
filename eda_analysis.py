#!/usr/bin/env python3

# /// script
# dependencies = [
#   "pandas>=2.0.0",
#   "matplotlib>=3.5.0", # Para gráficos
#   "seaborn>=0.11.0",   # Para gráficos más estéticos
#   "rich>=13.7.0",      # Para salida formateada en consola
#   "numpy>=1.20.0"      # Para manejar NaN
# ]
# ///

"""
Análisis Exploratorio de Datos (EDA) para Datos Municipales Procesados
(Versión compatible con Python 3.9+)

Carga el archivo JSON generado por SFA1 (v3 - con imputación y trazabilidad),
muestra información básica, estadísticas descriptivas y algunas visualizaciones
para entender la calidad y distribución de los datos.
"""

import pandas as pd
import numpy as np
import json
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import argparse
import sys
from typing import Union # <--- AÑADIR ESTA LÍNEA

# --- Configuración Visual (Rich) ---
try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text # Importar Text
    console = Console(stderr=True); USE_RICH = True
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
# --- Fin Configuración Visual ---

# --- Constantes ---
# (Sin cambios)
NUMERIC_COLS = ['population', 'total_waste_tons_year', 'recycling_rate_percent', 'collection_coverage_population_percent']
STATUS_COLS = [f"{col}_status" for col in NUMERIC_COLS]
CATEGORICAL_COLS = ['country', 'income_level', 'primary_collection_mode']
PLOTS_DIR = "eda_plots"
# --- Fin Constantes ---

# --- CORRECCIÓN AQUÍ ---
def load_data(json_path: str) -> Union[pd.DataFrame, None]:
# --- FIN CORRECCIÓN ---
    """Carga los datos desde el archivo JSON a un DataFrame de Pandas."""
    print_info(f"Cargando datos desde: {json_path}")
    path = Path(json_path)
    if not path.is_file():
        print_error(f"El archivo JSON no se encontró en: {json_path}")
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        print_success(f"Datos cargados exitosamente. {len(df)} registros.")
        # Asegurar tipos numéricos donde sea posible (columnas clave)
        for col in NUMERIC_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df
    except json.JSONDecodeError as e:
        print_error(f"Error al decodificar JSON: {e}")
        return None
    except Exception as e:
        print_error(f"Error inesperado al cargar los datos: {e}")
        return None

def display_basic_info(df: pd.DataFrame):
    """Muestra información básica del DataFrame."""
    print_heading("Información Básica del DataFrame")
    print(f"Dimensiones: {df.shape[0]} filas, {df.shape[1]} columnas")
    print("\nPrimeras 5 filas:")
    if USE_RICH:
        # Usar console.print para que Rich maneje la tabla de Pandas
        console.print(df.head())
    else:
        # Usar to_string para evitar truncamiento en terminales simples
        print(df.head().to_string())
    print("\nTipos de datos por columna:")
    # df.info() imprime a stdout, capturarlo si es necesario o dejarlo así
    # Para Rich, podríamos intentar capturar y mostrar en panel, pero es complejo
    # Dejamos que imprima directamente por ahora.
    df.info(buf=sys.stderr if not USE_RICH else None) # Imprimir a stderr si no hay Rich


def display_numeric_summary(df: pd.DataFrame):
    """Muestra estadísticas descriptivas para columnas numéricas."""
    print_heading("Resumen Estadístico (Columnas Numéricas)")
    numeric_cols_present = [col for col in NUMERIC_COLS if col in df.columns]
    if not numeric_cols_present:
        print_warning("No se encontraron columnas numéricas clave para el resumen.")
        return

    numeric_df = df[numeric_cols_present].copy()
    summary = numeric_df.describe().T # Transponer para mejor lectura

    # Crear tabla Rich
    if USE_RICH:
        table = Table(title="Estadísticas Descriptivas", show_header=True, header_style="bold magenta")
        table.add_column("Métrica", style="dim", width=35)
        # Añadir columnas de estadísticas dinámicamente
        for stat_name in summary.columns:
             table.add_column(stat_name, justify="right")

        # Añadir filas
        for index, row in summary.iterrows():
            row_data = [index] # Nombre de la columna numérica
            for stat_value in row:
                # Formatear números
                if pd.notna(stat_value):
                    # Usar formato con comas, ajustar decimales según necesidad
                    if abs(stat_value) >= 1000 or abs(stat_value) < 0.01 and stat_value != 0:
                         formatted_value = f"{stat_value:,.2f}"
                    else:
                         formatted_value = f"{stat_value:.2f}" # Menos decimales para números pequeños
                else:
                    formatted_value = "N/A"
                row_data.append(formatted_value)
            table.add_row(*row_data)
        console.print(table)
    else:
        # Formatear para print simple
        summary_formatted = summary.applymap(lambda x: f"{x:,.2f}" if pd.notna(x) else "N/A")
        print(summary_formatted.to_string())


def display_status_distribution(df: pd.DataFrame):
    """Muestra la distribución de los valores en las columnas de estado."""
    print_heading("Distribución de Estado de Datos (Trazabilidad)")
    status_columns_present = [col for col in STATUS_COLS if col in df.columns]

    if not status_columns_present:
        print_warning("No se encontraron columnas de estado (_status) en el DataFrame.")
        return

    if USE_RICH:
        table = Table(title="Conteo por Estado en Columnas Numéricas", show_header=True, header_style="bold blue")
        table.add_column("Columna Numérica", style="dim", width=35)
        table.add_column("Estado", style="cyan")
        table.add_column("Conteo", style="magenta", justify="right")
        table.add_column("Porcentaje (%)", style="green", justify="right")

        for col_status in status_columns_present:
            col_numeric = col_status.replace('_status', '')
            # Asegurarse que la columna existe antes de value_counts
            if col_status not in df.columns:
                table.add_row(f"[bold]{col_numeric}[/bold]", Text("Columna Status No Encontrada", style="red"), "-", "-")
                table.add_section()
                continue

            counts = df[col_status].value_counts()
            total = counts.sum()
            # Añadir fila de cabecera para la columna
            table.add_row(f"[bold]{col_numeric}[/bold]", "", "", "")
            if counts.empty:
                 table.add_row("  └─ (Sin datos de estado)", Text("-", style="dim"), "0", "0.0%")
            else:
                for status, count in counts.items():
                    percentage = (count / total * 100) if total > 0 else 0
                    # Aplicar estilo según el estado
                    status_style = ""
                    status_str = str(status) # Convertir a string por si acaso
                    if "imputed" in status_str: status_style = "cyan"
                    elif "invalid" in status_str or "missing" in status_str: status_style = "yellow"
                    elif "original" in status_str: status_style = "green"

                    table.add_row(f"  └─ {status_str}", Text(status_str, style=status_style), f"{count:,}", f"{percentage:.1f}%")
            table.add_section()
        console.print(table)
    else:
        # Salida simple para terminal sin Rich
        for col_status in status_columns_present:
            col_numeric = col_status.replace('_status', '')
            print(f"\nDistribución para: {col_numeric}")
            if col_status in df.columns:
                counts = df[col_status].value_counts(normalize=True) * 100
                if not counts.empty:
                    print(counts.to_string(float_format="%.1f%%"))
                else:
                    print("  (Sin datos de estado)")
            else:
                print("  (Columna Status No Encontrada)")


def plot_distributions(df: pd.DataFrame, output_dir: str):
    """Genera histogramas para columnas numéricas clave."""
    print_heading("Visualización de Distribuciones Numéricas")
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    print_info(f"Guardando gráficos en: {output_path.resolve()}")

    sns.set_theme(style="whitegrid")

    for col in NUMERIC_COLS:
        if col in df.columns and df[col].notna().any():
            plt.figure(figsize=(10, 6))
            data_to_plot = df[col].dropna()
            # Usar log scale si el rango es muy amplio (ej. población)
            # Añadir chequeo para evitar error si mediana es 0 o negativa
            use_log_scale = False
            try:
                 median_val = data_to_plot.median()
                 if pd.notna(median_val) and median_val > 0 and data_to_plot.max() / median_val > 100:
                      use_log_scale = True
            except ZeroDivisionError:
                 pass # No usar escala log si mediana es 0

            sns.histplot(data_to_plot, kde=True, log_scale=use_log_scale)
            title = f'Distribución de {col.replace("_", " ").title()}{" (Escala Log)" if use_log_scale else ""}'
            plt.title(title)
            plt.xlabel(col.replace("_", " ").title())
            plt.ylabel('Frecuencia')
            plot_filename = output_path / f"distribution_{col}.png"
            try:
                plt.savefig(plot_filename)
                print_success(f"  Gráfico guardado: {plot_filename.name}")
            except Exception as e:
                print_error(f"  No se pudo guardar el gráfico para {col}: {e}")
            plt.close() # Cerrar figura para liberar memoria
        else:
            print_warning(f"  No hay datos suficientes o la columna '{col}' no existe para generar gráfico.")

def plot_status_bars(df: pd.DataFrame, output_dir: str):
    """Genera gráficos de barras para la distribución de estados."""
    print_heading("Visualización de Estado de Datos")
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    status_columns_present = [col for col in STATUS_COLS if col in df.columns]
    if not status_columns_present:
        print_warning("No se encontraron columnas de estado para visualizar.")
        return

    num_plots = len(status_columns_present)
    # Ajustar figsize dinámicamente
    fig_height = max(4, num_plots * 2.5) # Incrementar altura por gráfico
    fig_width = 8

    try:
        fig, axes = plt.subplots(num_plots, 1, figsize=(fig_width, fig_height), squeeze=False) # squeeze=False para asegurar que axes sea siempre 2D

        for i, col_status in enumerate(status_columns_present):
            ax = axes[i, 0] # Acceder al subplot
            if col_status in df.columns:
                status_counts = df[col_status].value_counts()
                if not status_counts.empty:
                    status_counts.plot(kind='barh', ax=ax, color=sns.color_palette("viridis", len(status_counts)))
                    ax.set_title(f'Estado para {col_status.replace("_status", "").replace("_", " ").title()}')
                    ax.set_xlabel('Número de Registros')
                    ax.grid(axis='x', linestyle='--', alpha=0.7)
                else:
                    ax.text(0.5, 0.5, 'Sin datos de estado', horizontalalignment='center', verticalalignment='center', transform=ax.transAxes)
                    ax.set_title(f'Estado para {col_status.replace("_status", "").replace("_", " ").title()}')
            else:
                 ax.text(0.5, 0.5, 'Columna Status No Encontrada', horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, color='red')
                 ax.set_title(f'Estado para {col_status.replace("_status", "").replace("_", " ").title()}')


        plt.tight_layout(h_pad=3.0) # Ajustar padding vertical
        plot_filename = output_path / "data_status_summary.png"

        plt.savefig(plot_filename)
        print_success(f"  Gráfico de estado guardado: {plot_filename.name}")
    except Exception as e:
        print_error(f"  No se pudo guardar el gráfico de estado: {e}")
    finally:
        plt.close('all') # Cerrar todas las figuras


def main():
    parser = argparse.ArgumentParser(description="Realiza EDA sobre el JSON procesado por SFA1 v3.")
    parser.add_argument("input_json", help="Ruta al archivo JSON de entrada (ej. processed_city_data_large_imputed.json).")
    parser.add_argument("--plots-dir", default=PLOTS_DIR, help=f"Directorio para guardar los gráficos (default: {PLOTS_DIR}).")
    args = parser.parse_args()

    df = load_data(args.input_json)

    if df is not None:
        display_basic_info(df)
        display_numeric_summary(df)
        display_status_distribution(df)
        plot_distributions(df, args.plots_dir)
        plot_status_bars(df, args.plots_dir)
        print_heading("Análisis Exploratorio Completado")
        print_info(f"Se generaron gráficos en el directorio: '{Path(args.plots_dir).resolve()}'")

if __name__ == "__main__":
    main()
