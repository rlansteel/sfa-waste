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
Single File Agent (SFA) 4: Visualization Generator (v1)

Lee el archivo JSON procesado (con datos imputados y estado), calcula
métricas derivadas y genera visualizaciones clave sobre la gestión
de residuos municipales.
"""

import pandas as pd
import numpy as np
import json
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import argparse
import sys
from typing import Union # Para compatibilidad Python < 3.10

# --- Configuración Visual (Rich) ---
try:
    from rich.console import Console
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
PLOTS_DIR = "sfa4_visualizations" # Directorio de salida para gráficos
# Columnas numéricas de interés para visualización
COLS_TO_VISUALIZE = ['population', 'total_waste_tons_year', 'recycling_rate_percent', 'collection_coverage_population_percent', 'waste_per_capita_kg_day']
# --- Fin Constantes ---

def load_data(json_path: str) -> Union[pd.DataFrame, None]:
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
        # Asegurar tipos numéricos
        numeric_cols_base = ['population', 'total_waste_tons_year', 'recycling_rate_percent', 'collection_coverage_population_percent']
        for col in numeric_cols_base:
            if col in df.columns:
                # Intentar convertir, los errores se volverán NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df
    except json.JSONDecodeError as e:
        print_error(f"Error al decodificar JSON: {e}")
        return None
    except Exception as e:
        print_error(f"Error inesperado al cargar los datos: {e}")
        return None

def calculate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula métricas derivadas como residuos per cápita."""
    print_info("Calculando métricas derivadas...")
    df_calc = df.copy()

    # Calcular Residuos Per Cápita (kg/día)
    # Asegurarse de que población > 0 para evitar división por cero
    if 'population' in df_calc.columns and 'total_waste_tons_year' in df_calc.columns:
        # Convertir a numérico explícitamente aquí por si acaso
        pop = pd.to_numeric(df_calc['population'], errors='coerce')
        waste = pd.to_numeric(df_calc['total_waste_tons_year'], errors='coerce')

        # Máscara para valores válidos (población > 0 y ambos no NaN)
        valid_mask = (pop > 0) & pop.notna() & waste.notna()

        # Inicializar columna
        df_calc['waste_per_capita_kg_day'] = np.nan

        # Calcular solo donde es válido
        # (toneladas * 1000 kg/ton) / (población * 365 días/año)
        df_calc.loc[valid_mask, 'waste_per_capita_kg_day'] = (waste[valid_mask] * 1000) / (pop[valid_mask] * 365)

        # Añadir columna de estado si no existe (aunque no se imputa aquí)
        if 'waste_per_capita_kg_day_status' not in df_calc.columns:
             df_calc['waste_per_capita_kg_day_status'] = np.where(df_calc['waste_per_capita_kg_day'].notna(), 'calculated', 'missing_or_invalid_input')

        num_calculated = valid_mask.sum()
        num_missing_input = len(df_calc) - num_calculated
        print_success(f"  Métrica 'waste_per_capita_kg_day' calculada para {num_calculated} registros.")
        if num_missing_input > 0:
            print_warning(f"  No se pudo calcular para {num_missing_input} registros debido a datos faltantes/inválidos en población o residuos.")
    else:
        print_warning("  No se pudieron calcular residuos per cápita (faltan 'population' o 'total_waste_tons_year').")
        df_calc['waste_per_capita_kg_day'] = np.nan # Asegurar que la columna exista
        if 'waste_per_capita_kg_day_status' not in df_calc.columns:
             df_calc['waste_per_capita_kg_day_status'] = 'missing_input_columns'


    return df_calc

def generate_visualizations(df: pd.DataFrame, output_dir: str):
    """Genera y guarda las visualizaciones."""
    print_heading("Generando Visualizaciones")
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    print_info(f"Guardando gráficos en: {output_path.resolve()}")

    sns.set_theme(style="whitegrid")
    income_order = ['LIC', 'LMC', 'UMC', 'HIC'] # Orden deseado para los gráficos

    # --- Gráfico 1: Boxplot Residuos Per Cápita por Nivel de Ingresos ---
    col_y = 'waste_per_capita_kg_day'
    col_x = 'income_level'
    plot_filename = output_path / "waste_per_capita_vs_income.png"
    if col_y in df.columns and col_x in df.columns and df[col_y].notna().any():
        plt.figure(figsize=(10, 7))
        plot_data = df.dropna(subset=[col_y, col_x])
        # Asegurar que income_level sea categórico y ordenado
        plot_data[col_x] = pd.Categorical(plot_data[col_x], categories=income_order, ordered=True)
        plot_data = plot_data.sort_values(col_x)

        sns.boxplot(data=plot_data, x=col_x, y=col_y, palette="viridis", showfliers=False)
        plt.title('Residuos Generados por Persona por Día vs Nivel de Ingresos')
        plt.xlabel('Nivel de Ingresos del País')
        plt.ylabel('Residuos Per Cápita (kg/día)')
        # Ajustar límite Y basado en percentil 95 para evitar que outliers distorsionen mucho
        upper_limit = plot_data[col_y].quantile(0.95) if not plot_data.empty else 2.5
        plt.ylim(0, upper_limit * 1.1) # Añadir un pequeño margen superior
        try: plt.savefig(plot_filename); print_success(f"  Gráfico guardado: {plot_filename.name}")
        except Exception as e: print_error(f"  Error guardando gráfico {plot_filename.name}: {e}")
        plt.close()
    else: print_warning(f"  No se pudo generar gráfico '{plot_filename.stem}' (faltan datos en {col_y} o {col_x}).")

    # --- Gráfico 2: Boxplot Tasa de Reciclaje por Nivel de Ingresos ---
    col_y = 'recycling_rate_percent'
    col_x = 'income_level'
    plot_filename = output_path / "recycling_rate_vs_income.png"
    if col_y in df.columns and col_x in df.columns and df[col_y].notna().any():
        plt.figure(figsize=(10, 7))
        plot_data = df.dropna(subset=[col_y, col_x])
        plot_data[col_x] = pd.Categorical(plot_data[col_x], categories=income_order, ordered=True)
        plot_data = plot_data.sort_values(col_x)

        sns.boxplot(data=plot_data, x=col_x, y=col_y, palette="viridis", showfliers=False)
        plt.title('Tasa de Reciclaje (%) vs Nivel de Ingresos')
        plt.xlabel('Nivel de Ingresos del País')
        plt.ylabel('Tasa de Reciclaje (%)')
        plt.ylim(0, 100) # Rango lógico para porcentaje
        try: plt.savefig(plot_filename); print_success(f"  Gráfico guardado: {plot_filename.name}")
        except Exception as e: print_error(f"  Error guardando gráfico {plot_filename.name}: {e}")
        plt.close()
    else: print_warning(f"  No se pudo generar gráfico '{plot_filename.stem}' (faltan datos en {col_y} o {col_x}).")

    # --- Gráfico 3: Scatter Plot Población vs Residuos Totales ---
    col_x = 'population'
    col_y = 'total_waste_tons_year'
    col_hue = 'income_level'
    plot_filename = output_path / "population_vs_waste.png"
    if col_x in df.columns and col_y in df.columns and col_hue in df.columns and df[col_x].notna().any() and df[col_y].notna().any():
        plt.figure(figsize=(12, 8))
        plot_data = df.dropna(subset=[col_x, col_y, col_hue])
        # Filtrar población > 0 para escala logarítmica
        plot_data = plot_data[plot_data[col_x] > 0]

        if not plot_data.empty:
            scatter_plot = sns.scatterplot(data=plot_data, x=col_x, y=col_y, hue=col_hue,
                                           palette='viridis', alpha=0.7, size=col_x, sizes=(20, 400),
                                           hue_order=income_order) # Usar el orden definido
            # Aplicar escala logarítmica
            scatter_plot.set(xscale="log", yscale="log")
            plt.title('Población vs Residuos Totales Generados (Escala Log)')
            plt.xlabel('Población (Escala Log)')
            plt.ylabel('Residuos Totales (Toneladas/Año, Escala Log)')
            plt.legend(title='Nivel Ingresos')
            plt.grid(True, which="both", ls="--", linewidth=0.5)
            try: plt.savefig(plot_filename); print_success(f"  Gráfico guardado: {plot_filename.name}")
            except Exception as e: print_error(f"  Error guardando gráfico {plot_filename.name}: {e}")
            plt.close()
        else:
            print_warning(f"  No hay datos válidos (población > 0) para generar gráfico '{plot_filename.stem}'.")
            plt.close()
    else: print_warning(f"  No se pudo generar gráfico '{plot_filename.stem}' (faltan datos en {col_x}, {col_y} o {col_hue}).")


def main():
    parser = argparse.ArgumentParser(description="SFA4 v1: Genera visualizaciones EDA del JSON procesado.")
    parser.add_argument("input_json", help="Ruta al archivo JSON de entrada (ej. processed_city_data_large_imputed.json).")
    parser.add_argument("--plots-dir", default=PLOTS_DIR, help=f"Directorio para guardar los gráficos (default: {PLOTS_DIR}).")
    args = parser.parse_args()

    df = load_data(args.input_json)

    if df is not None:
        df_metrics = calculate_metrics(df)
        generate_visualizations(df_metrics, args.plots_dir)
        print_heading("Generación de Visualizaciones Completada")
        print_info(f"Se generaron gráficos en el directorio: '{Path(args.plots_dir).resolve()}'")

if __name__ == "__main__":
    main()
