import sqlite3
import argparse
from pathlib import Path

def add_missing_columns(db_path_str):
    """
    Añade las columnas faltantes a las tablas 'cities' y 'city_measurements'
    si no existen.
    """
    db_path = Path(db_path_str)
    if not db_path.exists():
        print(f"Error: El archivo de base de datos no existe en {db_path}")
        return

    # Columnas a añadir a 'cities' y sus tipos (SQLite es flexible con tipos)
    cities_cols_to_add = {
        "original_wfd_id": "REAL",
        "waste_gen_rate_kg_cap_day": "REAL",
        "collection_pct_formal": "REAL",
        "collection_pct_informal": "REAL",
        "treatment_pct_recycling_composting": "REAL",
        "treatment_pct_thermal": "REAL",
        "treatment_pct_landfill": "REAL",
        "composition_sum_pct": "REAL",
        "composition_sum_flag": "TEXT" # O INTEGER/BOOLEAN si prefieres
    }

    # Columna a añadir a 'city_measurements'
    measurements_cols_to_add = {
        "value_text": "TEXT"
    }

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print(f"Conectado a {db_path}")

        # --- Modificar tabla 'cities' ---
        print("\nVerificando/Modificando tabla 'cities'...")
        cursor.execute("PRAGMA table_info(cities)")
        cities_existing_columns = [info[1] for info in cursor.fetchall()]

        for col_name, col_type in cities_cols_to_add.items():
            if col_name not in cities_existing_columns:
                print(f"  Añadiendo columna '{col_name}' ({col_type}) a 'cities'...")
                try:
                    cursor.execute(f"ALTER TABLE cities ADD COLUMN {col_name} {col_type}")
                    print(f"  ¡Columna '{col_name}' añadida a 'cities'!")
                except sqlite3.OperationalError as e:
                     # Manejar caso donde la columna ya existe a pesar de la verificación (raro)
                     if "duplicate column name" in str(e):
                          print(f"  Advertencia: La columna '{col_name}' ya existe (detectado por error ALTER).")
                     else:
                          raise e # Relanzar otros errores
            else:
                print(f"  Columna '{col_name}' ya existe en 'cities'.")
        conn.commit() # Guardar cambios en cities

        # --- Modificar tabla 'city_measurements' ---
        print("\nVerificando/Modificando tabla 'city_measurements'...")
        cursor.execute("PRAGMA table_info(city_measurements)")
        measurements_existing_columns = [info[1] for info in cursor.fetchall()]

        for col_name, col_type in measurements_cols_to_add.items():
             if col_name not in measurements_existing_columns:
                print(f"  Añadiendo columna '{col_name}' ({col_type}) a 'city_measurements'...")
                try:
                    cursor.execute(f"ALTER TABLE city_measurements ADD COLUMN {col_name} {col_type}")
                    print(f"  ¡Columna '{col_name}' añadida a 'city_measurements'!")
                except sqlite3.OperationalError as e:
                     if "duplicate column name" in str(e):
                          print(f"  Advertencia: La columna '{col_name}' ya existe (detectado por error ALTER).")
                     else:
                          raise e
             else:
                print(f"  Columna '{col_name}' ya existe en 'city_measurements'.")
        conn.commit() # Guardar cambios en city_measurements

        print("\nModificación de la base de datos completada.")

    except sqlite3.Error as e:
        print(f"Error de SQLite al modificar tablas: {e}")
    except Exception as e:
        print(f"Error inesperado: {e}")
    finally:
        if conn:
            conn.close()
            print("Conexión a la base de datos cerrada.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Añade columnas faltantes a las tablas 'cities' y 'city_measurements'.")
    parser.add_argument("--db-file", default="output/waste_data.db", help="Ruta a la base de datos SQLite (default: output/waste_data.db).")
    args = parser.parse_args()
    add_missing_columns(args.db_file)
