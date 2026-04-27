import os
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from datetime import datetime
from services.db_connector import get_connection

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
load_dotenv()
EXCEL_PATH = os.getenv("EMPLEADOS_EXCEL_PATH").strip('"')
LOG_PATH   = "logs/empleados_update.log"
TABLE_NAME = "empleados_finance"

# Columnas esperadas en el Excel y su mapeo a columnas de BBDD
COLUMNS_MAP = {
    "Número de personal (P)":                  "SAP",
    "id capado":                               "NIF_CAPADO",
    "División de personal":                    "SAP_Tienda",
    "Nombre editado del empleado o candidato": "Nombre"
}


# ---------------------------------------------------------------------------
# Helpers de log y fechas
# ---------------------------------------------------------------------------

def get_file_modification_date(path: str) -> str:
    """Devuelve la fecha de modificación de un archivo como string."""
    return datetime.fromtimestamp(os.path.getmtime(path)).strftime('%Y-%m-%d %H:%M:%S')


def read_log_date() -> str | None:
    """Lee la fecha de la última ejecución exitosa desde el log."""
    if not os.path.exists(LOG_PATH):
        return None
    with open(LOG_PATH, "r") as f:
        return f.read().strip()


def write_log_date(date_str: str) -> None:
    """Persiste la fecha de ejecución actual en el log."""
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "w") as f:
        f.write(date_str)


# ---------------------------------------------------------------------------
# Lectura de datos
# ---------------------------------------------------------------------------

def read_excel_data(path: str) -> pd.DataFrame:
    """Lee el Excel, selecciona y renombra las columnas necesarias."""
    df = pd.read_excel(path, dtype=str)
    df = df[list(COLUMNS_MAP.keys())]
    df = df.rename(columns=COLUMNS_MAP)
    df = df.fillna("")
    df["SAP"] = df["SAP"].str.lstrip("0")   # eliminar ceros a la izquierda
    return df


def read_sql_data() -> pd.DataFrame:
    """Lee todos los empleados actuales de la tabla en base de datos."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT SAP, NIF_CAPADO, SAP_Tienda, Nombre FROM {TABLE_NAME}")
    rows    = cursor.fetchall()
    columnas = [col[0] for col in cursor.description]
    conn.close()
    return pd.DataFrame([dict(zip(columnas, row)) for row in rows])


# ---------------------------------------------------------------------------
# Sincronización principal
# ---------------------------------------------------------------------------

def sync_empleados() -> None:
    """Sincroniza los datos de empleados del Excel con la base de datos."""

    # -- Guardia: salir si el archivo no ha cambiado -------------------------
    current_file_date = get_file_modification_date(EXCEL_PATH)
    last_logged_date  = read_log_date()

    if current_file_date == last_logged_date:
        print("⏸️  El archivo no ha cambiado desde la última ejecución. No se actualiza.")
        return

    # -- Carga de datos ------------------------------------------------------
    print("📥 Cargando datos desde Excel...")
    excel_df = read_excel_data(EXCEL_PATH)

    print("📤 Cargando datos actuales desde base de datos...")
    db_df = read_sql_data()

    conn   = get_connection()
    cursor = conn.cursor()

    # Inicializar contadores/DataFrames para el resumen final
    nuevos    = pd.DataFrame()
    eliminados = pd.Series(dtype=str)
    updates   = pd.DataFrame()

    if db_df.empty:
        # Tabla vacía → insertar todo el Excel directamente
        print("⚠️  La tabla en la base de datos está vacía. Se insertarán todos los registros del Excel.")
        nuevos = excel_df.copy()

    else:
        # --- 1. Detectar NUEVOS registros -----------------------------------
        # Empleados presentes en el Excel pero ausentes en la BBDD
        merged_new  = pd.merge(excel_df, db_df, on="SAP", how="left", indicator=True)
        nuevos_saps = merged_new[merged_new["_merge"] == "left_only"]["SAP"]
        nuevos      = excel_df[excel_df["SAP"].isin(nuevos_saps)]

        # --- 2. Detectar registros ELIMINADOS --------------------------------
        # Empleados presentes en la BBDD pero ya no en el Excel
        merged_del = pd.merge(db_df, excel_df, on="SAP", how="left", indicator=True)
        eliminados = merged_del[merged_del["_merge"] == "left_only"]["SAP"]

        if not eliminados.empty:
            print(f"🗑️  Eliminando {len(eliminados)} registros obsoletos...")
            sql_delete = f"DELETE FROM {TABLE_NAME} WHERE SAP = ?"
            # executemany requiere una lista de tuplas, no de strings sueltos
            cursor.executemany(sql_delete, [(sap,) for sap in eliminados])
            conn.commit()

        # --- 3. Detectar ACTUALIZACIONES ------------------------------------
        # Empleados que existen en ambas fuentes pero con datos distintos
        merged_update = pd.merge(
            excel_df, db_df,
            on="SAP", how="inner",
            suffixes=('', '_db')
        )
        updates = merged_update[
            (merged_update["NIF_CAPADO"]  != merged_update["NIF_CAPADO_db"]) |
            (merged_update["SAP_Tienda"]  != merged_update["SAP_Tienda_db"])
        ]

        if not updates.empty:
            print(f"🔁 Actualizando {len(updates)} registros (NIF_CAPADO y/o SAP_Tienda cambiados)...")
            sql_update = f"""
                UPDATE {TABLE_NAME}
                SET NIF_CAPADO = ?, SAP_Tienda = ?
                WHERE SAP = ?
            """
            update_data = [
                (row["NIF_CAPADO"], row["SAP_Tienda"], row["SAP"])
                for _, row in updates.iterrows()
            ]
            cursor.executemany(sql_update, update_data)
            conn.commit()

    # --- 4. Insertar los NUEVOS registros -----------------------------------
    if not nuevos.empty:
        print(f"➕ Insertando {len(nuevos)} registros nuevos...")
        sql_insert = f"""
            INSERT INTO {TABLE_NAME} (SAP, NIF_CAPADO, SAP_Tienda, Nombre)
            VALUES (?, ?, ?, ?)
        """
        insert_data = [
            (row["SAP"], row["NIF_CAPADO"], row["SAP_Tienda"], row["Nombre"])
            for _, row in nuevos.iterrows()
        ]
        cursor.executemany(sql_insert, insert_data)
        conn.commit()

    cursor.close()
    conn.close()

    # -- Actualizar log y mostrar resumen ------------------------------------
    write_log_date(current_file_date)

    print("\n📊 Resumen de la sincronización:")
    print(f"   ➕ Insertados:   {len(nuevos)}")
    print(f"   🗑️  Eliminados:   {len(eliminados)}")
    print(f"   🔁 Actualizados: {len(updates)}")
    print("✅ Sincronización completada. Log actualizado.")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    sync_empleados()
