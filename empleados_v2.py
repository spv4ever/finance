import os
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from datetime import datetime
from services.db_connector import get_connection

# Cargar variables de entorno
load_dotenv()
EXCEL_PATH = os.getenv("EMPLEADOS_EXCEL_PATH").strip('"')
LOG_PATH    = "logs/empleados_update.log"
TABLE_NAME  = "empleados_finance"

# Columnas esperadas y su mapeo
COLUMNS_MAP = {
    "Número de personal (P)": "SAP",
    "id capado": "NIF_CAPADO",
    "División de personal": "SAP_Tienda",
    "Nombre editado del empleado o candidato": "Nombre"
}


def get_file_modification_date(path):
    """Obtiene la fecha de modificación de un archivo."""
    return datetime.fromtimestamp(os.path.getmtime(path)).strftime('%Y-%m-%d %H:%M:%S')


def read_log_date():
    """Lee la fecha de la última ejecución desde el archivo de log."""
    if not os.path.exists(LOG_PATH):
        return None
    with open(LOG_PATH, "r") as f:
        return f.read().strip()


def write_log_date(date_str):
    """Escribe la fecha de la ejecución actual en el archivo de log."""
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "w") as f:
        f.write(date_str)


def read_excel_data(path):
    """Lee y procesa los datos del archivo Excel."""
    df = pd.read_excel(path, dtype=str)
    # Seleccionar y renombrar solo las columnas necesarias
    df = df[list(COLUMNS_MAP.keys())]
    df = df.rename(columns=COLUMNS_MAP)
    df = df.fillna("")
    # Limpiar el campo SAP quitando ceros a la izquierda
    df["SAP"] = df["SAP"].str.lstrip("0")
    return df


def read_sql_data():
    """Lee todos los empleados actuales de la base de datos."""
    conn = get_connection()
    cursor = conn.cursor()
    query = f"SELECT SAP, NIF_CAPADO, SAP_Tienda, Nombre FROM {TABLE_NAME}"
    cursor.execute(query)
    rows = cursor.fetchall()
    columnas = [column[0] for column in cursor.description]
    data = [dict(zip(columnas, row)) for row in rows]
    conn.close()
    return pd.DataFrame(data)


def sync_empleados():
    """Sincroniza los datos de empleados del Excel con la base de datos."""
    current_file_date = get_file_modification_date(EXCEL_PATH)
    last_logged_date = read_log_date()

    if current_file_date == last_logged_date:
        print("⏸️ El archivo no ha cambiado desde la última ejecución. No se actualiza.")
        return

    print("📥 Cargando datos desde Excel...")
    excel_df = read_excel_data(EXCEL_PATH)

    print("📤 Cargando datos actuales desde base de datos...")
    db_df = read_sql_data()

    conn = get_connection()
    cursor = conn.cursor()

    # DataFrames para almacenar los cambios
    nuevos = pd.DataFrame()
    eliminados = pd.Series(dtype=str)
    updates = pd.DataFrame()

    if db_df.empty:
        print("⚠️ La tabla en la base de datos está vacía. Se insertarán todos los registros del Excel.")
        nuevos = excel_df.copy()
    else:
        # --- 1. Detectar NUEVOS registros ---
        # Empleados que están en el Excel pero no en la BBDD
        merged_new = pd.merge(excel_df, db_df, on="SAP", how="left", indicator=True)
        nuevos_saps = merged_new[merged_new["_merge"] == "left_only"]["SAP"]
        nuevos = excel_df[excel_df["SAP"].isin(nuevos_saps)]

        # --- 2. Detectar registros ELIMINADOS ---
        # Empleados que están en la BBDD pero ya no en el Excel
        merged_del = pd.merge(db_df, excel_df, on="SAP", how="left", indicator=True)
        eliminados = merged_del[merged_del["_merge"] == "left_only"]["SAP"]
        if not eliminados.empty:
            print(f"🗑️ Eliminando {len(eliminados)} registros obsoletos...")
            # Usar executemany para una operación más eficiente
            sql_delete = f"DELETE FROM {TABLE_NAME} WHERE SAP = ?"
            cursor.executemany(sql_delete, eliminados.tolist())
            conn.commit()

        # --- 3. Detectar y preparar ACTUALIZACIONES ---
        # Comparamos empleados que existen en ambas fuentes
        merged_update = pd.merge(excel_df, db_df, on="SAP", how="inner", suffixes=('', '_db'))
        
        # Filtrar si NIF_CAPADO o SAP_Tienda han cambiado
        updates = merged_update[
            (merged_update["NIF_CAPADO"] != merged_update["NIF_CAPADO_db"]) |
            (merged_update["SAP_Tienda"] != merged_update["SAP_Tienda_db"])
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

    # --- 4. Insertar los NUEVOS registros ---
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
    
    # Actualizar el log con la fecha del archivo procesado
    write_log_date(current_file_date)

    # Imprimir resumen final
    print("\n📊 Resumen de la sincronización:")
    print(f"   ➕ Insertados:   {len(nuevos)}")
    print(f"   🗑️ Eliminados:   {len(eliminados)}")
    print(f"   🔁 Actualizados: {len(updates)}")
    print("✅ Sincronización completada. Log actualizado.")


if __name__ == "__main__":
    sync_empleados()