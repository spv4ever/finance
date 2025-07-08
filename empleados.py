import os
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from datetime import datetime
from services.db_connector import get_connection

# Cargar variables de entorno
load_dotenv()
EXCEL_PATH = os.getenv("EMPLEADOS_EXCEL_PATH").strip('"')
LOG_PATH = "logs/empleados_update.log"
TABLE_NAME = "empleados_finance"

# Columnas esperadas y su mapeo
COLUMNS_MAP = {
    "Número de personal (P)": "SAP",
    "id capado": "NIF_CAPADO",
    "División de personal": "SAP_Tienda",
    "Nombre editado del empleado o candidato": "Nombre"
}


def get_file_modification_date(path):
    return datetime.fromtimestamp(os.path.getmtime(path)).strftime('%Y-%m-%d %H:%M:%S')


def read_log_date():
    if not os.path.exists(LOG_PATH):
        return None
    with open(LOG_PATH, "r") as f:
        return f.read().strip()


def write_log_date(date_str):
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "w") as f:
        f.write(date_str)


def read_excel_data(path):
    df = pd.read_excel(path, dtype=str)
    df = df[COLUMNS_MAP.keys()]
    df = df.rename(columns=COLUMNS_MAP)
    df = df.fillna("")
    df["SAP"] = df["SAP"].str.lstrip("0")
    return df


def read_sql_data():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT SAP, NIF_CAPADO, SAP_Tienda, Nombre FROM {TABLE_NAME}")
    rows = cursor.fetchall()
    columnas = [column[0] for column in cursor.description]
    data = [dict(zip(columnas, row)) for row in rows]
    conn.close()
    return pd.DataFrame(data)


def sync_empleados():
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

    nuevos = pd.DataFrame()
    eliminados = pd.Series(dtype=str)

    if db_df.empty:
        print("⚠️ La tabla en base de datos está vacía. Se insertarán todos los registros del Excel.")
        nuevos = excel_df.copy()
    else:
        # Detectar nuevos
        merged = pd.merge(excel_df, db_df, on="SAP", how="left", indicator=True)
        nuevos_saps = merged[merged["_merge"] == "left_only"]["SAP"]
        nuevos = excel_df[excel_df["SAP"].isin(nuevos_saps)]

        # Detectar eliminados
        merged_del = pd.merge(db_df, excel_df, on="SAP", how="left", indicator=True)
        eliminados = merged_del[merged_del["_merge"] == "left_only"]["SAP"]

        if not eliminados.empty:
            print(f"🗑️ Eliminando {len(eliminados)} registros obsoletos...")
            for sap in eliminados:
                cursor.execute(f"DELETE FROM {TABLE_NAME} WHERE SAP = ?", sap)
            conn.commit()

        # 🔄 Actualizar NIF_CAPADO si ha cambiado
        merged_update = pd.merge(excel_df, db_df, on="SAP", how="inner", suffixes=('', '_db'))
        updates = merged_update[merged_update["NIF_CAPADO"] != merged_update["NIF_CAPADO_db"]]
        if not updates.empty:
            print(f"🔁 Actualizando {len(updates)} registros con nuevo NIF_CAPADO...")
            for _, row in updates.iterrows():
                cursor.execute(
                    f"UPDATE {TABLE_NAME} SET NIF_CAPADO = ? WHERE SAP = ?",
                    row["NIF_CAPADO"], row["SAP"]
                )
            conn.commit()

    # Insertar nuevos
    if not nuevos.empty:
        print(f"➕ Insertando {len(nuevos)} registros nuevos...")
        for _, row in nuevos.iterrows():
            cursor.execute(
                f"""INSERT INTO {TABLE_NAME} (SAP, NIF_CAPADO, SAP_Tienda, Nombre)
                    VALUES (?, ?, ?, ?)""",
                row["SAP"], row["NIF_CAPADO"], row["SAP_Tienda"], row["Nombre"]
            )
        conn.commit()

    cursor.close()
    conn.close()
    write_log_date(current_file_date)

    print("\n📊 Resumen:")
    print(f"   ➕ Insertados: {len(nuevos)}")
    print(f"   🗑️ Eliminados: {len(eliminados)}")
    print(f"   🔁 Actualizados: {len(updates) if not db_df.empty else 0}")
    print("✅ Sincronización completada. Log actualizado.")


if __name__ == "__main__":
    sync_empleados()
