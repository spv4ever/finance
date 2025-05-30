import sys
import os

# Añadir carpeta raíz del proyecto al sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)

import pandas as pd
from dotenv import load_dotenv
from services.db_connector import get_connection

# Cargar .env
load_dotenv()
EXCEL_PATH = os.getenv("PRODUCCION_EXCEL_PATH").strip('"')

TABLAS = {
    "mes en curso": "produccion",
    "acumulado": "produccion_historial"
}

COLUMNAS_MAP = {
    "fecha": "fecha",
    "Codigo Tienda": "Codigo_Tienda",
    "Producción Rentable": "produccion_rentable",
    "Ventas_Venta_gross": "Ventas_Venta_Gross"
}

def leer_existentes(tabla):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT fecha FROM {tabla}")
    fechas = set(row[0] for row in cursor.fetchall())
    cursor.close()
    conn.close()
    return fechas

def insertar_nuevos(tabla, df, batch_size=500):
    if df.empty:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    total = len(df)

    for i in range(0, total, batch_size):
        batch = df.iloc[i:i + batch_size]
        print(f"📦 Insertando registros {i + 1} a {i + len(batch)} de {total}...")
        for _, row in batch.iterrows():
            cursor.execute(f"""
                INSERT INTO {tabla} (fecha, Codigo_Tienda, Produccion_Rentable, Ventas_Venta_Gross)
                VALUES (?, ?, ?, ?)
            """, row["fecha"], row["Codigo_Tienda"], row["produccion_rentable"], row["Ventas_Venta_Gross"])
        conn.commit()

    cursor.close()
    conn.close()
    return total


def procesar_pestana(pestana, tabla_destino):
    print(f"\n📄 Procesando pestaña: {pestana} → tabla: {tabla_destino}")
    df = pd.read_excel(EXCEL_PATH, sheet_name=pestana, engine='openpyxl', converters={col: str for col in COLUMNAS_MAP.keys()})

    df = df.astype(str)

    #print(df.dtypes)

    #print("📋 Columnas leídas:", df.columns.tolist())
    #print(df)

    df = df[list(COLUMNAS_MAP.keys())]
    df = df.rename(columns=COLUMNAS_MAP)
    df = df.fillna("")

    # Fecha
    df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce').dt.date

    # Conversión segura de campos numéricos
    for col in ['produccion_rentable', 'Ventas_Venta_Gross']:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .replace('', '0')      # coma decimal a punto
        )
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(2)
        #print(df)

    df = df.dropna(subset=['fecha'])

    # Comparar fechas
    fechas_existentes = leer_existentes(tabla_destino)
    nuevos_df = df[~df['fecha'].isin(fechas_existentes)]
    #print(df)

    insertados = insertar_nuevos(tabla_destino, nuevos_df, batch_size=500)

    print(f"✅ Insertados: {insertados} nuevas filas en '{tabla_destino}'")
    return insertados

def main():
    total_insertados = 0
    for pestana, tabla in TABLAS.items():
        insertados = procesar_pestana(pestana, tabla)
        total_insertados += insertados

    print("\n📊 Resumen total:")
    print(f"✔️ Registros insertados: {total_insertados}")

if __name__ == "__main__":
    main()
