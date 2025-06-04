import os
import shutil
import pandas as pd
from dotenv import load_dotenv
from services.db_connector import get_connection

# Cargar variables de entorno
load_dotenv()
OBJETIVOS_FOLDER = os.getenv("OBJETIVOS_FOLDER")

# Configuración de Pandas
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)

def get_objetivos_files(folder_path):
    return sorted([
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.lower().endswith((".xlsx", ".xls"))
    ])

def read_hoja1(path):
    xl = pd.ExcelFile(path)
    hoja1 = [s for s in xl.sheet_names if s.strip().lower() == 'hoja1']
    if not hoja1:
        raise ValueError(f"No se encontró la hoja 'Hoja1' en el archivo {os.path.basename(path)}")
    return xl.parse(hoja1[0], dtype=str)

def fix_trc_objetivo(df):
    # Normalizamos los nombres de columnas
    df.columns = [col.strip().upper().replace(" ", "_") for col in df.columns]

    if "TRC_OBJETIVO" not in df.columns:
        raise ValueError("No se encontró la columna 'TRC OBJETIVO' en el archivo.")

    df["TRC_OBJETIVO"] = (
    df["TRC_OBJETIVO"]
    .str.replace("%", "", regex=False)
    .str.replace(",", ".")
    .astype(float)
    .round(4)
)

    return df

def validar_duplicados(df):
    if df.duplicated(subset=["SAP", "MES"]).any():
        raise ValueError("⚠️ Existen combinaciones duplicadas de SAP + MES en el archivo.")
    return df

def subir_objetivos(df, tabla_destino="trc_objetivos", batch_size=500):
    conn = get_connection()
    cursor = conn.cursor()

    total = len(df)
    print(f"🚀 Subiendo {total} registros a la tabla '{tabla_destino}'...")

    for i in range(0, total, batch_size):
        batch = df.iloc[i:i+batch_size]
        for _, row in batch.iterrows():
            cursor.execute(f"""
                MERGE {tabla_destino} AS target
                USING (SELECT ? AS SAP, ? AS MES) AS source
                ON target.SAP = source.SAP AND target.MES = source.MES
                WHEN MATCHED THEN
                    UPDATE SET TRC_OBJETIVO = ?
                WHEN NOT MATCHED THEN
                    INSERT (SAP, MES, TRC_OBJETIVO)
                    VALUES (?, ?, ?);
            """, row["SAP"], row["MES"], row["TRC_OBJETIVO"],
                 row["SAP"], row["MES"], row["TRC_OBJETIVO"])
        conn.commit()

    cursor.close()
    conn.close()
    print("✅ Subida completada.")

def mover_a_completos(path):
    carpeta_destino = os.path.join(os.path.dirname(path), "completos")
    os.makedirs(carpeta_destino, exist_ok=True)
    nuevo_path = os.path.join(carpeta_destino, os.path.basename(path))
    shutil.move(path, nuevo_path)
    print(f"📦 Archivo movido a: {nuevo_path}")

def main():
    if not OBJETIVOS_FOLDER:
        print("❌ La variable de entorno OBJETIVOS_FOLDER no está definida.")
        return

    files = get_objetivos_files(OBJETIVOS_FOLDER)
    if not files:
        print("📭 No se encontraron archivos .xlsx/.xls para procesar.")
        return

    for path in files:
        print(f"\n📂 Procesando archivo: {os.path.basename(path)}")
        try:
            df = read_hoja1(path)
            df = fix_trc_objetivo(df)
            df = validar_duplicados(df)
            subir_objetivos(df)
            mover_a_completos(path)
        except Exception as e:
            print(f"❌ Error procesando {os.path.basename(path)}: {e}")

if __name__ == "__main__":
    main()
