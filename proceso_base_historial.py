import os
import pandas as pd
from dotenv import load_dotenv
import pyodbc
from services.db_connector import get_connection
import uuid
import time

# Cargar .env
load_dotenv()
FOLDER_PATH = os.getenv("PROCESO_BASE_FOLDER").strip('"')

# Extensiones válidas
VALID_EXTENSIONS = (".xlsx", ".xls", ".csv")

pd.set_option("display.width", None)
pd.set_option("display.max_columns", None)

def generar_guid():
    return uuid.uuid4().hex[:12]

def get_files_from_folder(folder_path):
    return sorted([
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.lower().endswith(VALID_EXTENSIONS)
    ])

def read_file_as_text(path):
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(path, dtype=str)
    elif ext == ".csv":
        return pd.read_csv(path, dtype=str, sep=';', encoding='utf-8', engine='python')
    else:
        raise ValueError(f"Extensión de archivo no soportada: {ext}")

def fix_vend_firma(df):
    df['VEND_FIRMA'] = df['VEND_FIRMA'].fillna('')
    condition = (df['VEND_FIRMA'].str.strip() == '') | (df['VEND_FIRMA'].str.strip() == '0')
    df.loc[condition, 'VEND_FIRMA'] = df.loc[condition, 'COD_VEND']
    return df

def fix_codigos_vacios(df):
    for col in ['COD_VEND', 'VEND_FIRMA']:
        df[col] = df[col].fillna('').astype(str).str.strip()
    return df

def fix_importe(df):
    df['IMPORTE'] = pd.to_numeric(df['IMPORTE'], errors='coerce').fillna(0).round(2)
    return df

def fix_comisiones(df):
    df['IMPORTE'] = pd.to_numeric(df['IMPORTE'], errors='coerce').fillna(0).round(2)
    df['com_COD_VEND'] = (df['IMPORTE'] * 0.20).round(2) #### Cambio a 0.40 pendiente de aprobar
    df['com_VEND_FIRMA'] = (df['IMPORTE'] - df['com_COD_VEND']).round(2)
    return df

def desdoblar_comisiones(df):
    df['IMPORTE_NUMERICO'] = df['IMPORTE']
    
    # COD_VEND
    cod_rows = df.copy()
    cod_rows['FUENTE'] = 'COD_VEND'
    cod_rows['IMPORTE'] = cod_rows['com_COD_VEND']
    cod_rows['VENDEDOR'] = cod_rows['COD_VEND']
    cod_rows['guid'] = df['guid']
    
    # VEND_FIRMA
    firma_rows = df.copy()
    firma_rows['FUENTE'] = 'VEND_FIRMA'
    firma_rows['IMPORTE'] = firma_rows['com_VEND_FIRMA']
    firma_rows['VENDEDOR'] = firma_rows['VEND_FIRMA']
    firma_rows['guid'] = df['guid']

    df_final = pd.concat([cod_rows, firma_rows], ignore_index=True)
    df_final['numPersonal'] = df_final['VENDEDOR']
    df_final['indice'] = df_final['SAP'] + df_final['VENDEDOR']
    df_final = df_final.drop(columns=[
        'COD_VEND', 'VEND_FIRMA', 'com_COD_VEND', 'com_VEND_FIRMA', 'KPI', 'AÑO', 'MES','NOMBRE'
    ], errors='ignore')
    df_final = df_final.sort_values(by=['guid', 'FUENTE']).reset_index(drop=True)
    return df_final

def subir_comisiones_historico(df, tabla_destino, batch_size=500):
    if df.empty:
        print("ℹ️ El DataFrame está vacío, no hay nada que subir.")
        return

    conn = get_connection()
    cursor = conn.cursor()

    df['FECHA_ALTA'] = pd.to_datetime(df['FECHA_ALTA'], errors='coerce')
    
    primera_fecha_valida = df['FECHA_ALTA'].dropna().iloc[0]
    mes_a_cargar = primera_fecha_valida.month
    año_a_cargar = primera_fecha_valida.year

    print(f"🗓️  Verificando si los datos para {mes_a_cargar}/{año_a_cargar} ya existen en la tabla '{tabla_destino}'...")

    cursor.execute(f"""
        SELECT TOP 1 1 
        FROM {tabla_destino} 
        WHERE YEAR(FECHA_ALTA) = ? AND MONTH(FECHA_ALTA) = ?
    """, año_a_cargar, mes_a_cargar)

    if cursor.fetchone():
        print(f"⚠️  Los datos para el mes {mes_a_cargar}/{año_a_cargar} ya existen. No se subirán de nuevo.")
        cursor.close()
        conn.close()
        return

    print(f"✅ El mes {mes_a_cargar}/{año_a_cargar} no existe. Procediendo a la carga en '{tabla_destino}'...")
    
    for col in ['SAP', 'VENDEDOR', 'indice', 'numPersonal', 'FTCI', 'guid']:
        df[col] = df[col].fillna('').astype(str).str.strip()
    
    for col in ['IND_PRIMERA_UTIL_INTERNA', 'NUM_OPERACIONES']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    for col in ['IMPORTE_NUMERICO', 'IMPORTE']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(2)
    
    df['FECHA_ALTA'] = df['FECHA_ALTA'].dt.date

    count_local = len(df)
    
    # ## CAMBIO AÑADIDO ##
    print(f"\n🚀 Se subirán un total de {count_local} registros a la tabla '{tabla_destino}'.")

    for i in range(0, count_local, batch_size):
        batch = df.iloc[i:i+batch_size]
        print(f"📦 Insertando registros {i+1} a {i+len(batch)}...")
        for idx, row in batch.iterrows():
            try:
                cursor.execute(f"""
                    INSERT INTO {tabla_destino} (
                        FECHA_ALTA, SAP, IND_PRIMERA_UTIL_INTERNA, FTCI, NUM_OPERACIONES,
                        IMPORTE_NUMERICO, VENDEDOR, IMPORTE, FUENTE, indice, numPersonal, guid
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, row['FECHA_ALTA'], row['SAP'], row['IND_PRIMERA_UTIL_INTERNA'], row['FTCI'],
                    row['NUM_OPERACIONES'], row['IMPORTE_NUMERICO'], row['VENDEDOR'],
                    row['IMPORTE'], row['FUENTE'], row['indice'], row['numPersonal'], row['guid'])
            except Exception as e:
                print(f"❌ Error en fila {idx}: {e}")
                print(row.to_dict())
        conn.commit()

        if i + batch_size < count_local:
            print(f"☕ Lote confirmado. Pausando 5 segundos...")
            time.sleep(5)

    print(f"✅ Subida finalizada para {mes_a_cargar}/{año_a_cargar}. Se insertaron {count_local} registros en '{tabla_destino}'.")
    cursor.close()
    conn.close()


def main():
    files = get_files_from_folder(FOLDER_PATH)
    if not files:
        print("📭 No se encontraron archivos para procesar.")
        return

    for path in files:
        print(f"\n📂 Procesando archivo: {os.path.basename(path)}")
        df_original = read_file_as_text(path)
        
        df_original["guid"] = [generar_guid() for _ in range(len(df_original))]
        df_original = fix_vend_firma(df_original)
        df_original = fix_codigos_vacios(df_original)
        df_original = fix_importe(df_original)
        df_original = fix_comisiones(df_original)
        df_final = desdoblar_comisiones(df_original)

        subir_comisiones_historico(df_final, "Datos_Normalizados_historial", batch_size=1000)
        
        procesados_path = os.path.join(FOLDER_PATH, "procesados")
        os.makedirs(procesados_path, exist_ok=True)
        archivo_destino = os.path.join(procesados_path, os.path.basename(path))
        
        if os.path.exists(archivo_destino):
            os.remove(archivo_destino)

        os.rename(path, archivo_destino)
        print(f"📁 Archivo movido a: {archivo_destino}")

if __name__ == "__main__":
    main()