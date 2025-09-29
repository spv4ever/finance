import os
import pandas as pd
from dotenv import load_dotenv
import pyodbc
from services.db_connector import get_connection
import math
import uuid

# Cargar .env
load_dotenv()
FOLDER_PATH = os.getenv("PROCESO_BASE_FOLDER").strip('"')

# Extensiones válidas
VALID_EXTENSIONS = (".xlsx", ".xls", ".csv")

pd.set_option("display.width", None)
pd.set_option("display.max_columns", None)


def generar_guid():
    return uuid.uuid4().hex[:12]  # Suficiente para 100k registros

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

def mostrar_tabla_completa(df, titulo=""):
    print(f"\n📊 Vista completa tras: {titulo}")
    print(df.head(50).to_string(index=False))  # ← muestra hasta 50 filas completas con todas las columnas

def fix_codigos_vacios(df):
    for col in ['COD_VEND', 'VEND_FIRMA']:
        df[col] = df[col].fillna('').astype(str).str.strip()
    return df

def fix_importe(df):
    df['IMPORTE'] = (
        df['IMPORTE']
        .astype(str)
        .str.strip()
    )
    df['IMPORTE'] = pd.to_numeric(df['IMPORTE'], errors='coerce').fillna(0).round(2)
    return df

def fix_comisiones(df):
    # Asegurarse de que IMPORTE está en float (por si acaso)
    df['IMPORTE'] = pd.to_numeric(df['IMPORTE'], errors='coerce').fillna(0).round(2)

    # Calcular 20% para com_COD_VEND -- Cambio a 0.40 pendiente de aprobar
    
    df['com_COD_VEND'] = (df['IMPORTE'] * 0.20).round(2)

    # Calcular el resto para com_VEND_FIRMA
    df['com_VEND_FIRMA'] = (df['IMPORTE'] - df['com_COD_VEND']).round(2)

    return df

def desdoblar_comisiones(df):
    df['IMPORTE_NUMERICO'] = df['IMPORTE']  # conservar valor original


    # Propagar el GUID en ambas mitades
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

    # Unir
    df_final = pd.concat([cod_rows, firma_rows], ignore_index=True)

    # numPersonal = últimos 5 caracteres de VENDEDOR
    df_final['numPersonal'] = df_final['VENDEDOR']

    # índice = SAP + VENDEDOR + numPersonal
    df_final['indice'] = df_final['SAP'] + df_final['VENDEDOR']

    # Eliminar columnas intermedias
    df_final = df_final.drop(columns=[
        'COD_VEND', 'VEND_FIRMA', 'com_COD_VEND', 'com_VEND_FIRMA', 'KPI', 'AÑO', 'MES','NOMBRE'
    ], errors='ignore')
    df_final = df_final.sort_values(by=['guid', 'FUENTE']).reset_index(drop=True)

    return df_final

def chequear_equilibrio(df):
    total_importe = df['IMPORTE'].sum()
    total_origen = df['IMPORTE_NUMERICO'].sum()

    print(f"\n🔍 Verificación de suma:")
    print(f"🧾 Total IMPORTE desdoblado:     {total_importe:.2f}")
    print(f"🧾 Total IMPORTE_NUMERICO total: {total_origen:.2f}")
    print(f"🧮 Esperado: {total_origen / 2:.2f}")

    if abs(total_importe - (total_origen / 2)) < 0.01:
        print("✅ Suma correcta: la descomposición es consistente.")
    else:
        print("❌ Error: las sumas no coinciden, revisar cálculo.")

def subir_comisiones(df, tabla_destino, batch_size=500):
    conn = get_connection()
    cursor = conn.cursor()

    # 🔍 Totales locales
    count_local = len(df)
    sum_local = round(df['IMPORTE'].sum(), 2)

    # 🔍 Totales en BBDD
    cursor.execute(f"SELECT COUNT(*), SUM(IMPORTE) FROM {tabla_destino}")
    count_db, sum_db = cursor.fetchone()
    sum_db = round(sum_db or 0, 2)

    print(f"💾 En BBDD: {count_db} registros / {sum_db:.2f}")
    print(f"📄 En local: {count_local} registros / {sum_local:.2f}")

    if count_db == 0:
        print("🚀 Subiendo registros nuevos...")
    elif count_db != count_local or not math.isclose(sum_db, sum_local, abs_tol=0.10):
        print("⚠️ Inconsistencia detectada. Borrando toda la tabla...")
        cursor.execute(f"DELETE FROM {tabla_destino}")
        conn.commit()
    else:
        print("✅ Los datos ya están cargados correctamente. No se sube nada.")
        cursor.close()
        conn.close()
        return
        # 🧹 Sanitizar NUM_OPERACIONES y demás campos numéricos
    # Convertir NaN a cadenas vacías en campos texto obligatorios
    for col in ['SAP', 'VENDEDOR', 'indice', 'numPersonal', 'FTCI']:
        df[col] = df[col].fillna('').astype(str).str.strip()

    # Para campos enteros obligatorios
    df['IND_PRIMERA_UTIL_INTERNA'] = pd.to_numeric(df['IND_PRIMERA_UTIL_INTERNA'], errors='coerce').fillna(0).astype(int)
        # Conversión y validación estricta
    df['NUM_OPERACIONES'] = pd.to_numeric(df['NUM_OPERACIONES'], errors='coerce').fillna(0).astype(int)
    df['IMPORTE_NUMERICO'] = pd.to_numeric(df['IMPORTE_NUMERICO'], errors='coerce').fillna(0).round(2)
    df['IMPORTE'] = pd.to_numeric(df['IMPORTE'], errors='coerce').fillna(0).round(2)
    # Subida en bloques
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

    print("✅ Subida finalizada.")
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
        #print("✅ Archivo leído. Columnas detectadas:")
        #print(df_original.columns.tolist())
        df_original = fix_vend_firma(df_original)
        print("🛠️ Fix aplicado: VEND_FIRMA completado si estaba vacío o era 0.")
        df_original = fix_codigos_vacios(df_original)
        df_original = fix_importe(df_original)
        df_original = fix_comisiones(df_original)
        #mostrar_tabla_completa(df_original, "fix COD_VEND y VEND_FIRMA vacíos")
        #mostrar_tabla_completa(df_original, "fix VEND_FIRMA")
        df_original = desdoblar_comisiones(df_original)
        chequear_equilibrio(df_original)
        subir_comisiones(df_original, "Datos_Normalizados", batch_size=500)
        #mostrar_tabla_completa(df_original, "🔁 Desdoble de comisiones por COD_VEND y VEND_FIRMA")
        # mostrar_tabla_completa(df_original, "fix VEND_FIRMA")
        # Aquí comenzará el flujo de transformaciones posteriores...
        # ✅ Mover archivo procesado
        procesados_path = os.path.join(FOLDER_PATH, "procesados")
        os.makedirs(procesados_path, exist_ok=True)
        archivo_destino = os.path.join(procesados_path, os.path.basename(path))
        os.rename(path, archivo_destino)
        print(f"📁 Archivo movido a: {archivo_destino}")

if __name__ == "__main__":
    main()
