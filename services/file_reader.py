import pandas as pd
import os

# Mapeo: columnas del archivo → columnas destino en BD
EXPECTED_COLUMNS = {
    'year': 'AÑO',
    'month': 'MES',
    'sap_code': 'SAP',
    'salesperson_no': 'NUMERO_SAP_VENDEDOR',
    'operations': 'NUM_OPERACIONES',
    'amount': 'IMPORTE_FINANCIADO'
}

def get_excel_or_csv_files(folder_path):
    return [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.lower().endswith(('.csv', '.xls', '.xlsx'))
    ]

def read_file(file_path):
    # Leer archivo según extensión
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path, sep=';', dtype=str)
    else:
        df = pd.read_excel(file_path)

    # Normalizar nombres de columnas del archivo
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]

    # Verificar columnas necesarias originales
    missing_originals = [orig for orig in EXPECTED_COLUMNS if orig not in df.columns]
    if missing_originals:
        raise ValueError(f"❌ Faltan columnas en el archivo {file_path}: {missing_originals}")

    # Renombrar columnas del archivo → modelo destino
    rename_map = {orig: dest for orig, dest in EXPECTED_COLUMNS.items()}
    df = df.rename(columns=rename_map)

    # Inspección rápida de valores que fallarán al convertir IMPORTE_FINANCIADO
    # temp = df.copy()

    # # Normalizar texto
    # temp['IMPORTE_SANITIZED'] = temp['IMPORTE_FINANCIADO'].astype(str).str.strip().replace('', pd.NA)
    # temp['IMPORTE_SANITIZED'] = temp['IMPORTE_SANITIZED'].str.replace(',', '.', regex=False)

    # # Intento de conversión
    # temp['IMPORTE_VALIDO'] = pd.to_numeric(temp['IMPORTE_SANITIZED'], errors='coerce')

    # # Filtrar solo los que fallarán
    # errores = temp[temp['IMPORTE_VALIDO'].isna()]

    # # Mostrar resumen
    # print(f"🔎 Total registros en archivo: {len(temp)}")
    # print(f"❌ IMPORTE_FINANCIADO inválido en: {len(errores)} registros")
    # print("📋 Ejemplos de errores:")
    # print(errores[['IMPORTE_FINANCIADO']].head(10))

    # Añadir columna fija FECHA_ALTA
    df['FECHA_ALTA'] = pd.Timestamp('1900-01-01')

    # Reordenar columnas
    ordered_columns = ['FECHA_ALTA', 'SAP', 'NUMERO_SAP_VENDEDOR', 'IMPORTE_FINANCIADO', 'AÑO', 'MES', 'NUM_OPERACIONES']
    df = df[ordered_columns]

    # Limpieza y diagnóstico
    original_len = len(df)

    # Convertir NUMERO_SAP_VENDEDOR (descartar si NaN o no numérico)
    # Reemplazar N/A y vacíos por 0
    df['NUMERO_SAP_VENDEDOR'] = df['NUMERO_SAP_VENDEDOR'].replace(['N/A', 'n/a', '', None], '0')

    # Convertir a numérico (0 incluidos)
    df['NUMERO_SAP_VENDEDOR'] = pd.to_numeric(df['NUMERO_SAP_VENDEDOR'], errors='coerce').fillna(0)

    df['IMPORTE_FINANCIADO'] = df['IMPORTE_FINANCIADO'].astype(str).str.strip()
    df['IMPORTE_FINANCIADO'] = df['IMPORTE_FINANCIADO'].str.replace('.', '', regex=False)  # quitar separador de miles
    df['IMPORTE_FINANCIADO'] = df['IMPORTE_FINANCIADO'].str.replace(',', '.', regex=False)  # coma decimal → punto
    df['IMPORTE_FINANCIADO'] = pd.to_numeric(df['IMPORTE_FINANCIADO'], errors='coerce')

    # Convertir otros campos numéricos
    df['NUM_OPERACIONES'] = pd.to_numeric(df['NUM_OPERACIONES'], errors='coerce')
    df['AÑO'] = pd.to_numeric(df['AÑO'], errors='coerce')
    df['MES'] = pd.to_numeric(df['MES'], errors='coerce')

    # Diagnóstico por campo
    campos_clave = ['SAP', 'IMPORTE_FINANCIADO', 'AÑO', 'MES', 'NUM_OPERACIONES']
    for campo in campos_clave:
        nulos = df[campo].isna().sum()
        if nulos > 0:
            print(f"⚠️ {nulos} registros con valor nulo en {campo}")

    # Eliminar filas con datos obligatorios nulos
    df = df.dropna(subset=campos_clave)

    final_len = len(df)
    descartados = original_len - final_len
    if descartados > 0:
        print(f"⚠️ {descartados} registros descartados por valores inválidos.")

    return df.to_dict(orient='records')
