import pyodbc
from config import DB_CONFIG
import time


def get_connection():
    conn_str = (
        f"DRIVER={{{DB_CONFIG['driver']}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['user']};"
        f"PWD={DB_CONFIG['password']}"
    )
    return pyodbc.connect(conn_str)

def insert_records(records, batch_size=500, pause_seconds=0):
    conn = get_connection()
    cursor = conn.cursor()
    total = len(records)
    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        print(f'📦 Insertando registros {i + 1} a {i + len(batch)} de {total}...')
        for rec in batch:
            cursor.execute("""
                INSERT INTO dbo.finance_mes (
                    FECHA_ALTA, SAP, NUMERO_SAP_VENDEDOR,
                    IMPORTE_FINANCIADO, AÑO, MES, NUM_OPERACIONES
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, rec['FECHA_ALTA'], rec['SAP'], rec['NUMERO_SAP_VENDEDOR'],
                 rec['IMPORTE_FINANCIADO'], rec['AÑO'], rec['MES'], rec['NUM_OPERACIONES'])
        conn.commit()
        # 😴 Pausa entre bloques
        if pause_seconds > 0:
            print(f"⏸️ Esperando {pause_seconds} segundos antes del siguiente lote...")
            time.sleep(pause_seconds)
    cursor.close()
    conn.close()
