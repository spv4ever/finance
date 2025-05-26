from services.db_connector import get_connection

def get_existing_keys():
    """
    Devuelve un set con claves únicas existentes en la base de datos:
    (FECHA_ALTA, SAP, NUMERO_SAP_VENDEDOR, AÑO, MES)
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT FECHA_ALTA, SAP, NUMERO_SAP_VENDEDOR, AÑO, MES FROM dbo.finance_mes
    """)
    existing_keys = set()
    for row in cursor.fetchall():
        key = (str(row[0]), row[1], row[2], row[3], row[4])
        existing_keys.add(key)
    cursor.close()
    conn.close()
    return existing_keys

def filter_new_records(records, existing_keys):
    """
    Filtra los registros que no estén en existing_keys.
    """
    new_records = []
    for r in records:
        key = (str(r['FECHA_ALTA']), r['SAP'], r['NUMERO_SAP_VENDEDOR'], r['AÑO'], r['MES'])
        if key not in existing_keys:
            new_records.append(r)
    return new_records
