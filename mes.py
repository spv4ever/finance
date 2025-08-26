### url_test:

from services.file_reader import get_excel_or_csv_files, read_file
from services.db_connector import insert_records
from services.deduplication import get_existing_keys, filter_new_records

INPUT_FOLDER = 'finance_mes_import'

import shutil
import os
from config import SHARED_FOLDER

def main():
    files = get_excel_or_csv_files(SHARED_FOLDER)

    if not files:
        print("📭 No se encontraron archivos para procesar en la carpeta compartida.")
        return
    
    for file_path in files:
        print(f'📂 Procesando archivo: {file_path}')
        records = read_file(file_path)

        print(f'🔍 Obteniendo claves existentes de la base de datos...')
        existing_keys = get_existing_keys()

        print(f'🧹 Filtrando registros duplicados...')
        new_records = filter_new_records(records, existing_keys)

        print(f'✅ {len(new_records)} registros nuevos encontrados. Insertando...')
        if new_records:
            confirm = input(f"Se han detectado {len(new_records)} registros nuevos. ¿Deseas subirlos? (s/n): ")
            if confirm.lower() == 's':
                insert_records(new_records, batch_size=1000, pause_seconds=5)
            else:
                print("🚫 Inserción cancelada por el usuario.")
        else:
            print("No hay registros nuevos para insertar.")
        # ➕ Mover archivo a carpeta mes_procesado/
        destino_dir = os.path.join(os.path.dirname(file_path), "mes_procesado")
        os.makedirs(destino_dir, exist_ok=True)
        nombre_archivo = os.path.basename(file_path)
        nuevo_path = os.path.join(destino_dir, nombre_archivo)
        shutil.move(file_path, nuevo_path)
        print(f"📦 Archivo movido a: {nuevo_path}")


        #insert_records(new_records)

if __name__ == '__main__':
    main()
