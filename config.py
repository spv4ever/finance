import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "server": os.getenv("DB_SERVER"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": os.getenv("DB_DRIVER")
}

SHARED_FOLDER = os.getenv("SHARED_FOLDER")
