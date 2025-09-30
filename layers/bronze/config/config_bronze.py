import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

RAW_PATH_ESTB = os.getenv("RAW_PATH_ESTB")
BASE_DIR = Path(__file__).resolve().parents[1]
OUT_PATH_ESTB_BRONZE = BASE_DIR / 'data' / 'conformed' / 'estabelecimentos'