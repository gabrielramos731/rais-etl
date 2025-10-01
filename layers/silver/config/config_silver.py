from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]  # sobe 2 níveis
PATH_ESTB_BRONZE = BASE_DIR / 'bronze' / 'data' / 'conformed' / 'estabelecimentos'
OUT_PATH_ESTB_SILVER = BASE_DIR / 'silver' / 'data' / 'estabelecimentos'
DIM_RAW_PATH = BASE_DIR.parent / 'dicionarios'
DIM_OUT_PATH = BASE_DIR / 'silver' / 'data' / 'dimensions'