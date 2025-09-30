from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]  # sobe 2 níveis
PATH_ESTB_BRONZE = BASE_DIR / 'bronze' / 'data' / 'conformed' / 'estabelecimentos'
OUT_PATH_ESTB_SILVER = BASE_DIR / 'silver' / 'data' / 'estabelecimentos'