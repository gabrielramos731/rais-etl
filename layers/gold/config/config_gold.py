from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]  # sobe 2 níveis
PATH_ESTB_SILVER = BASE_DIR / 'silver' / 'data' / 'estabelecimentos'
PATH_ESTB_GOLD = BASE_DIR / 'gold' / 'data' / 'estabelecimentos'
DIM_PATH = BASE_DIR / 'silver' / 'data' / 'dimensions'
