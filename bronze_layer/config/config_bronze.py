import os
from dotenv import load_dotenv

load_dotenv()

RAW_PATH_ESTB = os.getenv("RAW_PATH_ESTB")
OUT_PATH_ESTB_BRONZE = os.path.join(os.path.dirname(__file__), '..', 'data', 'conformed', 'estabelecimentos')