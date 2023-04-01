import os
from pathlib import Path
from dotenv import load_dotenv
from dataclasses import dataclass

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
ENV_FILE = ROOT_DIR / ".env"
DATA_PATH = ROOT_DIR / "data"
MODELS_PATH = ROOT_DIR / "models"
CACHE_PATH = DATA_PATH / "cache"

load_dotenv(ENV_FILE)

@dataclass
class Config:
    NDL_API_KEY = os.getenv("NASDAQ_DATA_LINK_API_KEY")



