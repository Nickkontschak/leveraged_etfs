import os
from dotenv import load_dotenv
from dataclasses import dataclass

BASE_DIR = os.path.abspath(os.path.dirname(_file_))
ROOT_DIR = os.path.join(BASE_DIR, "..")
DATA_PATH = os.path.join(ROOT_DIR, "data")
MODELS_PATH = os.path.join(ROOT_DIR, "models")
CACHE_PATH = os.path.join(DATA_PATH, "cache")


load_dotenv(ROOT_DIR)

@dataclass
class Config:
    NDL_API_KEY = os.getenv("NASDAQ_DATA_LINK_API_KEY")


