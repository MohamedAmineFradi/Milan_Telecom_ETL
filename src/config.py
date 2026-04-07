import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'milan_telecom'),
    'user': os.getenv('DB_USER', 'bl4z'),
    'password': os.getenv('DB_PASSWORD', ''),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = Path(os.getenv('DATA_DIR', BASE_DIR / 'data_milan_cdr_kaggle'))

MILANO_GRID_FILE = DATA_DIR / 'milano-grid.geojson'
PROVINCES_FILE = DATA_DIR / 'Italian_provinces.geojson'
ISTAT_FILE = DATA_DIR / 'ISTAT_census_variables_2011.csv'

TRAFFIC_PATTERN = 'sms-call-internet-mi-*.csv'
MOBILITY_PATTERN = 'mi-to-provinces-*.csv'

TARGET_CRS = 'EPSG:32632'
