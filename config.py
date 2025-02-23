import os
import logging
from pathlib import Path

# Directories
PROJECT_DIR = Path(__file__).parent.resolve()
DATA_DIR = PROJECT_DIR / "data"
WAREHOUSE_DIR = PROJECT_DIR / "warehouse"

# OpenAQ API
OPENAQ_API_BASE_URL = "https://api.openaq.org/v2"

# Iceberg Catalog
CATALOG_CONFIG = {
    "uri": "http://localhost:8181",
    "warehouse": str(WAREHOUSE_DIR),
}

# Logging configuration
def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(PROJECT_DIR / "openaq-iceberg.log")
        ]
    )
