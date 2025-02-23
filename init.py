import logging
from config import (
    PROJECT_DIR,
    DATA_DIR,
    OPENAQ_API_BASE_URL,
    CATALOG_CONFIG,
    configure_logging
)

# Initialize logging
configure_logging()
logger = logging.getLogger("openaq-to-iceberg")

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    StructType,
)
import pyiceberg.transforms as transforms
from datetime import datetime, timezone
import json

# Ensure data directory exists
DATA_DIR.mkdir(parents=True, exist_ok=True)
