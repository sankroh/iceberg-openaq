import os
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from pyiceberg.catalog import load_catalog, Catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)
import pyiceberg.transforms as transforms
from typing import List, Dict, Any

from config import CATALOG_CONFIG, configure_logging
from transform import transform_measurements, transform_locations

# Set up logging
logger = configure_logging()


def setup_iceberg_catalog() -> Catalog:
    """
    Set up the Iceberg catalog and create necessary namespaces.

    Returns:
        pyiceberg.catalog.Catalog: Configured Iceberg catalog
    """
    # Create warehouse directory if it doesn't exist
    Path(CATALOG_CONFIG["warehouse"]).mkdir(parents=True, exist_ok=True)

    # Load the REST catalog
    catalog = load_catalog("rest-catalog", **CATALOG_CONFIG)

    # Create the namespace if it doesn't exist
    catalog.create_namespace_if_not_exists("openaq")
    logger.info("Verified 'openaq' namespace is in Iceberg catalog")

    return catalog


def create_measurements_table(catalog: Catalog) -> None:
    """
    Create or replace the measurements table in the Iceberg catalog.

    Args:
        catalog: Configured Iceberg catalog
    """
    # Define schema for measurements table
    measurements_schema = Schema(
        NestedField(1, "measurement_id", StringType(), required=False),
        NestedField(2, "location_id", StringType(), required=False),
        NestedField(3, "location_name", StringType(), required=False),
        NestedField(4, "country", StringType(), required=False),
        NestedField(5, "city", StringType(), required=False),
        NestedField(6, "latitude", DoubleType(), required=False),
        NestedField(7, "longitude", DoubleType(), required=False),
        NestedField(8, "parameter", StringType(), required=False),
        NestedField(9, "value", DoubleType(), required=False),
        NestedField(10, "unit", StringType(), required=False),
        NestedField(11, "timestamp_utc", TimestampType(), required=False),
        NestedField(12, "timestamp_local", TimestampType(), required=False),
        NestedField(13, "source_name", StringType(), required=False),
        NestedField(14, "attribution", StringType(), required=False),
        NestedField(15, "averagingPeriod", StringType(), required=False),
    )

    # Check if table exists and create/replace as needed
    table_identifier = "openaq.measurements"

    if catalog.table_exists(table_identifier):
        catalog.drop_table(table_identifier)
        logger.info(f"Dropped existing table {table_identifier}")

    # Create the table with appropriate partitioning
    catalog.create_table(
        identifier=table_identifier,
        schema=measurements_schema,
        partition_spec=transforms.PartitionSpec(
            transforms.day("timestamp_utc"), transforms.identity("country")
        ),
        properties={
            "format-version": "2",
            "description": "Air quality measurements from OpenAQ",
        },
    )
    logger.info(f"Created table {table_identifier}")


def create_locations_table(catalog: Catalog) -> None:
    """
    Create or replace the locations table in the Iceberg catalog.

    Args:
        catalog: Configured Iceberg catalog
    """
    # Define schema for locations table
    locations_schema = Schema(
        NestedField(1, "location_id", StringType(), required=False),
        NestedField(2, "location_name", StringType(), required=False),
        NestedField(3, "country", StringType(), required=False),
        NestedField(4, "city", StringType(), required=False),
        NestedField(5, "latitude", DoubleType(), required=False),
        NestedField(6, "longitude", DoubleType(), required=False),
        NestedField(7, "is_mobile", IntegerType(), required=False),
        NestedField(8, "is_analysis", IntegerType(), required=False),
        NestedField(9, "parameters", StringType(), required=False),
        NestedField(10, "sources", StringType(), required=False),
        NestedField(11, "first_updated", TimestampType(), required=False),
        NestedField(12, "last_updated", TimestampType(), required=False),
        NestedField(13, "counts_by_measurement", StringType(), required=False),
        NestedField(14, "counts_by_day", StringType(), required=False),
    )

    # Check if table exists and create/replace as needed
    table_identifier = "openaq.locations"

    if catalog.table_exists(table_identifier):
        catalog.drop_table(table_identifier)
        logger.info(f"Dropped existing table {table_identifier}")

    # Create the table with appropriate partitioning
    catalog.create_table(
        identifier=table_identifier,
        schema=locations_schema,
        partition_spec=transforms.PartitionSpec(transforms.identity("country")),
        properties={
            "format-version": "2",
            "description": "Air quality monitoring locations from OpenAQ",
        },
    )
    logger.info(f"Created table {table_identifier}")


def write_to_iceberg(df: pd.DataFrame, table_identifier: str, catalog: Catalog) -> None:
    """
    Write a DataFrame to an Iceberg table.

    Args:
        df (pandas.DataFrame): Data to write
        table_identifier (str): Iceberg table identifier (e.g., 'openaq.measurements')
        catalog: Configured Iceberg catalog
    """
    if df.empty:
        logger.warning(f"No data to write to {table_identifier}")
        return

    # Get the table
    table = catalog.load_table(table_identifier)

    # Convert DataFrame to PyArrow Table
    arrow_table = pa.Table.from_pandas(df)

    # Save to Iceberg
    local_path = os.path.join(
        CATALOG_CONFIG["warehouse"], table_identifier.replace(".", "/")
    )
    ds.write_dataset(
        arrow_table,
        local_path,
        format="arrow",
        partitioning=table.spec().to_dict().get("partition-spec", None),
        existing_data_behavior="overwrite_or_ignore",
    )

    logger.info(f"Successfully wrote {len(df)} records to {table_identifier}")


def write_measurements(
    measurements_data: List[Dict[str, Any]], catalog: Catalog
) -> None:
    """
    Transform and write measurements data to Iceberg.

    Args:
        measurements_data (list): Raw measurements data from OpenAQ API
        catalog: Configured Iceberg catalog
    """
    df = transform_measurements(measurements_data)
    write_to_iceberg(df, "openaq.measurements", catalog)


def write_locations(locations_data: List[Dict[str, Any]], catalog: Catalog) -> None:
    """
    Transform and write locations data to Iceberg.

    Args:
        locations_data (list): Raw locations data from OpenAQ API
        catalog: Configured Iceberg catalog
    """
    df = transform_locations(locations_data)
    write_to_iceberg(df, "openaq.locations", catalog)
