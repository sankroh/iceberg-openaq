from openaq_client import fetch_locations, fetch_measurements
from iceberg import (
    setup_iceberg_catalog,
    create_measurements_table,
    create_locations_table,
    write_locations,
    write_measurements,
)

from config import configure_logging, DATA_DIR

# Set up logging
logger = configure_logging()


def main():
    """
    Main function to run the OpenAQ to Iceberg pipeline.
    """

    # Make sure DATA_DIR exists.
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Starting OpenAQ to Iceberg pipeline")

    try:
        # Set up Iceberg catalog
        logger.info("Setting up Iceberg catalog")
        catalog = setup_iceberg_catalog()

        # Create tables
        logger.info("Creating Iceberg tables")
        create_measurements_table(catalog)
        create_locations_table(catalog)

        # Fetch and write locations data
        logger.info("Fetching locations data")
        locations_data = fetch_locations(country="US")
        logger.info(f"Fetched {len(locations_data)} locations")

        logger.info("Writing locations data to Iceberg")
        write_locations(locations_data, catalog)

        # Fetch and write measurements data
        # Limiting to a small date range for the example
        logger.info("Fetching measurements data")
        measurements_data = fetch_measurements(
            country="US", parameter="pm25", date_from="2023-01-01", date_to="2023-01-02"
        )
        logger.info(f"Fetched {len(measurements_data)} measurements")

        logger.info("Writing measurements data to Iceberg")
        write_measurements(measurements_data, catalog)

        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
