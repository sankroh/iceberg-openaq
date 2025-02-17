# iceberg-openaq
Create an iceberg catalog for OpenAQ data.


# Setup Instructions for OpenAQ to Iceberg Pipeline

## Prerequisites

- Python 3.8+
- Docker (for running the Iceberg REST catalog)
- pip packages: `requests`, `pandas`, `pyarrow`, `pyiceberg`

## Installation

1. Install required Python packages:

```bash
pip install requests pandas pyarrow pyiceberg
```

2. Run the Iceberg REST catalog service using Docker:

```bash
docker run -p 8181:8181 \
  -e CATALOG_WAREHOUSE=/tmp/warehouse \
  -e CATALOG_IO__IMPL=org.apache.iceberg.io.FileIO \
  -e CATALOG_URI=http://localhost:8181 \
  tabulario/iceberg-rest:0.6.0
```

## Project Structure

Create the following files in your project directory:

1. `config.py` - Configuration and imports
2. `openaq_client.py` - OpenAQ API client functions
3. `transform.py` - Data transformation functions
4. `iceberg_setup.py` - Iceberg catalog and table setup
5. `iceberg_writer.py` - Functions to write data to Iceberg
6. `main.py` - Main script to run the pipeline

## Running the Pipeline

1. Start the Iceberg REST catalog service using the Docker command above
2. Run the main script:

```bash
python main.py
```

## Customization

You can modify the pipeline by:

1. Changing the date range, countries, or parameters in the `main.py` file
2. Adjusting the schema or partitioning in the `iceberg_setup.py` file
3. Modifying the transformation logic in the `transform.py` file

## Troubleshooting

If you encounter issues:

1. Check that the Iceberg REST catalog service is running
2. Verify network connectivity to the OpenAQ API
3. Check the log output for detailed error messages