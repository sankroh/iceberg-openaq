import json
import pandas as pd

def transform_measurements(measurements):
    """
    Transform raw measurements data into a clean format for Iceberg.

    Args:
        measurements (list): Raw measurements from OpenAQ API

    Returns:
        pandas.DataFrame: Transformed data
    """
    # Extract relevant fields and flatten the data
    transformed_data = []

    for measurement in measurements:
        location = measurement.get("location", {})
        if isinstance(location, str):
            location_name = location
            location_id = None
        else:
            location_name = location.get("name")
            location_id = location.get("id")

        coordinates = measurement.get("coordinates", {})
        date = measurement.get("date", {})

        transformed_record = {
            "measurement_id": measurement.get("id"),
            "location_id": location_id,
            "location_name": location_name,
            "country": measurement.get("country"),
            "city": measurement.get("city"),
            "latitude": coordinates.get("latitude") if coordinates else None,
            "longitude": coordinates.get("longitude") if coordinates else None,
            "parameter": measurement.get("parameter"),
            "value": measurement.get("value"),
            "unit": measurement.get("unit"),
            "timestamp_utc": date.get("utc") if date else None,
            "timestamp_local": date.get("local") if date else None,
            "source_name": measurement.get("sourceName"),
            "attribution": json.dumps(measurement.get("attribution", [])),
            "averagingPeriod": json.dumps(measurement.get("averagingPeriod", {})),
        }

        transformed_data.append(transformed_record)

    # Convert to DataFrame
    df = pd.DataFrame(transformed_data)

    # Convert timestamp strings to datetime objects
    if "timestamp_utc" in df.columns:
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    if "timestamp_local" in df.columns:
        df["timestamp_local"] = pd.to_datetime(df["timestamp_local"])

    return df


def transform_locations(locations):
    """
    Transform raw location data into a clean format for Iceberg.

    Args:
        locations (list): Raw locations from OpenAQ API

    Returns:
        pandas.DataFrame: Transformed data
    """
    transformed_data = []

    for location in locations:
        coordinates = location.get("coordinates", {})

        transformed_record = {
            "location_id": location.get("id"),
            "location_name": location.get("name"),
            "country": location.get("country"),
            "city": location.get("city"),
            "latitude": coordinates.get("latitude") if coordinates else None,
            "longitude": coordinates.get("longitude") if coordinates else None,
            "is_mobile": location.get("isMobile", False),
            "is_analysis": location.get("isAnalysis", False),
            "parameters": json.dumps(location.get("parameters", [])),
            "sources": json.dumps(location.get("sources", [])),
            "first_updated": location.get("firstUpdated"),
            "last_updated": location.get("lastUpdated"),
            "counts_by_measurement": json.dumps(
                location.get("countsByMeasurement", {})
            ),
            "counts_by_day": json.dumps(location.get("countsByDay", {})),
        }

        transformed_data.append(transformed_record)

    # Convert to DataFrame
    df = pd.DataFrame(transformed_data)

    # Convert timestamp strings to datetime objects
    for col in ["first_updated", "last_updated"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True)

    return df
