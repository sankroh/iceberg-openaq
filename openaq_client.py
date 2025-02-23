import requests


def fetch_openaq_data(endpoint, params=None, max_pages=5):
    """
    Fetch data from OpenAQ API with pagination support.

    Args:
        endpoint (str): API endpoint (e.g., 'measurements', 'locations')
        params (dict): Query parameters
        max_pages (int): Maximum number of pages to fetch

    Returns:
        list: Fetched data items
    """
    url = f"{OPENAQ_API_BASE_URL}/{endpoint}"
    params = params or {}

    all_results = []
    page = 1

    while page <= max_pages:
        # Add page to parameters
        params["page"] = page
        params["limit"] = 1000  # Maximum allowed by the API

        logger.info(f"Fetching {endpoint} data - page {page}")
        response = requests.get(url, params=params)

        if response.status_code != 200:
            logger.error(f"Failed to fetch data: {response.status_code}")
            logger.error(f"Response: {response.text}")
            break

        data = response.json()
        results = data.get("results", [])

        if not results:
            logger.info(f"No more results found for {endpoint}")
            break

        all_results.extend(results)
        logger.info(f"Fetched {len(results)} items. Total so far: {len(all_results)}")

        page += 1

    return all_results


def fetch_measurements(
    country=None, city=None, parameter=None, date_from=None, date_to=None
):
    """
    Fetch air quality measurements from OpenAQ API.

    Args:
        country (str): Country code
        city (str): City name
        parameter (str): Measurement parameter (e.g., 'pm25', 'pm10', 'co')
        date_from (str): Start date in ISO format (e.g., '2023-01-01')
        date_to (str): End date in ISO format

    Returns:
        list: Measurement data
    """
    params = {}
    if country:
        params["country"] = country
    if city:
        params["city"] = city
    if parameter:
        params["parameter"] = parameter
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to

    return fetch_openaq_data("measurements", params)


def fetch_locations(country=None, city=None):
    """
    Fetch monitoring location information from OpenAQ API.

    Args:
        country (str): Country code
        city (str): City name

    Returns:
        list: Location data
    """
    params = {}
    if country:
        params["country"] = country
    if city:
        params["city"] = city

    return fetch_openaq_data("locations", params)
