import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
from requests.exceptions import RequestException

class CostOfLivingScraper:

    def __init__(self) -> None:
        # Initialize data as a list of dictionaries (rows)
        self.data = []
        self.missing_data = []

        # Initialize master_columns for handling columns
        self.master_columns = []

        # Initialize global column mapping
        self.column_mapping = {}

        # Ensure the 'data' directory exists
        self.data_dir = os.path.join(os.getcwd(), 'data')
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

        # Print initialization message
        print("Initialized CostOfLivingScraper.")

    def get_country_name_list(self, country_url="https://www.numbeo.com/cost-of-living/"):
        try:
            response = requests.get(country_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            country_list = []
            for anchor_tag in soup.find_all('a', href=True):
                if 'country_result' in anchor_tag['href']:
                    country_name = anchor_tag["href"].split("=")[1]
                    country_name = country_name.replace("+", " ")  # Replace '+' with space
                    country_list.append(country_name)
            print(f"Fetched list of countries: {len(country_list)} countries found.")
            return country_list
        except RequestException as e:
            print(f"Failed to fetch country list from {country_url}: {e}")
            return []

    def fetch_cost_of_living(self, location_url, country_name, city_name="average", retries=3, backoff=2):
        """
        Fetches cost of living for a specific country or city.

        Args:
            location_url (str): The URL to fetch data from.
            country_name (str): The name of the country.
            city_name (str): The name of the city (default is "average" for country-level data).
            retries (int): Number of retry attempts for failed requests.
            backoff (int): Seconds to wait before retrying after a failure.

        Returns:
            bool: True if data was successfully fetched and parsed, False otherwise.
        """
        for attempt in range(retries):
            try:
                response = requests.get(location_url, timeout=10)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")
                table = soup.find("table", class_="data_wide_table")

                if table is None:
                    # Do not print a warning here; simply return False
                    return False

                # Initialize a new record
                record = {
                    "Country": country_name,
                    "City": city_name
                }
                # Initialize columns_in_order with predefined columns
                columns_in_order = ["Country", "City"]

                # Initialize column_name_counts for this country/city
                column_name_counts = {}

                for row in table.find_all("tr"):
                    columns = row.find_all("td")
                    if columns:
                        name = columns[0].text.strip()
                        price = columns[1].text.strip()
                        range_data = columns[2].text.strip() if len(columns) > 2 else ""

                        # Clean the price string
                        price_clean = price.replace('\xa0$', '').replace(',', '').strip()

                        # Parse range data
                        low_range, high_range = self.parse_range(range_data)

                        # Update occurrence count
                        column_name_counts[name] = column_name_counts.get(name, 0) + 1
                        occurrence_index = column_name_counts[name]

                        # Create key for mapping
                        key = (name, occurrence_index)

                        # Check if this key exists in global mapping
                        if key in self.column_mapping:
                            unique_name = self.column_mapping[key]
                        else:
                            if occurrence_index == 1:
                                unique_name = name
                            else:
                                unique_name = f"{name}_{occurrence_index}"
                            self.column_mapping[key] = unique_name

                        # Add data to the record
                        record[unique_name] = self.safe_float(price_clean)
                        record[f"{unique_name} Low Range"] = low_range
                        record[f"{unique_name} High Range"] = high_range

                        # Update columns_in_order
                        if unique_name not in columns_in_order:
                            columns_in_order.append(unique_name)
                            columns_in_order.append(f"{unique_name} Low Range")
                            columns_in_order.append(f"{unique_name} High Range")

                # Update master_columns with the columns in order
                self.update_master_columns(columns_in_order)

                # Append the record to the data list
                self.data.append(record)
                # Successful data fetch
                return True

            except RequestException as e:
                print(f"Error fetching {location_url}: {e}")
                if attempt < retries - 1:
                    print(f"Retrying in {backoff} seconds... (Attempt {attempt + 2}/{retries})")
                    time.sleep(backoff)
                else:
                    print(f"Failed to fetch {location_url} after {retries} attempts.")
                    return False
            finally:
                # Rate limiting: sleep for 1 second between requests
                time.sleep(1)

    def update_master_columns(self, new_columns_in_order):
        """
        Updates the master column list with new unique column names,
        inserting them at the appropriate positions based on the order they appear in the data.

        Args:
            new_columns_in_order (list): List of new column names in the order they appeared in the data.
        """
        master_i = 0
        new_i = 0
        while new_i < len(new_columns_in_order):
            new_col = new_columns_in_order[new_i]
            if master_i < len(self.master_columns) and self.master_columns[master_i] == new_col:
                # Columns match, move to next
                master_i += 1
            elif new_col in self.master_columns:
                # Column exists elsewhere in master_columns, move master_i to that position + 1
                master_i = self.master_columns.index(new_col) + 1
            else:
                # Insert new column at master_i
                self.master_columns.insert(master_i, new_col)
                master_i += 1
            new_i += 1

    def safe_float(self, s):
        """
        Safely convert a string to float. Returns np.nan if conversion fails.

        Args:
            s (str): The string to convert.

        Returns:
            float: The converted float or np.nan if conversion fails.
        """
        try:
            return float(s)
        except (ValueError, TypeError):
            return np.nan

    def parse_range(self, range_string):
        """Parse range string and return low and high values as floats."""
        try:
            # Remove currency symbols and split the range
            cleaned = range_string.replace('\xa0$', '').replace(',', '').strip()
            low, high = cleaned.split('-')
            return self.safe_float(low), self.safe_float(high)
        except (ValueError, AttributeError):
            # Return NaN for both if parsing fails
            return np.nan, np.nan

    def clean_location_name(self, location_name):
        """Cleans up location names by replacing encoded characters."""
        location_name = location_name.replace("%28", "(")
        location_name = location_name.replace("%29", ")")
        location_name = location_name.replace("+", " ")
        return location_name

    def parse_city_arguments(self, args, country_list):
        """
        Parse the space-separated countries and cities into a dictionary.
        Countries will be followed by their cities, until a new country appears.
        Handles hyphenated multi-word country and city names.
        """
        country_city_dict = {}
        current_country = None

        for arg in args:
            # Replace hyphens with spaces
            arg_clean = arg.replace('-', ' ')

            if arg_clean in country_list:
                current_country = arg_clean
                country_city_dict[current_country] = []
            elif current_country:
                # If it's not a country, treat it as a city for the current country
                country_city_dict[current_country].append(arg_clean)

        return country_city_dict

    def format_country_name_for_url(self, country_name):
        """Convert a country name into a URL-friendly format by replacing spaces with plus signs."""
        return country_name.replace(" ", "+").replace("-", "+").replace("(", "%28").replace(")", "%29")

    def format_city_name_for_url(self, city_name):
        """Convert a multi-word city name into a URL-friendly format by replacing spaces with hyphens."""
        return city_name.replace(" ", "-").replace("--", "-").replace("(", "").replace(")", "")

    def merge_data(self, country_city_dict, download_all_countries=False):
        """
        Merge data for specific countries and cities.

        country_city_dict: A dictionary of countries with a list of specific cities to fetch data for.
        download_all_countries: Boolean flag to indicate whether to download all countries' data.
        """
        if download_all_countries:
            # Download data for all countries (including those not in the list)
            country_list = self.get_country_name_list()
            print(f"Downloading data for all countries (total number of countries: {len(country_list)})\n")
        else:
            # Download data only for countries specified in the dictionary
            country_list = list(country_city_dict.keys())
            print(f"Downloading data only for specified countries: {country_list}\n")

        for country_name in country_list:
            country_name_clean = self.clean_location_name(country_name)
            country_name_url = self.format_country_name_for_url(country_name_clean)

            # First, get the cost of living data for the entire country (average data)
            country_url = f"https://www.numbeo.com/cost-of-living/country_result.jsp?country={country_name_url}&displayCurrency=USD"
            success = self.fetch_cost_of_living(country_url, country_name_clean)
            if success:
                print(f"    Successfully collected data for {country_name_clean} (average)")
            else:
                print(f"    Warning: Failed to collect data for {country_name_clean} (average)")

            # If the country is in the provided list, download city-level data (if specified)
            if country_city_dict and country_name in country_city_dict:
                cities = country_city_dict[country_name]
                for city_name in cities:
                    city_name_clean = self.format_city_name_for_url(city_name)
                    # Use the country_name_url defined earlier
                    # Try city-country format first
                    city_url = f"https://www.numbeo.com/cost-of-living/in/{city_name_clean}-{country_name_url}?displayCurrency=USD"
                    success = self.fetch_cost_of_living(city_url, country_name_clean, city_name)

                    # If no data, try the city-only format
                    if not success:
                        city_url = f"https://www.numbeo.com/cost-of-living/in/{city_name_clean}?displayCurrency=USD"
                        success = self.fetch_cost_of_living(city_url, country_name_clean, city_name)

                    # Only print the warning if both formats have failed
                    if not success:
                        location = f"{city_name}, {country_name_clean}"
                        self.missing_data.append(location)  # Log the missing location
                        print(f"Warning: No cost of living data found for {location}")
                    else:
                        print(f"    Successfully completed city {city_name} in {country_name_clean}")
        return self.data

    def save_data_to_parquet(self, file_name='cost_of_living_data.parquet'):
        """
        Saves the collected data to a Parquet file.

        Args:
            file_name (str): The name of the Parquet file to save.
        """
        try:
            df = pd.DataFrame(self.data, columns=self.master_columns)

            # Convert numeric columns, coercing errors to NaN
            for col in df.columns:
                if col not in ['Country', 'City']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # Ensure all numeric columns are float64
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            df[numeric_columns] = df[numeric_columns].astype('float64')

            output_path = os.path.join(self.data_dir, file_name)
            df.to_parquet(output_path, index=False)
            print(f"Data saved successfully to {output_path}")

            if self.missing_data:
                print(f"\nLocations with missing data: {', '.join(self.missing_data)}")

            print("\nData collection and saving completed successfully.")

        except Exception as e:
            print(f"An error occurred while saving data: {e}")

