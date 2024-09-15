import os
import requests
from bs4 import BeautifulSoup
import pandas as pd


class CostOfLivingScraper:

    def __init__(self) -> None:
        self.data_dict = dict()
        self.data_dict["Country"] = list()
        self.data_dict["City"] = list()
        self.missing_data = []

        # Ensure the 'data' directory exists
        self.data_dir = os.path.join(os.getcwd(), 'data')
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

    def get_country_name_list(self, country_url="https://www.numbeo.com/cost-of-living/"):
        response = requests.get(country_url).text
        soup = BeautifulSoup(response, "html.parser")

        country_list = list()

        for anchor_tag in soup.find_all('a', href=True):
            if 'country_result' in anchor_tag['href']:
                country_list.append(anchor_tag["href"].split("=")[1])

        return country_list

    def fetch_cost_of_living(self, location_url, country_name, city_name="average"):
        """Fetches cost of living for a specific country or city."""
        response = requests.get(location_url).text
        soup = BeautifulSoup(response, "html.parser")
        table = soup.find("table", class_="data_wide_table")

        # Check if the table exists before trying to parse it
        if table is None:
            return False  # Return False to indicate missing data

        # Append country and city to the data_dict
        self.data_dict["Country"].append(country_name)
        self.data_dict["City"].append(city_name)

        for row in table.find_all("tr"):
            column = row.find_all("td")
            if column:
                name = column[0].text.strip()
                price = column[1].text.strip()

                # Clean price data by removing symbols like $ and non-breaking spaces
                price = price.replace('\xa0$', '').replace(',', '').strip()

                if name not in self.data_dict:
                    self.data_dict[name] = [price]
                else:
                    self.data_dict[name].append(price)
        return True  # Return True to indicate successful data extraction

    def clean_location_name(self, location_name):
        """Cleans up location names by replacing encoded characters."""
        location_name = location_name.replace("%28", "(")
        location_name = location_name.replace("%29", ")")
        location_name = location_name.replace("+", " ")

        return location_name

    def format_city_name_for_url(self, city_name):
        """Convert a multi-word city name into a URL-friendly format by replacing spaces with hyphens."""
        return city_name.replace(" ", "-")

    def parse_city_arguments(self, args, country_list):
        """
        Parse the space-separated countries and cities into a dictionary.
        Countries will be followed by their cities, until a new country appears.
        """
        country_city_dict = {}
        current_country = None

        for arg in args:
            if arg in country_list:
                # If the argument is a country, create a new entry for that country
                current_country = arg
                country_city_dict[current_country] = []
            elif current_country:
                # If it's not a country, treat it as a city for the current country
                country_city_dict[current_country].append(arg)

        return country_city_dict

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

            # First, get the cost of living data for the entire country (average data)
            country_url = f"https://www.numbeo.com/cost-of-living/country_result.jsp?country={country_name_clean}&displayCurrency=USD"
            self.fetch_cost_of_living(country_url, country_name_clean)
            print(f"    Successfully collected data for {country_name_clean} (average)")

            # If the country is in the provided list, download city-level data (if specified)
            if country_city_dict and country_name in country_city_dict:
                cities = country_city_dict[country_name]
                for city_name in cities:
                    city_name_clean = self.format_city_name_for_url(city_name)

                    # Try city-country format first
                    city_url = f"https://www.numbeo.com/cost-of-living/in/{city_name_clean}-{country_name_clean}?displayCurrency=USD"
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
                        print(f"    Successfully completed city {city_name_clean} in {country_name_clean}")
            else:
                print(f"    No specific cities for {country_name_clean}, only average data was downloaded.")

        return self.data_dict

    def save_data_to_parquet(self, file_name='cost_of_living_data.parquet'):
        data_dict = self.data_dict
        count_dict = dict()

        # Count how many data points each category has (for consistency)
        for key in data_dict:
            count = str(len(data_dict[key]))
            if count not in count_dict:
                count_dict[count] = 1
            else:
                count_dict[count] += 1

        # Find the maximum value to filter out incomplete data
        max_value = max(count_dict, key=count_dict.get)

        final_data_dict = {}
        for key in data_dict:
            if len(data_dict[key]) == int(max_value):
                final_data_dict[key] = data_dict[key]

        print("\nData collection completed successfully.")

        # Create a pandas DataFrame from the final dictionary
        df = pd.DataFrame(final_data_dict)

        # Convert all columns except 'Country' and 'City' to float64
        for col in df.columns:
            if col not in ['Country', 'City']:
                df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert to float, coerce errors to NaN

        # Save the DataFrame to a Parquet file in the 'data/' directory
        output_path = os.path.join(self.data_dir, file_name)
        df.to_parquet(output_path, index=False)
        print(f"Data saved successfully to {output_path}")

        if self.missing_data:
            print(f"\nLocations with missing data: {', '.join(self.missing_data)}")

        return df

