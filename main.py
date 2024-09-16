import argparse
from src.web_scraper import CostOfLivingScraper

# Command-line argument parsing
def parse_arguments():
    parser = argparse.ArgumentParser(description="Download cost-of-living data for specified countries and cities.")
    parser.add_argument("--selective", nargs='+', default=[],
                        help="List of countries followed by their respective cities. Use hyphens for multi-word names. Example: United-States New-York Canada Toronto")
    parser.add_argument("--all-countries", action="store_true", 
                        help="Download data for all countries in addition to the specified countries and cities.")
    return parser.parse_args()

# Main script
if __name__ == "__main__":
    args = parse_arguments()
    # Instantiate the web scraping class
    scraper = CostOfLivingScraper()
    # Get the list of available countries (dynamically fetch from Numbeo)
    available_countries = scraper.get_country_name_list()
    # Parse the cities argument to create a country-city dictionary
    country_city_dict = scraper.parse_city_arguments(args.selective, available_countries)
    # Download data for the specified countries and cities (with option to download all countries)
    scraper.merge_data(country_city_dict=country_city_dict, download_all_countries=args.all_countries)
    # Save the data to the data/ directory in Parquet format
    scraper.save_data_to_parquet()
