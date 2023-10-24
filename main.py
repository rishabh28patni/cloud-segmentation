import argparse
from satellite_processor import SatelliteProcessor  # Assuming your package is named your_package_name

def main(args):
    # Create an instance of SatelliteProcessor and initiate the processing
    satellite_processor = SatelliteProcessor(args.start_date, args.end_date)
    # Any other setup or function calls as needed

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process satellite data between specified dates.')
    parser.add_argument('start_date', type=str, help='The start date in YYYY-MM-DD format.')
    parser.add_argument('end_date', type=str, help='The end date in YYYY-MM-DD format.')

    args = parser.parse_args()
    main(args)
