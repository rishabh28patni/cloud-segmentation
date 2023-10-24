import boto3
import logging
import os
from pathlib import Path
from tqdm import tqdm
from composite_creator import CompositeCreator
from utils import convert_date, is_leap_year, get_total_days

# Get AWS credentials from environment variables
access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

class SatelliteProcessor:
    def __init__(self, start_date, end_date):
        self.bucket = 'noaa-goes17'
        self.channels = ['C03', 'C06', 'C07', 'C13', 'C15']
        self.timestamps = ['0-10', '10-20', '20-30', '30-40', '40-50', '50-60']
        self.base_directory = Path('/content/goes_data')
        self.s3 = boto3.client('s3', aws_access_key_id=access_key_id,aws_secret_access_key=secret_access_key)
        self.paginator = self.s3.get_paginator('list_objects_v2')
        self.get_files_download(start_date, end_date)


    def get_files_in_folder(self, year, day, hour, timestamp_index):
        files = []
        timestamp = self.timestamps[timestamp_index]
        for channel in self.channels:
              temp_files = []
              folder_prefix_m6 = f'ABI-L1b-RadF/{year}/{day}/{hour}/OR_ABI-L1b-RadF-M6{channel}_G17'
              for file_key in self.get_matching_s3_objects(folder_prefix_m6):
                  temp_files.append(file_key)
              temp_files.sort()
              files.append(temp_files[timestamp_index])
        return files


    def create_directories(self, year, day, hour):
        directory = self.base_directory.joinpath(str(year), str(day).zfill(3), str(hour).zfill(2))
        if not directory.exists():
            directory.mkdir(parents=True)
        return directory

    def download_files_from_s3(self, files_list, base_directory):
        downloaded_files = []
        for index in range(len(files_list)):
            file_name = files_list[index]
            destination = f"{base_directory}/{file_name.split('/')[-1]}"
            try:
                logging.info(f"Downloading {file_name} to {destination}")
                self.s3.download_file(self.bucket, file_name, destination)
                downloaded_files.append(os.path.join(base_directory, os.path.basename(file_name)))
            except Exception as e:
                logging.error(f"Failed to download {file_name}: {e}")
        return downloaded_files


    def get_matching_s3_objects(self, prefix):
        paginator = self.s3.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': self.bucket, 'Prefix': prefix}
        page_iterator = paginator.paginate(**operation_parameters)
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    yield obj['Key']

    def get_files_download(self, start_date, end_date):
        all_downloaded_files = []
        base_directory = '/content/goes_data'
        start_year, start_day = convert_date(start_date)
        end_year, end_day = convert_date(end_date)

        total_days = get_total_days(start_date, end_date)

        with tqdm(total=total_days, desc="Days") as pbar:

          for year in range(start_year, end_year + 1):
              first_day = 1
              last_day = 366 if is_leap_year(year) else 365

              if year == start_year:
                  first_day = start_day
              if year == end_year:
                  last_day = end_day

              for day in range(first_day, last_day + 1):
                  day_pad = str(day).zfill(3)
                  for hour in range(24):
                      hour_pad = str(hour).zfill(2)
                      directory = self.create_directories(year, day, hour)

                      for timestep in range(6):
                          files_m6 = self.get_files_in_folder(year, day_pad, hour_pad, timestep)
                          # Download all files for this channel at once
                          downloaded_files_for_channel = self.download_files_from_s3(files_m6, directory)
                          composite_creator = CompositeCreator(directory, self.timestamps[timestep])

                          for files in downloaded_files_for_channel:
                            os.remove(files)

                  pbar.update(1)
        return all_downloaded_files

