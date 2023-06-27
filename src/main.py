import os
import time
import threading
import pandas as pd
import geopandas as gpd
import pygeohash as gh
from fuzzywuzzy import process
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

boolStrings = ['true', '1', 'yes']


class TFLParser():
    def __init__(self):
        self.input_file_path = os.environ.get('FILE_INPUT_PATH', '/data/input')
        self.output_file_path = os.environ.get(
            'FILE_OUTPUT_PATH', '/data/output')
        self.meta_file_path = os.environ.get('FILE_META_PATH', '/data/meta')
        self.generate_csv = os.environ.get(
            'GENERATE_CSV', 'true').lower() in boolStrings
        self.generate_sqllite = os.environ.get(
            'GENERATE_SQLLITE', 'true').lower() in boolStrings

    def _toCSV(self, trainData, busData):
        trainData.to_csv(
            f'{self.output_file_path}/{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}-train-journeys.csv', index=False)
        busData.to_csv(
            f'{self.output_file_path}/{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}-bus-journeys.csv', index=False)

    def _get_coordinates_from_station(self, station, station_mapping):
        if pd.notnull(station):
            best_match = process.extractOne(station, station_mapping.keys())
            if best_match[1] < 70:
                print(station, best_match)
                return None
            return station_mapping[best_match[0]]
        else:
            return None

    def _calculate_geohash(self, row, lat, lng):
        return gh.encode(row[lat], row[lng], precision=12)

    def process(self):
        # Load CSV Files
        file_list = os.listdir(self.input_file_path)
        csv_files = [f for f in file_list if f.endswith('.csv')]
        df_list = []
        for file in csv_files:
            df = pd.read_csv(os.path.join(self.input_file_path, file))
            df_list.append(df)
        all_data = pd.concat(df_list, ignore_index=True)

        # Add additional fields from parsed data
        all_data['isTrainJourney'] = all_data['Journey'].apply(
            lambda x: ' to ' in x)
        all_data['isBusJourney'] = all_data['Journey'].apply(
            lambda x: 'Bus Journey' in x)
        all_data['fromStation'] = all_data.apply(lambda row: row['Journey'].split(
            ' to ')[0] if row['isTrainJourney'] else None, axis=1)
        all_data['toStation'] = all_data.apply(lambda row: row['Journey'].split(' to ')[
                                               1] if row['isTrainJourney'] else None, axis=1)
        all_data['busRoute'] = all_data.apply(lambda row: row['Journey'].split(
            'Bus Journey, Route')[1] if row['isBusJourney'] else None, axis=1)

        # Handles Time and Date fields
        all_data['startTimeStr'] = all_data.apply(
            lambda row: row['Time'].split(' - ')[0] if row['isTrainJourney'] else row['Time'], axis=1)

        all_data['endTimeStr'] = all_data.apply(
            lambda row: row['Time'].split(' - ')[1] if row['isTrainJourney'] else row['Time'], axis=1)

        all_data['startDate'] = pd.to_datetime(
            all_data['Date'] + ' ' + all_data['startTimeStr'], format='%d/%m/%Y %H:%M', errors='coerce')
        all_data['endDate'] = pd.to_datetime(
            all_data['Date'] + ' ' + all_data['endTimeStr'], format='%d/%m/%Y %H:%M', errors='coerce')

        # Drop and Rename Fields
        # all_data = all_data.drop(
        #     ['Date', 'Time', 'Notes', 'Capped', 'startTimeStr', 'endTimeStr'], axis=1)
        # all_data = all_data.rename(columns={'Journey': 'journey'})

        # Split Train and Bus Journeys
        train_journeys = all_data[all_data['isTrainJourney']]
        train_journeys = train_journeys[(train_journeys['fromStation'] != 'Unknown') & (
            train_journeys['toStation'] != 'Unknown')]

        bus_journeys = all_data[all_data['isBusJourney']]

        # Station Geodata Match
        station_geo_data = gpd.read_file(
            f'{self.meta_file_path}/stations.geojson')
        station_mappings = {row['name']: [
            row['geometry'].x, row['geometry'].y] for _, row in station_geo_data.iterrows()}

        # Coordinates and Geohash
        train_journeys[['toLng', 'toLat']] = train_journeys['fromStation'].apply(
            self._get_coordinates_from_station, args=(station_mappings, )).apply(pd.Series)

        train_journeys[['fromLng', 'fromLat']] = train_journeys['toStation'].apply(
            self._get_coordinates_from_station, args=(station_mappings, )).apply(pd.Series)

        train_journeys['fromGeohash'] = train_journeys.apply(
            self._calculate_geohash, args=('fromLat', 'fromLng'), axis=1)
        train_journeys['toGeohash'] = train_journeys.apply(
            self._calculate_geohash, args=('toLat', 'toLng'), axis=1)

        if self.generate_csv:
            print('export csv files')
            self._toCSV(train_journeys, bus_journeys)


class FileHandler(FileSystemEventHandler):
    def __init__(self):
        self.last_modified = time.time()
        self.debounce_period = self.input_file_path = os.environ.get(
            'DEBOUNCE_PERIOD', 30)  # debounce period in seconds
        self.timer = None

    def trigger(self):
        print('START PROCESSING')
        tfl_parser = TFLParser()
        tfl_parser.process()

    def on_created(self, event):
        filename = event.src_path
        _, extension = os.path.splitext(filename)
        if extension == ".csv":
            print(f'File {filename} is a CSV file')
            if self.timer and self.timer.is_alive():
                self.timer.cancel()
            self.timer = threading.Timer(self.debounce_period, self.trigger)
            self.timer.start()


if __name__ == "__main__":
    input_file_path = os.environ.get('FILE_INPUT_PATH', '/data/input')
    event_handler = FileHandler()
    observer = Observer()
    observer.schedule(event_handler, path=input_file_path, recursive=False)
    observer.start()
    print('INITIAL PROCESSING')
    tfl_parser = TFLParser()
    tfl_parser.process()
    try:
        while True:
            time.sleep(30)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
