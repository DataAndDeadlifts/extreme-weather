from pathlib import Path
import requests
from html.parser import HTMLParser
import gzip
from tqdm.auto import tqdm
import pandas as pd

# Parent Directories
DATA_DIR = Path(__file__).parent.parent.parent / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
INTERIM_DATA_DIR = DATA_DIR / "interim"
# Created Directories
STORM_EVENTS_GZIP_PATH = RAW_DATA_DIR / "StormEventsGZIP"
STORM_EVENTS_CSV_PATH = INTERIM_DATA_DIR / "StormEventsCSV"
STORM_EVENTS_COMBINED_PATH = INTERIM_DATA_DIR / "StormEventsCombined"


class StormDataLinkParser(HTMLParser):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.download_file_list = []

    def handle_data(self, data):
        if "StormEvents" in data and ".csv.gz" in data:
            self.download_file_list.append(data)

    def error(self, message):
        print("Parser Error: {}", str(message))


def get_data_from_http() -> None:
    """
    Download data from the StormEvents HTTP server
    """
    if not STORM_EVENTS_GZIP_PATH.exists():
        STORM_EVENTS_GZIP_PATH.mkdir()
    http_server_uri = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
    html_parser = StormDataLinkParser()
    index_page = requests.get(http_server_uri)
    html_parser.feed(index_page.text)

    for file_name in tqdm(html_parser.download_file_list):
        file_name_download_path = STORM_EVENTS_GZIP_PATH / file_name
        if not file_name_download_path.exists():
            download_response = requests.get(http_server_uri + file_name, stream=True)
            if download_response.status_code == 200:
                with file_name_download_path.open('ab+') as f:
                    for chunk in download_response.iter_content(chunk_size=1024):
                        if chunk:
                            f.write(chunk)


def unpack_data() -> None:
    """
    The data we got from the HTTP server is in csv.gz format. Lets unpack all that data.
    """
    zipped_storm_files = list(STORM_EVENTS_CSV_PATH.glob("*.csv.gz"))
    if len(zipped_storm_files) == 0:
        raise Exception("No files found at {} to unzip".format(str(STORM_EVENTS_CSV_PATH)))
    unzip_path = INTERIM_DATA_DIR / "StormEventsCSV"
    if not unzip_path.exists():
        unzip_path.mkdir(parents=True)
    for gzipped_file_path in tqdm(zipped_storm_files):
        csv_file_path = unzip_path / str(gzipped_file_path).rsplit("\\", 1)[-1].replace(".gz", "")
        if not csv_file_path.exists():
            with gzip.open(gzipped_file_path, 'rb') as f_in:
                csv_bytes = f_in.read()
                with open(csv_file_path, 'wb+') as f_out:
                    f_out.write(csv_bytes)


def headers_match(columns, csv_file_path) -> bool:
    """
    Simple function to test if the headers given are the same as the headers in a csv file.
    :param columns: Pandas Columns object.
    :param csv_file_path: Pathlike str or Path to csv file.
    :return: True if the columns are the same, else False.
    """
    return columns.intersection(pd.read_csv(csv_file_path, nrows=0).columns).size == len(columns)


def combine_files_from_list(write_path, file_list, chunk_size=1000) -> None:
    """
    Combine a list of file paths with data in the same format into a single csv file.
    :param write_path: Path to write to. String or Path object.
    :param file_list: List of file paths to read and consequently combine.
    :param chunk_size: Size of chunks to read in one at a time to save memory.
    """
    # Write header
    pd.read_csv(file_list[0], nrows=0).to_csv(write_path, header=True, index=False)
    # Write data
    for file_path in tqdm(file_list):
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            chunk.to_csv(write_path, mode='a', header=False, index=False)


def combine_csv_files() -> None:
    # All csv files
    storm_events_csv_files = list(STORM_EVENTS_CSV_PATH.glob("*.csv"))
    # Specific csv files
    event_detail_files = [x for x in storm_events_csv_files if "_details-" in str(x)]
    event_fatality_files = [x for x in storm_events_csv_files if "_fatalities-" in str(x)]
    event_location_files = [x for x in storm_events_csv_files if "_locations-" in str(x)]
    # Headers on the files
    event_file_header_baseline = pd.read_csv(event_detail_files[0], nrows=0).columns
    event_fatality_file_header_baseline = pd.read_csv(event_fatality_files[0], nrows=0).columns
    event_location_file_header_baseline = pd.read_csv(event_location_files[0], nrows=0).columns
    # Double checking file formats for consistency
    assert all([headers_match(event_file_header_baseline, x) for x in event_detail_files])
    assert all([headers_match(event_fatality_file_header_baseline, x) for x in event_fatality_files])
    assert all([headers_match(event_location_file_header_baseline, x) for x in event_location_files])
    if not STORM_EVENTS_COMBINED_PATH.exists():
        STORM_EVENTS_COMBINED_PATH.mkdir()

    combine_files_from_list(
        STORM_EVENTS_COMBINED_PATH / "StormEvents_details-all.csv",
        event_detail_files
    )
    combine_files_from_list(
        STORM_EVENTS_COMBINED_PATH / "StormEvents_fatalities-all.csv",
        event_fatality_files
    )
    combine_files_from_list(
        STORM_EVENTS_COMBINED_PATH / "StormEvents_locations-all.csv",
        event_location_files
    )
