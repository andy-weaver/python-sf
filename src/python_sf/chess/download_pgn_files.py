from python_sf.snowflake_session import SnowflakeSession
import requests
from tqdm import tqdm

KILOBYTE_BLOCK = 1024
SINGLE_BLOCK = KILOBYTE_BLOCK * 100

def download_single_pgn_file(year: int, month: int) -> None:
    """Download a single PGN file for the given year and month."""
    url = _get_url(year, month)
    file_path = _get_filename(year, month)

    _download_pgn_file(url, file_path)

def _get_url(year: int, month: int) -> str:
    """Return the URL for the PGN file for the given year and month."""
    return f"https://database.lichess.org/standard/lichess_db_standard_rated_{year}-{month:02}.pgn.zst"

def _get_filename(year: int, month: int) -> str:
    """Return the filename for the PGN file for the given year and month."""
    return f"lichess_{year}-{month:02}.pgn.zst"

def _create_stage_for_raw_pgn_file(session: SnowflakeSession, stage_name: str = "RAW_PGN_STAGE") -> None:
    """Create a stage in Snowflake for the raw PGN file."""
    session.sql(f"CREATE OR REPLACE STAGE {stage_name}").collect()

def _download_pgn_file(url: str, file_path: str) -> None:
    """Download the PGN file from the given URL to the given file path."""
    # response = requests.get(url)
    # Path(file_path).write_bytes(response.content)
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get("content-length", 0))
    progress_bar = tqdm(total=total_size, unit="B", unit_scale=True)

    with open(file_path, "wb") as file:
        for data in response.iter_content(SINGLE_BLOCK):
            progress_bar.update(len(data))
            file.write(data)

    progress_bar.close()

def download_file(url, output_path):
    """
    Downloads a large file with a progress bar.

    Parameters
    ----------
    url : str
        The URL of the file to download.
    output_path : str
        The local file path where the downloaded file will be saved.
    """