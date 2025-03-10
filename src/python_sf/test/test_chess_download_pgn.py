from __future__ import annotations
from pathlib import Path
import requests
import pytest

# Import functions from your module
from python_sf.chess.download_pgn_files import (
    download_single_pgn_file,
    _get_url,
    _get_filename,
    _download_pgn_file,
)


@pytest.mark.parametrize(
    "year, month, expected_url",
    [
        (
            2021,
            9,
            "https://database.lichess.org/standard/lichess_db_standard_rated_2021-09.pgn.zst",
        ),
        (
            2020,
            12,
            "https://database.lichess.org/standard/lichess_db_standard_rated_2020-12.pgn.zst",
        ),
    ],
)
def test_get_url(year, month, expected_url):
    """Test that _get_url returns the expected URL for a given year and month."""
    assert _get_url(year, month) == expected_url, (
        f"Expected: {expected_url}, Actual: {_get_url(year, month)}"
    )


@pytest.mark.parametrize(
    "year, month, expected_filename",
    [
        (2021, 9, "lichess_2021-09.pgn.zst"),
        (2020, 12, "lichess_2020-12.pgn.zst"),
        (2025, 1, "lichess_2025-01.pgn.zst"),
    ],
)
def test_get_filename(year, month, expected_filename):
    """Test that _get_filename returns the expected filename for a given year and month."""
    assert _get_filename(year, month) == expected_filename, (
        f"Expected: {expected_filename}, Actual: {_get_filename(year, month)}"
    )


def test_download_pgn_file(tmp_path, monkeypatch):
    """Test that _download_pgn_file downloads content and writes it to a file."""
    fake_content = b"fake pgn content"
    fake_headers = {"Content-Length": str(len(fake_content))}
    fake_response = requests.Response()
    fake_response.headers = fake_headers

    class DummyResponse:
        """Dummy response class to simulate requests.get()."""

        content = fake_content
        headers = fake_headers

        def iter_content(self, chunk_size=1):
            """Return the content in chunks."""
            for i in range(0, len(self.content), chunk_size):
                yield self.content[i : i + chunk_size]

    def fake_get(url, *args, **kwargs):
        """Fake requests.get() that returns a DummyResponse."""
        return DummyResponse()


    # Replace requests.get with our fake_get
    monkeypatch.setattr(requests, "get", fake_get)
    file_path = str(tmp_path / "testfile.pgn.zst")

    # Call the function under test.
    _download_pgn_file("http://example.com/test", file_path)

    # Verify that the file was created with the expected content.
    written_content = Path(file_path).read_bytes()
    assert written_content == fake_content, (
        f"Expected: {fake_content}, Actual: {written_content}"
    )


@pytest.mark.parametrize(
    "year, month, expected_filename",
    [
        (2021, 9, "lichess_2021-09.pgn.zst"),
        (2020, 12, "lichess_2020-12.pgn.zst"),
        (2025, 1, "lichess_2025-01.pgn.zst"),
    ],
)
def test_download_single_pgn_file(monkeypatch, year, month, expected_filename):
    """Test that download_single_pgn_file calls _download_pgn_file with the correct URL and filename."""
    captured_args = {}

    def fake_download_pgn_file(url: str, file_path: str) -> None:
        """Fake _download_pgn_file that captures the arguments."""
        captured_args["url"] = url
        captured_args["file_path"] = file_path

    # Replace _download_pgn_file in the module with our fake version.
    monkeypatch.setattr(
        "python_sf.chess.download_pgn_files._download_pgn_file", fake_download_pgn_file
    )

    download_single_pgn_file(year, month)

    expected_url = f"https://database.lichess.org/standard/lichess_db_standard_rated_{year}-{month:02}.pgn.zst"
    assert captured_args["url"] == expected_url, (
        f"Expected: {expected_url}, Actual: {captured_args['url']}"
    )
    assert captured_args["file_path"] == expected_filename, (
        f"Expected: {expected_filename}, Actual: {captured_args['file_path']}"
    )
