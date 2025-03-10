import zstandard
from pathlib import Path

from python_sf.chess.preprocess_pgn_file import _decompress_pgn_file


def test_decompress_pgn_file(tmp_path: Path) -> None:
    """
    Test the _decompress_pgn_file function by compressing a sample PGN text,
    writing it to a temporary file, decompressing it, and verifying the result.
    """
    original_text = (
        '[Event "Test Game"]\n'
        '[Site "?"]\n'
        '[Date "2025.03.10"]\n'
        '[Round "?"]\n'
        '[White "Player1"]\n'
        '[Black "Player2"]\n\n'
        "1. e4 e5 2. Nf3 Nc6 3. Bb5 a6 4. Ba4 Nf6 5. O-O Be7 1-0"
    )
    # Compress the original text using zstandard
    compressor = zstandard.ZstdCompressor()
    compressed_data = compressor.compress(original_text.encode("utf-8"))

    # Write the compressed data to a temporary file
    compressed_file = tmp_path / "test.pgn.zst"
    compressed_file.write_bytes(compressed_data)

    # Specify the output file path for decompression
    output_file = tmp_path / "decompressed.pgn"

    # Call the decompression function
    _decompress_pgn_file(str(compressed_file), str(output_file))

    # Read the output file and verify its content
    decompressed_text = output_file.read_text(encoding="utf-8")
    assert decompressed_text == original_text, (
        f"Expected: {original_text}, Actual: {decompressed_text}"
    )
