import os
import zstandard
import fastavro

from python_sf.chess.preprocess_pgn_file import (
    _split_pgn_games,
    _convert_pgn_to_dict,
    _get_avro_schema,
    _write_avro_file,
    _add_game_id_to_list_of_files,
    preprocess_pgn,
)


def test_split_pgn_games():
    """Test that PGN data is split into individual games based on headers and results."""
    sample_data = (
        '[Event "Game1"]\n[Site "Site1"]\n\n1. e4 e5 1-0\n\n'
        '[Event "Game2"]\n[Site "Site2"]\n\n1. d4 d5 0-1'
    )
    games = _split_pgn_games(sample_data)
    assert isinstance(games, list), (
        f"Expected games to be a list, Actual: {type(games)}"
    )
    assert len(games) == 2, f"Expected 2 games, Actual: {len(games)}"
    for game in games:
        assert game.startswith("[Event"), (
            f"Expected game to start with '[Event', Actual: {game[:10]}"
        )
        assert any(game.strip().endswith(res) for res in ["1-0", "0-1", "1/2-1/2"]), (
            f"Expected game to end with a result (1-0, 0-1, 1/2-1/2), Actual: {game[-5:]}"
        )


def test_convert_pgn_to_dict():
    """Test that a PGN game string is converted into a dictionary with headers, moves, and a unique game_id."""
    sample_game = (
        '[Event "Test Event"]\n[Site "Test Site"]\n[Date "2025.03.10"]\n\n1. e4 e5 1-0'
    )
    structured = _convert_pgn_to_dict([sample_game])
    assert isinstance(structured, list), (
        f"Expected structured to be a list, Actual: {type(structured)}"
    )
    assert len(structured) == 1, f"Expected 1 game, Actual: {len(structured)}"
    game_dict = structured[0]
    game_id = game_dict.get("game_id")
    assert game_id is not None, f"Expected 'game_id' to be a UUID, Actual: {game_id}"
    assert game_dict.get("Event") == "Test Event", (
        f"Expected 'Event' to be 'Test Event', Actual: {game_dict.get('Event')}"
    )
    assert game_dict.get("Site") == "Test Site", (
        f"Expected 'Site' to be 'Test Site', Actual: {game_dict.get('Site')}"
    )
    assert game_dict.get("Date") == "2025.03.10", (
        f"Expected 'Date' to be '2025.03.10', Actual: {game_dict.get('Date')}"
    )
    assert "moves" in game_dict and game_dict["moves"].startswith("1. e4"), (
        f"Expected 'moves' to start with '1. e4', Actual: {game_dict.get('moves')}"
    )
    game_id = game_dict.get("game_id")
    assert isinstance(game_id, str), (
        f"Expected 'game_id' to be a string, Actual: {type(game_id)}"
    )
    assert str(game_id) == game_id, (
        f"Expected 'game_id' to be a valid UUID, Actual: {game_id}"
    )
    assert len(game_id) == 36, (
        f"Expected 'game_id' to be a valid UUID, Actual: {game_id}"
    )
    assert "-" in game_id, f"Expected 'game_id' to be a valid UUID, Actual: {game_id}"
    game_id = str(game_id)


def test_get_avro_schema():
    """Test that the Avro schema is generated from the keys of the first game."""
    sample_game = {
        "Event": "Test Event",
        "Site": "Test Site",
        "Date": "2025.03.10",
        "moves": "1. e4 e5 1-0",
        "game_id": "dummy-uuid",
    }
    schema = _get_avro_schema([sample_game])
    assert isinstance(schema, dict), (
        f"Expected schema to be a dict, Actual: {type(schema)}"
    )
    assert schema.get("namespace") == "com.example.pgn", (
        f"Expected namespace to be 'com.example.pgn', Actual: {schema.get('namespace')}"
    )
    assert schema.get("type") == "record", (
        f"Expected type to be 'record', Actual: {schema.get('type')}"
    )
    assert schema.get("name") == "Game", (
        f"Expected name to be 'Game', Actual: {schema.get('name')}"
    )
    fields = schema.get("fields")
    for key in sample_game.keys():
        assert any(field["name"] == key for field in fields), (
            f"Expected field name to be '{key}', Actual: {fields}"
        )


def test_add_game_id_to_list_of_files(tmp_path):
    """Test that _add_game_id_to_list_of_files writes game IDs to the 'games' file once."""
    sample_games = [
        {"game_id": "uuid-1", "Event": "Test Event", "moves": "1. e4 e5 1-0"},
        {"game_id": "uuid-2", "Event": "Test Event", "moves": "1. d4 d5 0-1"},
    ]
    orig_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        games_file = tmp_path / "games"
        if games_file.exists():
            games_file.unlink()
        _add_game_id_to_list_of_files(sample_games)
        content = games_file.read_text().splitlines()
        assert len(content) == len(sample_games), (
            f"Expected {len(sample_games)} lines, Actual: {len(content)}"
        )
        assert "uuid-1" in content, f"Expected 'uuid-1' in content, Actual: {content}"
        assert "uuid-2" in content, f"Expected 'uuid-2' in content, Actual: {content}"
    finally:
        os.chdir(orig_cwd)


def test_write_avro_file(tmp_path):
    """Test that each game is written to its own Avro file and that the 'games' file is updated once."""
    sample_games = [
        {"Event": "Test Event", "moves": "1. e4 e5 1-0", "game_id": "test-uuid-1"},
        {"Event": "Test Event", "moves": "1. d4 d5 0-1", "game_id": "test-uuid-2"},
    ]
    schema = _get_avro_schema(sample_games)
    orig_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        games_list_file = tmp_path / "games"
        if games_list_file.exists():
            games_list_file.unlink()
        _write_avro_file(sample_games, schema)
        for game in sample_games:
            output_file = tmp_path / f"{game['game_id']}.avro"
            assert output_file.exists(), f"Expected {output_file} to exist"
            with open(output_file, "rb") as fo:
                reader = fastavro.reader(fo)
                records = list(reader)
                assert len(records) == 1, f"Expected 1 record, Actual: {len(records)}"
                for key in game:
                    assert records[0][key] == game[key], (
                        f"Expected {key} to be {game[key]}, Actual: {records[0][key]}"
                    )
        assert games_list_file.exists(), f"Expected {games_list_file} to exist"
        content = games_list_file.read_text().splitlines()
        # Now, one line per game is expected.
        expected_lines = len(sample_games)
        assert len(content) == expected_lines, (
            f"Expected {expected_lines} lines, Actual: {len(content)}"
        )
        assert "test-uuid-1" in content, (
            f"Expected 'test-uuid-1' in content, Actual: {content}"
        )
        assert "test-uuid-2" in content, (
            f"Expected 'test-uuid-2' in content, Actual: {content}"
        )
    finally:
        os.chdir(orig_cwd)


def test_preprocess_pgn(tmp_path):
    """
    Test the complete preprocessing pipeline using a compressed PGN file.
    """
    sample_pgn = (
        '[Event "Complete Test"]\n[Site "Test Site"]\n[Date "2025.03.10"]\n\n'
        "1. e4 e5 1-0"
    )
    compressor = zstandard.ZstdCompressor()
    compressed_data = compressor.compress(sample_pgn.encode("utf-8"))
    pgn_file = tmp_path / "sample.pgn.zst"
    pgn_file.write_bytes(compressed_data)

    orig_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        preprocess_pgn(str(pgn_file))
        games_file = tmp_path / "games"
        assert games_file.exists(), f"Expected {games_file} to exist"
        game_ids = games_file.read_text().splitlines()
        assert len(game_ids) == 1, f"Expected 1 line, Actual: {len(game_ids)}"
        game_id = game_ids[0]
        avro_file = tmp_path / f"{game_id}.avro"
        assert avro_file.exists(), f"Expected {avro_file} to exist"
        with open(avro_file, "rb") as fo:
            records = list(fastavro.reader(fo))
            assert len(records) == 1, f"Expected 1 record, Actual: {len(records)}"
            assert records[0].get("Event") == "Complete Test", (
                f"Expected 'Event' to be 'Complete Test', Actual: {records[0].get('Event')}"
            )
    finally:
        os.chdir(orig_cwd)
