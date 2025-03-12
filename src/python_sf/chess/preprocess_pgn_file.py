import re
import zstandard
import fastavro
from uuid import uuid4
from tqdm import tqdm
from pathlib import Path
from python_sf._pgn_parser import extract_tags, extract_moves, tag_to_dict


def preprocess_pgn(pgn_file: str) -> None:
    """
    Preprocess a raw PGN file and convert the format to a structured Avro file.

    This function decompresses the PGN file, splits it into games, converts each game
    into a dictionary, generates an Avro schema based on the first game, writes
    each game to its own Avro file (named with a unique game_id), and then writes
    a file named "games" that contains all the game IDs.

    Parameters
    ----------
    pgn_file : str
        The path to the compressed PGN file.

    Returns
    -------
    None
    """
    in_file = Path(pgn_file)
    if in_file.suffix == ".zst":
        pgn_file = str(in_file)
    else:
        raise ValueError("Input file must be a zstd compressed PGN file.")

    curr_dir = Path.cwd()
    out_file = Path(pgn_file).stem + ".pgn"

    data = _decompress_pgn_file(in_file, curr_dir / out_file)
    decompressed_file = curr_dir / out_file
    with open(decompressed_file, "r") as f:
        data = f.read()

    games = _split_pgn_games(data)
    structured_games = _convert_pgn_to_dict(games)
    schema = _get_avro_schema(structured_games)
    _write_avro_file(structured_games, schema)


def _decompress_pgn_file(pgn_file: str, output_file: str = "decompressed.pgn") -> None:
    """
    Decompress a zstd compressed PGN file using a streaming decompressor.

    This function reads the compressed file in chunks and writes out the decompressed data
    to a specified output file. This approach is memory efficient and handles multi-frame
    compressed files correctly.

    Parameters
    ----------
    pgn_file : str
        The path to the compressed PGN file.
    output_file : str, optional
        The path where the decompressed PGN text file should be written, by default "decompressed.pgn"

    Returns
    -------
    None
    """
    dctx = zstandard.ZstdDecompressor()
    chunk_size = 16384  # 16 KB chunk size
    with open(pgn_file, "rb") as compressed, open(output_file, "wb") as out:
        with dctx.stream_reader(compressed) as reader:
            while True:
                chunk = reader.read(chunk_size)
                if not chunk:
                    break
                out.write(chunk)


def _split_pgn_games(data: str) -> list[str]:
    """
    Split the decompressed PGN data into individual games.

    Each game is identified by starting with "[Event" and ending with one of the results:
    "1-0", "0-1", or "1/2-1/2".

    Parameters
    ----------
    data : str
        The decompressed PGN data.

    Returns
    -------
    list of str
        A list where each element is a PGN game string.
    """
    pattern = re.compile(r"(\[Event.*?(?:1-0|0-1|1/2-1/2))", re.DOTALL)
    games = pattern.findall(data)
    return games


def _extract_tags_from_game(game: str) -> dict:
    """
    Extract the header tags from a PGN game string.

    Parameters
    ----------
    game : str
        A PGN game string.

    Returns
    -------
    dict
        A dictionary of header tags and values.
    """
    tags = extract_tags(game)
    tags = [(tag_to_dict(tag)["name"], tag_to_dict(tag)["value"]) for tag in tags]
    moves = tag_to_dict(extract_moves(game))
    print(f"tags: {tags}")
    print(f"moves: {moves}")
    out = {}
    for tag in tags:
        out[tag[0]] = tag[1]
    out["moves"] = moves["value"]
    print(f"out: {out}")
    return out


def _convert_pgn_to_dict(games: list[str]) -> list[dict]:
    """
    Convert a list of PGN game strings into a structured dictionary format.

    Each game is split into header tags (enclosed in square brackets) and the moves.
    The returned dictionary contains header keys and a "moves" key for the move text.

    Parameters
    ----------
    games : list of str
        A list of PGN game strings.

    Returns
    -------
    list of dict
        A list where each dictionary represents a game with header fields, moves,
        and a unique game_id.
    """
    structured_games = []
    header_pattern = re.compile(r'\[(\w+)\s+"(.*?)"\]')
    for game in tqdm(games, desc="Converting PGN to Dict"):
        game_dict = {}
        headers = header_pattern.findall(game)
        for tag, value in headers:
            game_dict[tag] = value
        parts = game.split("\n\n", 1)
        if len(parts) == 2:
            moves = parts[1].strip()
        else:
            moves = "\n".join(
                line for line in game.splitlines() if not line.startswith("[")
            ).strip()
        game_dict["moves"] = moves
        game_dict["game_id"] = str(uuid4())
        structured_games.append(game_dict)
    return structured_games


def _get_avro_schema(structured_games: list[dict]) -> dict:
    """
    Generate an Avro schema based on the keys from the first structured game.

    Assumes that every field (including moves and game_id) is of type string.

    Parameters
    ----------
    structured_games : list of dict
        A list of structured game dictionaries.

    Returns
    -------
    dict
        An Avro schema dictionary.

    Raises
    ------
    ValueError
        If no games are provided.
    """
    if not structured_games:
        raise ValueError("No games available to generate schema.")
    fields = [{"name": key, "type": "string"} for key in structured_games[0].keys()]
    schema = {
        "namespace": "com.example.pgn",
        "type": "record",
        "name": "Game",
        "fields": fields,
    }
    return schema


def _add_game_id_to_list_of_files(structured_games: list[dict]) -> None:
    """
    Write a file named 'games' containing all game IDs, one per line.

    Parameters
    ----------
    structured_games : list of dict
        A list of structured game dictionaries.

    Returns
    -------
    None
    """
    with open("games", "w") as f:
        for game in tqdm(structured_games, desc="Writing Game IDs"):
            game_id = game.get("game_id", "unknown")
            f.write(f"{game_id}\n")


def _write_avro_file(structured_games: list[dict], schema: dict) -> None:
    """
    Write the structured game records to individual Avro files.

    Each game is written to its own Avro file named after its unique game ID.
    After processing all games, a single 'games' file is generated that records all game IDs.

    Parameters
    ----------
    structured_games : list of dict
        A list of structured game dictionaries.
    schema : dict
        The Avro schema to use for writing the records.

    Returns
    -------
    None
    """
    for game in tqdm(structured_games, desc="Writing Avro Files"):
        game_id = game.get("game_id", "unknown")
        output_file = f"{game_id}.avro"
        with open(output_file, "wb") as out:
            fastavro.writer(out, schema, [game])
    _add_game_id_to_list_of_files(structured_games)
