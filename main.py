import python_sf as sf
from python_sf.chess import preprocess_pgn, download_single_pgn_file

if __name__ == "__main__":

    # download_single_pgn_file(2017, 5)
    preprocess_pgn("lichess_2017-05.pgn.zst")
    # with sf.SnowflakeSession() as session:
    #     result = session.sql('select 1 as col1, 2 as col2')
    #     print(result.collect())
