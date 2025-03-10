from pathlib import Path
import pytest
from tempfile import TemporaryDirectory

from python_sf.util.avro_uploader import AvroUploader


class DummyQueryResponse:
    """
    Dummy query response to simulate the collect() method.
    """

    def __init__(self, query: str):
        self.query = query

    def collect(self):
        return f"Executed: {self.query}"


class DummySnowflakeSession:
    """
    Dummy Snowflake session for testing purposes.
    Records SQL queries and returns a dummy query response.
    """

    def __init__(self):
        self.queries = []

    def sql(self, query: str):
        self.queries.append(query)
        return DummyQueryResponse(query)


@pytest.fixture
def dummy_session():
    """
    Pytest fixture that returns a DummySnowflakeSession instance.
    """
    return DummySnowflakeSession()


def test_create_stage_default_without_auto_injest(dummy_session):
    """
    Test that create_stage generates the correct SQL command for the default stage without auto-ingest.
    """
    uploader = AvroUploader(dummy_session, auto_ingest=False)
    uploader.create_stage()
    expected_sql = "CREATE OR REPLACE STAGE RAW_AVRO_STAGE FILE_FORMAT = "
    expected_sql += "(TYPE = 'AVRO')"
    assert dummy_session.queries[0] == expected_sql, (
        f"Expected: {expected_sql}, Actual: {dummy_session.queries[0]}"
    )


def test_create_stage_with_auto_ingest(dummy_session):
    """
    Test that create_stage generates the correct SQL command when auto_ingest is enabled.
    """
    uploader = AvroUploader(dummy_session, auto_ingest=True)
    uploader.create_stage()
    expected_sql = "CREATE OR REPLACE STAGE RAW_AVRO_STAGE FILE_FORMAT = "
    expected_sql += "(TYPE = 'AVRO') AUTO_INGEST = TRUE"
    assert dummy_session.queries[0] == expected_sql, (
        f"Expected: {expected_sql}, Actual: {dummy_session.queries[0]}"
    )

    # Test that create_stage generates the correct SQL command when auto_ingest is disabled.
    uploader = AvroUploader(dummy_session, auto_ingest=False)
    uploader.create_stage()
    expected_sql = "CREATE OR REPLACE STAGE RAW_AVRO_STAGE FILE_FORMAT = "
    expected_sql += "(TYPE = 'AVRO')"
    assert dummy_session.queries[1] == expected_sql, (
        f"Expected: {expected_sql}, Actual: {dummy_session.queries[1]}"
    )
    assert len(dummy_session.queries) == 2


def test_create_pipe(dummy_session):
    """
    Test that create_pipe generates the correct SQL command for creating a Snowpipe.
    """
    uploader = AvroUploader(dummy_session)
    target_table = "CHESS_GAMES"
    uploader.create_pipe(target_table)
    expected_sql = (
        "CREATE OR REPLACE PIPE AVRO_PIPE AS COPY INTO CHESS_GAMES "
        "FROM @RAW_AVRO_STAGE FILE_FORMAT = (TYPE = 'AVRO')"
    )
    assert dummy_session.queries[0] == expected_sql, (
        f"Expected: {expected_sql}, Actual: {dummy_session.queries[0]}"
    )
    assert len(dummy_session.queries) == 1, (
        f"Expected 1 query, Actual: {len(dummy_session.queries)}"
    )
    # Test that the target table name is correctly inserted into the SQL command.
    target_table = "ANOTHER_TABLE"
    uploader.create_pipe(target_table)
    expected_sql = (
        "CREATE OR REPLACE PIPE AVRO_PIPE AS COPY INTO ANOTHER_TABLE "
        "FROM @RAW_AVRO_STAGE FILE_FORMAT = (TYPE = 'AVRO')"
    )
    assert dummy_session.queries[1] == expected_sql, (
        f"Expected: {expected_sql}, Actual: {dummy_session.queries[1]}"
    )
    assert len(dummy_session.queries) == 2, (
        f"Expected 2 queries, Actual: {len(dummy_session.queries)}"
    )


def test_upload_files(dummy_session, monkeypatch):
    """
    Test that upload_files generates the correct PUT commands for each Avro file in the directory.
    """
    # Create a temporary directory and dummy Avro files.
    with TemporaryDirectory() as tmpdirname:
        tmp_dir = Path(tmpdirname)
        # Create two dummy avro files.
        file1 = tmp_dir / "game1.avro"
        file2 = tmp_dir / "game2.avro"
        file1.write_text("dummy data 1")
        file2.write_text("dummy data 2")

        uploader = AvroUploader(dummy_session)
        responses = uploader.upload_files(tmp_dir)

        # Verify that two PUT commands were generated.
        assert len(responses) == 2, f"Expected 2 responses, Actual: {len(responses)}"
        # Check that the queries recorded in dummy_session contain the correct stage name and file paths.
        for file in (file1, file2):
            abs_path = file.resolve()
            expected_sql = f"PUT file://{abs_path} @RAW_AVRO_STAGE"
            assert expected_sql in dummy_session.queries, (
                f"Expected: {expected_sql}, Actual: {dummy_session.queries}"
            )
