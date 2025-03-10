"""
Module: avro_uploader.py

This module provides functionality to stage, upload, and automatically process raw Avro files (each representing a single chess game)
into Snowflake using Snowpark objects. It creates a stage with an appropriate AVRO file format, configures a Snowpipe for automatic ingestion,
and iterates over a given directory to upload files using the PUT command.
"""

from pathlib import Path
from typing import Union, List
from python_sf.snowflake_session import SnowflakeSession


class AvroUploader:
    """
    A class to handle the staging, uploading, and processing of raw Avro files in Snowflake.

    Parameters
    ----------
    session : SnowflakeSession
        A Snowflake session wrapper for executing SQL commands.
    stage_name : str, optional
        The name of the Snowflake stage to use, by default "RAW_AVRO_STAGE".
    pipe_name : str, optional
        The name of the Snowflake pipe to create for automatic ingestion, by default "AVRO_PIPE".
    auto_ingest : bool, optional
        Whether to configure the stage for auto-ingestion. Note that auto-ingest requires proper cloud messaging configuration,
        and is typically used for external stages. Default is False.
    """

    def __init__(
        self,
        session: SnowflakeSession,
        stage_name: str = "RAW_AVRO_STAGE",
        pipe_name: str = "AVRO_PIPE",
        auto_ingest: bool = True,
    ):
        self.session = session
        self.stage_name = stage_name
        self.pipe_name = pipe_name
        self.auto_ingest = auto_ingest

    def create_stage(self) -> None:
        """
        Create or replace a Snowflake stage for Avro files with the appropriate file format.
        Optionally configures auto-ingest if enabled.
        """
        auto_ingest_clause = " AUTO_INGEST = TRUE" if self.auto_ingest else ""
        sql = f"CREATE OR REPLACE STAGE {self.stage_name} FILE_FORMAT = "
        sql += f"(TYPE = 'AVRO'){auto_ingest_clause}"
        self.session.sql(sql).collect()

    def create_pipe(self, target_table: str) -> None:
        """
        Create or replace a Snowflake pipe for automatically ingesting data from the stage into a target table.

        Parameters
        ----------
        target_table : str
            The name of the target table to load data into.
        """
        sql = (
            f"CREATE OR REPLACE PIPE {self.pipe_name} AS "
            f"COPY INTO {target_table} "
            f"FROM @{self.stage_name} "
            f"FILE_FORMAT = (TYPE = 'AVRO')"
        )
        self.session.sql(sql).collect()

    def upload_files(self, directory: Union[str, Path]) -> List[str]:
        """
        Upload all Avro files from the specified directory to the Snowflake stage using the PUT command.

        Parameters
        ----------
        directory : Union[str, Path]
            The local directory containing Avro files.

        Returns
        -------
        List[str]
            A list of responses from the PUT commands.
        """
        directory = Path(directory)
        responses = []
        for file_path in directory.glob("*.avro"):
            abs_path = file_path.resolve()
            # The PUT command requires a file:// URL with the absolute file path.
            sql = f"PUT file://{abs_path} @{self.stage_name}"
            response = self.session.sql(sql).collect()
            responses.append(response)
        return responses
