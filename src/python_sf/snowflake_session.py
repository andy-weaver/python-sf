import snowflake.snowpark as sp
from python_sf.snowflake_connection import SnowflakeConnectionConfig
from typing import Optional


class SnowflakeSession:
    """A context manager wrapper around the Snowflake Snowpark Session object.
    
    This class uses a SnowflakeConnectionConfig to create a Snowflake Snowpark Session,
    allowing users to utilize the session in a with-statement block.
    
    Examples
    --------
    >>> with SnowflakeSession() as session:
    ...     result = session.sql("SELECT 1").collect()
    """
    def __init__(self, config: Optional[SnowflakeConnectionConfig] = None):
        """
        Initialize the SnowflakeSession.
        
        Parameters
        ----------
        config : SnowflakeConnectionConfig, optional
            The configuration object containing Snowflake connection parameters.
            If not provided, a new SnowflakeConnectionConfig is instantiated.
        """
        if config is None:
            config = SnowflakeConnectionConfig()
        self.config = config
        self._session: Optional[sp.Session] = None

    def __enter__(self) -> sp.Session:
        """
        Enter the runtime context and create a Snowflake Snowpark Session.
        
        Returns
        -------
        sp.Session
            An instance of the Snowflake Snowpark Session.
        """
        connection_params = self.config.to_dict()
        self._session = sp.Session.builder.configs(connection_params).create()
        return self._session

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """
        Exit the runtime context and close the Snowflake Snowpark Session.
        
        Parameters
        ----------
        exc_type : type
            The exception type.
        exc_value : Exception
            The exception value.
        traceback : TracebackType
            The traceback.
        """
        if self._session is not None:
            self._session.close()
            self._session = None