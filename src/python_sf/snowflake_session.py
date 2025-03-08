from snowflake.snowpark import Session
from python_sf.snowflake_connection import SnowflakeConnectionConfig


class SnowflakeSession:
    def __init__(self):
        self.session = None
        self.config = SnowflakeConnectionConfig()
        self.is_started = False

    def start(self):
        """Start a snowflake session with the given configuration."""
        self.session = Session.builder.configs(self.config.to_dict()).create()
        self.is_started = True

    def stop(self):
        if self.is_started:
            self.session.close()
            self.is_started = False

    def __call__(self):
        return self.session

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False
