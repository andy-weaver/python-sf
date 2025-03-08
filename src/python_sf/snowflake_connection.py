from dotenv import load_dotenv
import os
from dataclasses import dataclass
from typing import Optional

load_dotenv()


@dataclass
class SnowflakeConnectionConfig:
    account: Optional[str] = None
    user: Optional[str] = None
    role: Optional[str] = None
    warehouse: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    password: Optional[str] = None

    def __post_init__(self):
        self.load()

    def load(self):
        self.account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = os.getenv("SNOWFLAKE_USER")
        self.role = os.getenv("SNOWFLAKE_ROLE")
        self.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
        self.database = os.getenv("SNOWFLAKE_DATABASE")
        self.schema = os.getenv("SNOWFLAKE_SCHEMA")
        self.password = os.getenv("SNOWFLAKE_PASSWORD")

    def to_dict(self) -> dict[str, Optional[str]]:
        return {
            "account": self.account or "",
            "user": self.user or "",
            "role": self.role or "",
            "warehouse": self.warehouse or "",
            "database": self.database or "",
            "schema": self.schema or "",
            "password": self.password or "",
        }
