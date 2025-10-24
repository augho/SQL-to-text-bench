from src.CliConfig import CliConfig, OutputFormat
from src.SqliteConnector import SqliteConnector
import pytest


@pytest.fixture(scope="session")
def db():
    print("Starting initialization...")
    CliConfig.advanced_init(
        filename="db/Chinook.db",
        do_logging=False,
        max_rpm=9,
        output_path="out/test",
        output_format=OutputFormat.JSON,
        do_extraction=False,
        do_llm_summary=False,
        save_metadata=False,
        dry_run=True,
    )
    db: SqliteConnector = SqliteConnector("db/Chinook.db")
    yield db

    print("Cleaning up...")
