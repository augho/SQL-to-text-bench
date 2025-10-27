from src.lib.Config import OutputFormat
from src.profiling.ProfilingConfig import ProfilingConfig
from src.lib.SqliteConnector import SqliteConnector
import pytest


@pytest.fixture(scope="session")
def db():
    print("Starting initialization...")
    db_conn_string = "db/Chinook.db"
    test_profiling_config = ProfilingConfig(
        DB_CONN_STRING=db_conn_string,
        DO_LOGGING=False,
        MAX_RPM=9,
        OUTPUT_PATH="out/test",
        OUTPUT_FORMAT=OutputFormat.JSON,
        DO_EXTRACTION=False,
        DO_LLM_SUMMARY=False,
        SAVE_METADATA=False,
        DRY_RUN=True,
        SKIP_INTERACTIONS=True
    )
    ProfilingConfig.init(test_profiling_config)

    db: SqliteConnector = SqliteConnector(db_conn_string)
    yield db

    print("Cleaning up...")
