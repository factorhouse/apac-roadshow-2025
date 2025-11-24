import argparse
import sys
import logging
import os
import psycopg2
from dotenv import load_dotenv
from pathlib import Path
from psycopg2 import sql, errors

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

# --- Path and Environment Setup ---
script_dir = Path(__file__).resolve().parent
SQL_FILE = script_dir / "postgres-init.sql"
DOTENV_FILE = script_dir.parent / ".env"
load_dotenv(dotenv_path=DOTENV_FILE)

# --- Load Configuration from Environment ---
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# --- List of objects to manage for schema-only operations ---
TABLES_TO_MANAGE = [
    "products",
    "customers",
    "orders",
    "order_items",
    "inventory",
    "product_views",
]
PUBLICATION_NAME = "workshop_cdc"
TRIGGER_FUNCTION_NAME = "update_updated_at_column"


def get_connection(dbname_override=None):
    """Establishes a connection to the PostgreSQL database."""
    db_config = {
        "dbname": dbname_override if dbname_override else DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "host": DB_HOST,
        "port": DB_PORT,
    }
    try:
        return psycopg2.connect(**db_config)
    except psycopg2.OperationalError as e:
        logger.error(f"Could not connect to database '{db_config['dbname']}': {e}")
        logger.error(
            "Please ensure the service is running and .env details are correct."
        )
        sys.exit(1)


def create_db():
    """
    Creates the PostgreSQL database but does not initialize the schema.
    Requires database creation privileges.
    """
    logger.info(f"Attempting to create database '{DB_NAME}'...")
    conn_admin = None
    try:
        conn_admin = get_connection(dbname_override="postgres")
        conn_admin.autocommit = True
        with conn_admin.cursor() as cur:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
            logger.info(f"Database '{DB_NAME}' created successfully.")
    except errors.DuplicateDatabase:
        logger.warning(f"Database '{DB_NAME}' already exists. No action taken.")
    except Exception as e:
        logger.error(f"An error occurred while trying to create the database: {e}")
        sys.exit(1)
    finally:
        if conn_admin:
            conn_admin.close()


def init_schema():
    """
    Applies the schema and data from the SQL file to the configured database.
    This function assumes the database already exists.
    """
    logger.info(f"Connecting to database '{DB_NAME}' to initialize schema...")
    try:
        with open(SQL_FILE, "r") as f:
            sql_script = "\n".join(
                line
                for line in f.read().split("\n")
                if not line.strip().startswith("\\")
            )
    except FileNotFoundError:
        logger.error(f"Initialization script '{SQL_FILE}' not found.")
        sys.exit(1)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            logger.info(f"Applying schema and data from '{SQL_FILE}'...")
            cur.execute(sql_script)
        conn.commit()
        logger.info("Database schema and data applied successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"An error occurred during schema initialization: {e}")
        sys.exit(1)
    finally:
        conn.close()


def teardown_schema():
    """
    Tears down the schema (tables, publication, etc.) within the database
    without dropping the database itself.
    """
    logger.info(f"Connecting to database '{DB_NAME}' to tear down schema...")
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            logger.info(f"Dropping publication '{PUBLICATION_NAME}'...")
            cur.execute(
                sql.SQL("DROP PUBLICATION IF EXISTS {}").format(
                    sql.Identifier(PUBLICATION_NAME)
                )
            )

            logger.info(f"Dropping tables: {', '.join(TABLES_TO_MANAGE)}...")
            for table in reversed(TABLES_TO_MANAGE):
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                        sql.Identifier(table)
                    )
                )

            logger.info(f"Dropping trigger function '{TRIGGER_FUNCTION_NAME}'...")
            cur.execute(
                sql.SQL("DROP FUNCTION IF EXISTS {}()").format(
                    sql.Identifier(TRIGGER_FUNCTION_NAME)
                )
            )

        conn.commit()
        logger.info("Database schema torn down successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"An error occurred during schema teardown: {e}")
        sys.exit(1)
    finally:
        conn.close()


def up():
    """
    Ensures the database exists (creating it if necessary) and then initializes the schema.
    """
    create_db()
    init_schema()


def down():
    """
    Tears down the entire database specified in the .env file.
    """
    logger.info(f"Dropping database '{DB_NAME}'...")
    conn_admin = get_connection(dbname_override="postgres")
    conn_admin.autocommit = True

    try:
        with conn_admin.cursor() as cur:
            command = sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(DB_NAME)
            )
            logger.info("Executing drop command...")
            cur.execute(command)
        logger.info(f"Database '{DB_NAME}' dropped successfully.")
    except Exception as e:
        logger.error(f"An error occurred during 'down' operation: {e}")
        sys.exit(1)
    finally:
        conn_admin.close()


if __name__ == "__main__":
    if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
        raise ValueError(
            "PostgreSQL credentials not found in the .env file of the project root directory."
        )

    parser = argparse.ArgumentParser(description="PostgreSQL Database & Schema Manager")
    parser.add_argument(
        "--action",
        choices=["up", "down", "init", "teardown", "create-db"],
        default="up",
        help=(
            "'up': Create DB and schema (default). "
            "'down': Drop entire DB. "
            "'init': Create schema in existing DB. "
            "'teardown': Drop schema from existing DB. "
            "'create-db': Create the database only."
        ),
    )
    args = parser.parse_args()

    action_map = {
        "up": up,
        "down": down,
        "init": init_schema,
        "teardown": teardown_schema,
        "create-db": create_db,
    }
    # Execute the chosen action
    action_map[args.action]()
