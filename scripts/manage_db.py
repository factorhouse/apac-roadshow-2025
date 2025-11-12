import argparse
import sys
import logging
import psycopg2
from pathlib import Path
from psycopg2 import sql, errors

# The source SQL file for the 'up' command.
script_dir = Path(__file__).parent
SQL_FILE = Path(__file__).parent / "postgres-init.sql"


def get_connection(args, db_override=None):
    """Establishes a connection to the PostgreSQL database using argparse arguments."""
    db_config = {
        "dbname": db_override if db_override else args.dbname,
        "user": args.user,
        "password": args.password,
        "host": args.host,
        "port": args.port,
    }
    try:
        conn = psycopg2.connect(**db_config)
        return conn
    except psycopg2.OperationalError as e:
        logging.error(
            f"❌ Could not connect to the database (dbname: '{db_config['dbname']}'): {e}"
        )
        logging.error(
            "   Please ensure PostgreSQL is running and connection details are correct."
        )
        sys.exit(1)


def up(args):
    """
    Ensures the database exists, then applies the schema and data from the SQL file.
    """
    # --- Step 1: Connect to the default 'postgres' db to create the target database ---
    logging.info(f"▶️  Ensuring database '{args.dbname}' exists...")
    conn_admin = get_connection(args, db_override="postgres")

    # CREATE DATABASE cannot run inside a transaction, so we use autocommit.
    conn_admin.autocommit = True
    try:
        with conn_admin.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(args.dbname))
            )
            logging.info(f"   - Database '{args.dbname}' created.")
    except errors.DuplicateDatabase:
        logging.info(
            f"   - Database '{args.dbname}' already exists. Skipping creation."
        )
    except Exception as e:
        logging.error(f"❌ An error occurred while creating the database: {e}")
        sys.exit(1)
    finally:
        conn_admin.close()

    logging.info(f"▶️  Applying schema and data from '{SQL_FILE}' to '{args.dbname}'...")
    try:
        with open(SQL_FILE, "r") as f:
            sql_script = f.read()
    except FileNotFoundError:
        logging.error(
            f"❌ Error: SQL file '{SQL_FILE}' not found in the current directory."
        )
        sys.exit(1)

    # Remove psql meta-commands like \c
    cleaned_sql = "\n".join(
        line for line in sql_script.split("\n") if not line.strip().startswith("\\")
    )

    conn_target = get_connection(args)
    conn_target.autocommit = False
    try:
        with conn_target.cursor() as cur:
            cur.execute(cleaned_sql)
        conn_target.commit()
        logging.info("✅ Database schema and data applied successfully.")
    except Exception as e:
        conn_target.rollback()
        logging.error(f"❌ An error occurred during schema application: {e}")
        sys.exit(1)
    finally:
        conn_target.close()


def down(args):
    """
    Tears down the entire database specified.
    """
    logging.info(f"▶️  Dropping database '{args.dbname}'...")

    # Connect to the default 'postgres' database to drop the target database
    conn_admin = get_connection(args, db_override="postgres")
    # DROP DATABASE cannot run inside a transaction, so we use autocommit.
    conn_admin.autocommit = True

    try:
        with conn_admin.cursor() as cur:
            command = sql.SQL("DROP DATABASE IF EXISTS {}").format(
                sql.Identifier(args.dbname)
            )
            logging.info(f"   - Executing: {command.as_string(cur)}")
            cur.execute(command)
        logging.info(f"✅ Database '{args.dbname}' dropped successfully.")
    except Exception as e:
        logging.error(f"❌ An error occurred during 'down' operation: {e}")
        sys.exit(1)
    finally:
        conn_admin.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(
        description="PostgreSQL Database Admin for Workshop"
    )

    parser.add_argument(
        "--action",
        choices=["up", "down"],
        default="up",
        help="Action to perform: 'up' (create DB and schema, default) or 'down' (drop DB)",
    )

    # --- Connection Arguments ---
    parser.add_argument("--host", "-H", required=True, help="Database host")
    parser.add_argument(
        "--port", "-t", default=5432, type=int, help="Database port (default: 5432)"
    )
    parser.add_argument("--user", "-u", required=True, help="Database user")
    parser.add_argument(
        "--password",
        "-p",
        required=True,
        help="Database password",
    )
    parser.add_argument(
        "--dbname", "-d", default="ecommerce", help="Database name (default: ecommerce)"
    )

    args = parser.parse_args()

    if args.action == "up":
        up(args)
    else:
        down(args)
