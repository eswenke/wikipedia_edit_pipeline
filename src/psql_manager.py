import os
from dotenv import load_dotenv
from datetime import datetime
import psycopg2


class PSQLManager:
    """Manages PostgreSQL connection, raw-event persistence, and retention tasks."""

    def __init__(self):
        """Load PostgreSQL connection settings from environment variables."""
        load_dotenv()
        self.dbname = os.getenv("PSQL_DBNAME")
        self.user = os.getenv("PSQL_USER")
        self.password = os.getenv("PSQL_PASSWORD")
        self.port = os.getenv("PSQL_PORT", "5432")
        self.host = os.getenv("PSQL_HOST", "localhost")
        self.today = datetime.now().strftime("%m-%d-%Y")
        self.conn = None

    def connect(self):
        """Open a PostgreSQL connection using configured credentials."""
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname, user=self.user, port=self.port, password=self.password, host=self.host
            )
        except psycopg2.Error as e:
            print(f"psql connection error: {e}")
            exit(1)

    def process_event(self, json_data):
        """Insert one Wikimedia event into raw_events."""
        try:
            cur = self.conn.cursor()
            cur.execute(
                """
                INSERT INTO raw_events 
                    (id, 
                    domain, 
                    dt, 
                    type, 
                    namespace, 
                    title, 
                    comment, 
                    "user", 
                    wiki, 
                    minor, 
                    patrolled, 
                    log_type,
                    length,
                    bot)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    json_data.get("meta", {}).get("id"),
                    json_data.get("meta", {}).get("domain"),
                    json_data.get("meta", {}).get("dt"),
                    json_data.get("type"),
                    json_data.get("namespace"),
                    json_data.get("title"),
                    json_data.get("comment"),
                    json_data.get("user"),
                    json_data.get("wiki"),
                    json_data.get("minor"),
                    json_data.get("patrolled"),
                    json_data.get("log_type"),
                    (  # store edit size delta when length object is present
                        json_data.get("length").get("new") - json_data.get("length").get("old", 0)
                        if "length" in json_data
                        else None
                    ),
                    json_data.get("bot"),
                ),
            )

        except psycopg2.IntegrityError as e:
            print(f"integrity error: {e}")
            self.conn.rollback()
            return False
        except Exception as e:
            print(f"error processing event JSON: {e}")
            self.conn.rollback()
            return False

        finally:
            self.conn.commit()
            cur.close()

        return True

    def print_events(self):
        """Print total count of rows currently stored in raw_events."""
        try:
            if self.conn:
                cur = self.conn.cursor()
                cur.execute("SELECT COUNT(*) FROM raw_events")
                count = cur.fetchone()[0]
                print("\n=== CHANGE EVENTS ===")
                print(f"total: {count}")
                cur.close()
            else:
                print("error: not connected to psql db")
                return
        except Exception as e:
            print(f"error getting event count: {e}")
        finally:
            if self.conn:
                self.conn.close()

    def prune_old_raw_events(self, retention_hours):
        """Delete raw_events rows older than retention_hours."""
        try:
            if not self.conn:
                print("error: not connected to psql db")
                return False

            cur = self.conn.cursor()
            cur.execute(
                """
                DELETE FROM raw_events
                WHERE dt < NOW() - (%s * INTERVAL '1 hour')
                """,
                (retention_hours,),
            )
            deleted_rows = cur.rowcount
            self.conn.commit()
            cur.close()
            print(f"pruned old raw events: {deleted_rows} rows removed")
            return True

        except Exception as e:
            print(f"error pruning old raw events: {e}")
            self.conn.rollback()
            return False

    def setup_db(self):
        """Placeholder for database bootstrap workflow."""
        return

        try:
            self.connect()
            cur = self.conn.cursor()

            # Execute schema script against the currently connected database.
            with open("psql_setup.sql", "r") as f:
                cur.execute(f.read())
            self.conn.commit()
            cur.close()
            self.conn.close()

        except Exception as e:
            print(f"psql db setup error: {e}")
            self.conn.rollback()
            self.conn.close()
            exit(1)

    def truncate_db(self):
        """Remove all rows from raw_events."""
        try:
            self.connect()
            cur = self.conn.cursor()
            cur.execute("TRUNCATE TABLE raw_events")
            self.conn.commit()
            print("raw_events table truncated successfully")

            cur.close()
            self.conn.close()
        except Exception as e:
            print(f"error truncating raw_events table: {e}")
            if self.conn:
                self.conn.close()
            exit(1)
