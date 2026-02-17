import os
from dotenv import load_dotenv
from datetime import datetime
import psycopg2
import json


# gonna end up containerizing everything, maybe start out with
# just redis, psql, and the python app. full services yaml will
# probably look like this:
# services:
#   postgres:  # Database
#   redis:     # Cache
#   app:       # The pipeline
#   dashboard: # Streamlit? something basic with react? plotly?
#   grafana:   # Monitoring

# steps:
# 1. [DONE*] download psql locally, set up a db with the GUI, test, *then set up again script and test
# 2. [DONE*]flesh out psql manager (connect to local db, create tables, insert raw events and *metrics / views)
# 3. [IN PROGRESS]download docker and set up containers for redis, psql, and the app

# NEED TO BE CONSISTENT WITH ERROR HANDLING


class PSQLManager:
    def __init__(self):
        load_dotenv()
        self.dbname = os.getenv("PSQL_DBNAME")
        self.user = os.getenv("PSQL_USER")
        self.password = os.getenv("PSQL_PASSWORD")
        self.port = os.getenv("PSQL_PORT", "5432")
        self.host = os.getenv("PSQL_HOST", "localhost")
        self.today = datetime.now().strftime("%m-%d-%Y")  # check notes on this
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname, user=self.user, port=self.port, password=self.password, host=self.host
            )
        except psycopg2.Error as e:
            print(f"psql connection error: {e}")
            exit(1)

    def process_event(self, json_data):
        try:
            cur = self.conn.cursor()
            cur.execute(
                """
                INSERT INTO raw_events 
                    (id, 
                    uri, 
                    domain, 
                    dt, 
                    type, 
                    namespace, 
                    title, 
                    title_url, 
                    comment, 
                    "user", 
                    bot, 
                    wiki, 
                    minor, 
                    patrolled, 
                    log_type,
                    length)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    json_data["meta"]["id"],
                    json_data["meta"]["uri"],
                    json_data["meta"]["domain"],
                    json_data["meta"]["dt"],
                    json_data["type"],
                    json_data["namespace"],
                    json_data["title"],  # during a block, the title will be the user who is blocked
                    json_data["title_url"],
                    json_data["comment"],
                    json_data["user"],
                    json_data["bot"],
                    json_data["wiki"],
                    json_data.get("minor"),
                    json_data.get("patrolled"),
                    json_data.get("log_type"),
                    (  # length change (jsonb data type being weird in psql, made it an int for now)
                        json_data.get("length").get("new") - json_data.get("length").get("old", 0)
                        if "length" in json_data
                        else None
                    ),
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
        # print out the number of raw events inserted in raw_events table
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

    def setup_db(self):
        # will finish this when containerizing
        # current connect() assumes a created db and errors if not, so the current logic will not work
        # container will first assume there is none, so should check and then create if not
        return

        try:
            # connect to db
            self.connect()

            # create cursor
            cur = self.conn.cursor()

            # execute sql file
            with open("psql_setup.sql", "r") as f:
                cur.execute(f.read())

            # commit
            self.conn.commit()

            # close cursor
            cur.close()

            # close connection
            self.conn.close()

        except Exception as e:
            # rollback in case of error
            print(f"psql db setup error: {e}")
            self.conn.rollback()
            self.conn.close()
            exit(1)

    def truncate_db(self):
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
            self.conn.close()
            exit(1)
