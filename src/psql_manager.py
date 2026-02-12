import os
from dotenv import load_dotenv
from datetime import datetime
import psycopg2

# need psql imports

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
# 1. download psql locally, set up a db with the GUI, test, then set up again script and test
# 2. flesh out psql manager (connect to local db, create tables, insert raw events and metrics / views)
# 3. download docker and set up containers for redis, psql, and the app

# NEED TO BE CONSISTENT WITH ERROR HANDLING


class PSQLManager:
    def __init__(self):
        load_dotenv()
        self.dbname = os.getenv("PSQL_DBNAME")
        self.user = os.getenv("PSQL_USER")
        self.password = os.getenv("PSQL_PASSWORD")
        self.port = os.getenv("PSQL_PORT", "5432")
        self.today = datetime.now().strftime("%m-%d-%Y")  # check notes on this
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname, user=self.user, port=self.port, password=self.password
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
                    user, 
                    bot, 
                    wiki, 
                    minor, 
                    length, 
                    patrolled, 
                    log_type) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    json_data["id"],
                    json_data["meta"]["uri"],
                    json_data["domain"]["domain"],
                    json_data["dt"],
                    json_data["type"],
                    json_data["namespace"],
                    json_data["title"],  # during a block, the title will be the user who is blocked
                    json_data["title_url"],
                    json_data["comment"],
                    json_data["user"],
                    json_data["bot"],
                    json_data["wiki"],
                    json_data.get("minor"),
                    json_data.get("length"),
                    json_data.get("patrolled"),
                    json_data.get("log_type"),
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

    def setup_db(self):
        try:
            # connect to db
            # create cursor
            # execute sql file
            # close cursor
            # close connection
            pass
        except Exception as e:
            print(f"psql setup error: {e}")
            exit(1)

    def close(self):
        self.conn.close()
