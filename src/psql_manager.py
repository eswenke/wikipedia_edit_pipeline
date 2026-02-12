import os
from dotenv import load_dotenv
from datetime import datetime

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


class PSQLManager:
    def __init__(self):
        load_dotenv()
        self.host = os.getenv("PSQL_HOST", "localhost")
        self.port = int(os.getenv("PSQL_PORT", 5432))
        self.user = os.getenv("PSQL_USER", "default")
        self.password = os.getenv("PSQL_PASSWORD", None)
        self.client = None
        self.today = datetime.now().strftime("%m-%d-%Y")  # check notes on this

    def connect(self):
        try:
            self.client = (self.host, self.port, self.user, self.password)
        except Exception as e:
            print(f"psql connection error: {e}")
            exit(1)

    def setup_db(self):
        try:
            # import psycopg2 up top
            # connect to db
            # create cursor
            # execute sql file
            # close cursor
            # close connection
            pass
        except Exception as e:
            print(f"psql setup error: {e}")
            exit(1)
