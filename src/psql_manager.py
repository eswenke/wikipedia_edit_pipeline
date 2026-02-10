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

# finish out psql manager, test locally on limited metrics, then
# containerize everything so far


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
