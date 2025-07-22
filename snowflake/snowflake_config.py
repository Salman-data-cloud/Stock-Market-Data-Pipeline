import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user = os.getenv('SNOWFLAKE_USER'),
        password = os.getenv('SNOWFLAKE_PASSWORD'),
        account = os.getenv('SNOWFLAKE_ACCOUNT'),
        database = os.getenv('SNOWFLAKE_DATABASE'),
        schema = os.getenv('SNOWFLAKE_SCHEMA'),
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    )

    return conn
