import sys
import os
import pandas as pd
from sqlalchemy import create_engine
import pytest
from dotenv import load_dotenv
import logging

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from etl.load import load_data_to_postgresql

load_dotenv()

@pytest.fixture
def test_df():
    return pd.DataFrame({'id': [1, 2], 'data': ['A', 'B']})

def test_load_data(test_df):

    table_name = "test_table_simple"

    try:
        load_data_to_postgresql(test_df, table_name)
    except Exception as e:
        logging.error(f"The load function failed unexpectedly: {e}")

    try:
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_name = os.getenv('DB_NAME')
        conn_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(conn_string)
        
        df_from_db = pd.read_sql_table(table_name, engine)

        assert len(test_df) == len(df_from_db)
        
        assert True

    except Exception as e:
        pytest.fail(f"Error during database verification: {e}")