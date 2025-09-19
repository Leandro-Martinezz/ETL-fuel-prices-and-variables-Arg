import sys
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

load_dotenv()

def load_data_to_postgresql(df, table_name):
    
    # credentials in a .env file
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')

    conn_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"


    try:  # Create the connection engine
        engine = create_engine(conn_string)
        logging.info("Connection to the database engine successful")
    except Exception as e:
        logging.error(f"Error connecting to the database engine: {e}")
        return

    try:  # Using the to_sql() method to export the DataFrame
        df.to_sql(
            table_name,
            engine,
            if_exists='replace', 
            index=False
        )
        logging.info(f"DataFrame successfully exported to table '{table_name}'.")
    except Exception as e:
        print(f"Error exporting DataFrame: {e}")
