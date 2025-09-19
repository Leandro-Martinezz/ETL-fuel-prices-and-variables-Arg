import os
import sys
import requests
import logging
from dotenv import load_dotenv
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

load_dotenv()

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

#  this function extracts data from a URL, with logging and exception handling.
def extract_api(url, params=None, dict_key=None):
    try :
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        logging.info("API data successfully obtained")

        if dict_key in data:
            return pd.DataFrame(data[dict_key])
        else:
            return pd.DataFrame(data)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        return None
    except KeyError as e:
        logging.error(f"Key '{dict_key}' not found in response: {e}")
        return None


def get_csv(file_csv):
    try: 
        file_readed = pd.read_csv(file_csv, encoding='latin-1')
        logging.info(f"csv {file_csv} successfully obtained")

    except FileNotFoundError as e :
        logging.error(f"cannot find the file {e}")
    return file_readed

