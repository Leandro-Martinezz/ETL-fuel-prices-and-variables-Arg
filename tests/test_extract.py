import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from unittest.mock import patch
import pandas as pd
from etl.extract import extract_api, get_csv


@patch('etl.extract.requests.get')
def test_extract_api_mock(mock_get):

    mock_get.return_value.json.return_value = [
        {'dolar': 'oficial', 'compra': 100, 'venta': 105},
        {'dolar': 'blue', 'compra': 200, 'venta': 205}
    ]

    test_df = extract_api("http://dummy-url.com")

    assert isinstance(test_df, pd.DataFrame)
    assert not test_df.empty
    assert len(test_df) > 0


@patch('pandas.read_csv')
def test_extract_csv_mock(mock_read_csv):

    mock_read_csv.return_value = pd.DataFrame({
        'producto': ['nafta', 'gasoil'],
        'precio': [100.5, 95.0]
    })

    test_df = get_csv("dummy-file.csv")

    assert isinstance(test_df, pd.DataFrame)
    assert not test_df.empty
    assert len(test_df) > 0



