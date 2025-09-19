import sys
import os
import pytest
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from etl.transform import clean_crude_oil, clean_dolar_price, clean_fuel_sales, clean_and_merge_fuel_prices

@pytest.fixture
def raw_crude_oil_df():
    data = {
        'date': ['2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01', '2023-05-01'],
        'value': ['85.50', ' 90.25 ', '.', '100.00', ' 95.00'],
        'other_col': [1, 2, 3, 4, 5]
    }
    return pd.DataFrame(data)

@pytest.fixture
def raw_dolar_price_df():
    data = {
        'casa': ['oficial ', 'blue', 'tarjeta'],
        'fecha': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'compra': [200, 300, 250],
        'venta': [205, 290, 280]
    }
    return pd.DataFrame(data)

@pytest.fixture
def raw_fuel_prices_df():
    current = pd.DataFrame({
        'fecha_vigencia': ['2023-01-01', '2023-02-01'],
        'provincia': ['BUENOS AIRES ', 'CORDOBA'],
        'idproducto': ['1', '2'],
        'producto': ['Nafta (súper) entre 92 y 95 Ron ', ' Gas Oil Grado 2'],
        'precio': [150.5, 140.0]
    })
    hist = pd.DataFrame({
        'fecha_vigencia': ['2022-12-01', '2022-11-01'],
        'provincia': ['SANTA FE', ' MENDOZA'],
        'idproducto': ['3', '4'],
        'producto': ['Nafta (premium) de mÃ¡s de 95 Ron', ' GNC '],
        'precio': [160.0, 50.0]
    })
    return current, hist

@pytest.fixture
def raw_fuel_sales_df():
    data = {
        'indice_tiempo': ['2023-01-01', '2023-02-01'],
        'provincia': ['Buenos Aires ', 'Córdoba '],
        'sector': ['residencial ', 'comercial'],
        'producto': ['Nafta Grado 2 (Súper)', 'Nafta Grado 3 (Ultra)'],
        'total': [1000, 1500],
        'unidad': ['litros', 'litros']
    }
    return pd.DataFrame(data)


def test_clean_crude_oil(raw_crude_oil_df):

    df_cleaned = clean_crude_oil(raw_crude_oil_df)
    
    assert 'precio_crudo' in df_cleaned.columns
    assert 'fecha' in df_cleaned.columns
    assert df_cleaned['precio_crudo'].dtype in ['float64', 'int64']
    assert df_cleaned['fecha'].dtype == 'datetime64[ns]'
    assert df_cleaned['precio_crudo'].isna().sum() == 0
    
def test_clean_dolar_price(raw_dolar_price_df):

    df_cleaned = clean_dolar_price(raw_dolar_price_df)
    
    assert 'tipo' in df_cleaned.columns
    assert df_cleaned['fecha'].dtype == 'datetime64[ns]'
    assert len(df_cleaned) == 2 # only 'oficial' & 'blue'
    
def test_clean_and_merge_fuel_prices(raw_fuel_prices_df):

    current_df, hist_df = raw_fuel_prices_df
    merged_df = clean_and_merge_fuel_prices(current_df, hist_df)
    
    assert 'precio_combustibles' in merged_df.columns
    assert 'fecha' in merged_df.columns

def test_clean_fuel_sales(raw_fuel_sales_df):

    df_cleaned = clean_fuel_sales(raw_fuel_sales_df)
    
    assert 'fecha' in df_cleaned.columns
    assert df_cleaned['fecha'].dtype == 'datetime64[ns]'
    assert 'total' in df_cleaned.columns
    assert 'producto' in df_cleaned.columns
