from datetime import datetime
import os
from etl.extract import extract_api, get_csv
from etl.transform import clean_crude_oil, clean_dolar_price, clean_and_merge_fuel_prices, clean_fuel_sales, aggregate_monthly, merge_all_data
from etl.load import load_data_to_postgresql

"""EXTRACTING DATA PART"""

## CRUDE OIL API

fred_key = os.getenv('FRED_API_KEY')

crude_oil_url = f"https://api.stlouisfed.org/fred/series/observations"

params = {
        'api_key': fred_key,
        'series_id': 'POILBREUSDM',
        'file_type': 'json',
        'observation_start': '2000-01-01',
        'observation_end': datetime.now().strftime('%Y-%m-%d')
    }

crude_oil_df = extract_api(crude_oil_url, params, dict_key='observations')

##  DOLAR API 

dolar_url = "https://api.argentinadatos.com/v1/cotizaciones/dolares"

dolar_df = extract_api(dolar_url)

## GETTING CSV FILES FROM SRC

hist_fuel_prices_arg = get_csv("./src/precios-historicos.csv")

current_fuel_prices_arg = get_csv("./src/precios-en-surtidor vigentes.csv")

fuel_sales = get_csv("./src/ventas-mercado-producto-provincia.csv")


"""TRANSFORMING DATA PART"""  

cleaned_crude_oil_df = clean_crude_oil(crude_oil_df)

cleaned_dolar_price_df = clean_dolar_price(dolar_df)

cleaned_fuel_prices_df = clean_and_merge_fuel_prices(current_fuel_prices_arg, hist_fuel_prices_arg)

cleaned_fuel_sales_df = clean_fuel_sales(fuel_sales)

#  these are monthly average summaries for each data frame, except for the crude oil price which is already presented monthly.

cleaned_dolar_price_monthly = aggregate_monthly(cleaned_dolar_price_df,'fecha','precio_dolar_venta', 'tipo')

cleaned_fuel_prices_monthly = aggregate_monthly(cleaned_fuel_prices_df, 'fecha', 'precio_combustibles', 'producto', 'provincia')

cleaned_fuel_sales_monthly = aggregate_monthly(cleaned_fuel_sales_df, 'fecha', 'total', 'sector', 'producto', 'provincia')

merged_all_data = merge_all_data(cleaned_crude_oil_df, cleaned_fuel_prices_monthly, cleaned_dolar_price_monthly, cleaned_fuel_sales_monthly)

print(merged_all_data.head())

"""LOADING DATA PART"""  

load_data = load_data_to_postgresql(merged_all_data,'fuel_prices_dw')



