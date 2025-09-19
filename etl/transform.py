import pandas as pd
import numpy as np
import logging

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

def clean_crude_oil(df): 
#  this function cleans data types, missing data, spaces, and column names.
    for column in df.columns:
        if df[column].dtype == 'object': #  if the column is object type, strip method cleans white space
            df[column] = df[column].str.strip()

    df['value'] = df['value'].replace('.', np.nan)  #  points in values ​​are replaced by null values

    df['value'] = pd.to_numeric(df['value'], errors='coerce') #   value column is converted to float

    df.dropna(subset=['value'], inplace=True)

    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # this assertion code check dtypes  and null values 
    assert df['value'].dtype in ['float64', 'int64']  
    assert df.value.isna().sum() == 0
    assert df.date.dtype == 'datetime64[ns]'
    
    df = df.loc[: , ['date', 'value']]
    df.rename(columns= {'date':'fecha', 'value':'precio_crudo'}, inplace = True)

    logging.info("Crude oil df successfully transformed")
    return df


def clean_dolar_price(dolar_price): 
    #  this function cleans data types, missing data, spaces, and column names.

    dolar_price = dolar_price.rename(columns={'casa': 'tipo'})

    dolar_price['tipo'] = dolar_price['tipo'].str.strip()
    
    dolar_price['fecha'] = pd.to_datetime(dolar_price['fecha'], errors='coerce') #  fecha column is converted to datetime type

    dolar_price.dropna(inplace=True)

    dolar_price['venta'] = np.where(dolar_price['venta'] > dolar_price['compra'], dolar_price['venta'], (dolar_price['venta'] + dolar_price['compra'])/2)
    #  in case the purchase price is higher than the sale price, the average is calculated, because this is an error.

    dolar_price = dolar_price.rename(columns={'venta' : 'precio_dolar_venta'})
    
    dolar_price = dolar_price.loc[dolar_price['tipo'].isin(['blue', 'oficial']), ['fecha', 'tipo', 'precio_dolar_venta'] ]
    # only the official and blue dollar types are used to simplify analysis.

    assert dolar_price['fecha'].dtype == 'datetime64[ns]'

    logging.info("Historic dolar price df successfully transformed")
    return dolar_price


def clean_and_merge_fuel_prices(current_fuel_prices, hist_fuel_prices):
# this function cleans and merges the CSV of historical and current fuel prices in Argentina.

    #  cleaning current_fuel_prices df
    current_fuel_prices['fecha_vigencia'] = pd.to_datetime(current_fuel_prices['fecha_vigencia'], errors='coerce')
      
    current_fuel_prices['provincia'].str.strip()
    current_fuel_prices['producto'].str.strip()

    current_fuel_prices = current_fuel_prices[['fecha_vigencia', 'provincia', 'idproducto', 'producto', 'precio']]

    #  cleaning historical_fuel_prices df

    hist_fuel_prices['fecha_vigencia'] = pd.to_datetime(hist_fuel_prices['fecha_vigencia'], errors='coerce')

    hist_fuel_prices['provincia'].str.strip()
    hist_fuel_prices['producto'].str.strip()

    hist_fuel_prices = hist_fuel_prices[['fecha_vigencia', 'provincia', 'idproducto', 'producto', 'precio']]
      
    #  merge of clean dataframes

    hist_fuel_prices = hist_fuel_prices.copy()
    current_fuel_prices = current_fuel_prices.copy()

    merged_fuel_prices = pd.concat([hist_fuel_prices, current_fuel_prices], axis=0)
    merged_fuel_prices.rename(columns={'fecha_vigencia':'fecha', 'precio':'precio_combustibles'}, inplace=True)
    merged_fuel_prices['fecha'] = pd.to_datetime(merged_fuel_prices['fecha'], errors='coerce')

    producto_mapping = {
            'GNC': 'Gas Natural',
            'Gas Oil Grado 2': 'Gasoil Grado 2 (Común)',
            'Gas Oil Grado 3': 'Gasoil Grado 3', 
            'Nafta (premium) de más de 95 Ron': 'Nafta Grado 3 (Ultra)',
            'Nafta (súper) entre 92 y 95 Ron': 'Nafta Grado 2 (Súper)',
            
            # Clean problematic characters from the price dataset
            'Nafta (premium) de mÃ¡s de 95 Ron': 'Nafta Grado 3 (Ultra)',
            'Nafta (sÃºper) entre 92 y 95 Ron': 'Nafta Grado 2 (Súper)'
        }

    merged_fuel_prices['producto'] = merged_fuel_prices['producto'].replace(producto_mapping)

    provincia_mapping = {
            'BUENOS AIRES': 'Buenos Aires',
            'CAPITAL FEDERAL': 'Capital Federal', 
            'CORDOBA': 'Córdoba',
            'LA PAMPA': 'La Pampa',
            'SANTA FE': 'Santa Fe',
            'TUCUMAN': 'Tucuman',
            'SALTA': 'Salta',
            'MENDOZA': 'Mendoza',
            'NEUQUEN': 'Neuquén',
            'SAN JUAN': 'San Juan',
            'ENTRE RIOS': 'Entre Rios',
            'JUJUY': 'Jujuy',
            'SANTIAGO DEL ESTERO': 'Santiago del Estero',
            'SAN LUIS': 'San Luis',
            'CATAMARCA': 'Catamarca',
            'CHACO': 'Chaco',
            'CHUBUT': 'Chubut',
            'CORRIENTES': 'Corrientes',
            'FORMOSA': 'Formosa',
            'LA RIOJA': 'La Rioja',
            'MISIONES': 'Misiones',
            'RIO NEGRO': 'Rio Negro',
            'SANTA CRUZ': 'Santa Cruz',
            'TIERRA DEL FUEGO': 'Tierra del Fuego'
        }

    merged_fuel_prices['provincia'] = merged_fuel_prices['provincia'].replace(provincia_mapping)

    #  for greater price consistency, years with fewer than 100 records are eliminated.
    df_over_100 = merged_fuel_prices.fecha.dt.year.value_counts()
    valid_years = df_over_100[df_over_100 > 100].index
    fuel_prices_df = merged_fuel_prices[merged_fuel_prices.fecha.dt.year.isin(valid_years)]

    logging.info("Historic and current fuel prices in Argentina df successfully merged and transformed")   
    return fuel_prices_df


def clean_fuel_sales(fuel_sales):
    #  this function cleans date and object types, and set the categories for the fuel sales

    fuel_sales['indice_tiempo'] = pd.to_datetime(fuel_sales['indice_tiempo'], errors='coerce')
    fuel_sales.rename(columns={'indice_tiempo': 'fecha'}, inplace=True)

    fuel_sales['provincia'].str.strip()
    fuel_sales['producto'].str.strip()
    fuel_sales['sector'].str.strip()

    fuel_sales = fuel_sales[['fecha', 'provincia', 'sector', 'producto', 'total', 'unidad']]

    categories = ['Gas Natural', 'Gasoil Grado 2 (Común)', 'Gasoil Grado 3 (Ultra)', 'Nafta Grado 2 (Súper)', 'Nafta Grado 3 (Ultra)']

    #setting categories
    fuel_sales = fuel_sales.loc[fuel_sales['producto'].isin(categories), :]

    condition = (fuel_sales['fecha'].dt.year == 2016) & (fuel_sales['fecha'].dt.month == 1) 

    fuel_sales = fuel_sales.loc[~condition, :] 

    logging.info("Fuel sales df successfully transformed")
    return fuel_sales


def aggregate_monthly(df, date_column, value_columns, aggregate_column=None, aggregate_column_2 = None, aggregate_colum_3 = None):
#  average monthly aggregate for each dataframe

    df['year_month'] = df[date_column].dt.to_period('M') #  this line of code set the column's date period to monthly

    group_columns = ['year_month']

    #  this if block adds grouping columns if necessary
    if aggregate_column is not None:
        group_columns.append(aggregate_column)

    if aggregate_column_2 is not None:
        group_columns.append(aggregate_column_2)    
    
    if aggregate_colum_3 is not None:
        group_columns.append(aggregate_colum_3)
    

    monthly_data = df.groupby(group_columns)[value_columns].mean().reset_index()
    monthly_data['fecha'] = monthly_data['year_month'].dt.start_time.dt.date
    monthly_data = monthly_data.drop('year_month', axis=1)
    monthly_data['fecha'] = pd.to_datetime(monthly_data['fecha'], errors='coerce')
    
    logging.info(f"monthly summary of the df made")

    return monthly_data


def merge_all_data(cleaned_crude_oil_df, cleaned_fuel_prices_monthly, cleaned_dolar_price_monthly, cleaned_fuel_sales_monthly):
    merge1 = cleaned_fuel_prices_monthly.merge(cleaned_crude_oil_df, on='fecha', how='inner')
    merge2 = merge1.merge(cleaned_dolar_price_monthly, on='fecha', how='inner')
    merge3 = merge2.merge(cleaned_fuel_sales_monthly, on=['fecha', 'producto', 'provincia'], how='inner')

    logging.info(f"All dataframes merged - Shape: {merge3.shape}")

    return merge3
