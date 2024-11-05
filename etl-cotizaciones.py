import logging
import os
import json
from datetime import datetime, timedelta
from io import BytesIO
import pandas as pd
import yfinance as yf
import numpy as np
import requests
import boto3
import sys
import psycopg2
from psycopg2.extras import execute_values
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv,
                          [
                              'db_host',
                              'db_port',
                              'db_name'])


# Configuración de logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Variables globales
fecha_actual = str(pd.to_datetime(datetime.now()).strftime("%Y-%m-%d"))
db_host = args['db_host']
db_port = args['db_port']
db_name = args['db_name']

# Columnas para base de datos
dict_columnas = {
    "FECHA": "DATE", "PRODUCTO": "VARCHAR(100)", "PUERTO": "VARCHAR(100)",
    "PRECIO_CIERRE_PESOS": "FLOAT", "PRECIO_CIERRE_DOLAR": "FLOAT", "FUENTE": "VARCHAR(100)"
}
# Funcion para obtener secretos AWS Secrets Manager


def get_secret(secret_name, region_name):
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name)
    except Exception as ex:
        raise ex
    secret = get_secret_value_response['SecretString']
    return secret

# Funcion para obtener ultima fecha de la tabla de la DB


def get_last_date(fuente):
    # Obtener las credenciales de la base de datos desde AWS Secrets Manager
    try:
        secret_db = json.loads(get_secret(
            'aqui_va_secret_name', 'aqui_va_region_name'))
        logging.info(
            'Obtención exitosa del -secret_db- desde AWS Secrets Manager.')
    except Exception as ex:
        logging.error(
            'Error al obtener el secreto desde AWS Secrets Manager: %s', ex)
        raise ex
    # Conexión a la base de datos
    try:
        with psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=secret_db['username'],
            password=secret_db['password'],
            sslrootcert='SSLCERTIFICATE'
        ) as conexion_db:
            logging.info('Conexión exitosa a la base de datos en AWS RDS.')
            # Leer la última fecha cargada en la tabla
            try:
                with conexion_db.cursor() as cursor:
                    query = f"""SELECT MAX(Fecha) as ultima_fecha FROM public.cotizaciones_cubo WHERE fuente = '{
                        fuente}';"""
                    cursor.execute(query)
                    ultima_fecha = cursor.fetchone()[0]
                    logging.info(
                        'Obtención exitosa de la última fecha actualizada: %s', ultima_fecha)
                    return ultima_fecha + timedelta(days=1)
            except Exception as ex:
                logging.error("Error al consultar la fecha: %s", ex)
                raise ex

    except Exception as ex:
        logging.error('Error al conectar a la base de datos: %s', ex)
        raise ex

# Función para extracción de datos


def extract_data(url, is_excel=False, sep=';', headers=None):
    logging.info("Extrayendo datos desde %s", url)
    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        content = response.content
        if is_excel:
            return pd.read_excel(BytesIO(content))
        else:
            return pd.read_csv(BytesIO(content), sep=sep)
    except requests.exceptions.RequestException as ex:
        logging.error("Error en la extracción de datos desde %s: %s", url, ex)
        return None

# Función para transformación de datos de BNA


def transform_bna_data(df_usd_hist):
    try:
        df_usd_hist = df_usd_hist.drop(columns=['Unnamed: 3'], errors='ignore')
        df_usd_hist['Fecha cotizacion'] = pd.to_datetime(
            df_usd_hist['Fecha cotizacion'], format='%d/%m/%Y')
        return df_usd_hist
    except Exception as ex:
        logging.error("Error en la transformación de datos de BNA: %s", ex)
        raise ex

# Función para transformación de datos de Bolsacereales


def transform_bolsacereales_data(df_bolsa_arg, df_usd_hist):
    try:
        df_bolsa_arg = df_bolsa_arg[df_bolsa_arg['Puerto'] != 'QUEQUEN']
        df_bolsa_arg.rename(columns={
            'Cereal': 'producto',
            'Cotizaciones Cámaras Arbitrales - Pesos por toneladas': 'precio_cierre_pesos',
            'Cotizaciones Cámaras Arbitrales - Dólares por toneladas': 'precio_cierre_dolar'
        }, inplace=True)
        df_bolsa_arg['Fecha'] = pd.to_datetime(df_bolsa_arg['Fecha'])
        df_joined = pd.merge(df_bolsa_arg, df_usd_hist,
                             left_on='Fecha', right_on='Fecha cotizacion', how='left')
        df_joined['Venta'] = df_joined['Venta'].str.replace(
            ',', '.').astype(float, errors='ignore')
        df_joined['precio_cierre_dolar'] = np.where(
            (df_joined['precio_cierre_pesos'] != 0) & (
                (df_joined['precio_cierre_dolar'] == 0) |
                (df_joined['precio_cierre_dolar'].isnull()) |
                (df_joined['precio_cierre_dolar'] == '')
            ),
            df_joined['precio_cierre_pesos'] /
            df_joined['Venta'], df_joined['precio_cierre_dolar']
        )
        df_joined['precio_cierre_pesos'] = np.where(
            (df_joined['precio_cierre_dolar'] != 0) & (
                (df_joined['precio_cierre_pesos'] == 0) |
                (df_joined['precio_cierre_pesos'].isnull()) |
                (df_joined['precio_cierre_pesos'] == '')
            ),
            df_joined['precio_cierre_dolar'] *
            df_joined['Venta'], df_joined['precio_cierre_pesos']
        )
        df_joined[['precio_cierre_pesos', 'precio_cierre_dolar']] = df_joined[[
            'precio_cierre_pesos', 'precio_cierre_dolar']].fillna(0)
        df_bolsa_arg_mod = df_joined[[
            'Fecha', 'producto', 'Puerto', 'precio_cierre_pesos', 'precio_cierre_dolar']]
        df_bolsa_arg_mod = df_bolsa_arg_mod.sort_values(
            by='Fecha', ascending=True)
        df_bolsa_arg_mod['fuente'] = 'bolsadecereales'
        # Cleansing stage
        remove_registers = ~((df_bolsa_arg_mod['producto'].isin(
            ['MAIZ', 'TRIGO ART. 12'])) & (df_bolsa_arg_mod['Puerto'] == 'BUENOS AIRES'))
        df_bolsa_arg_mod = df_bolsa_arg_mod[remove_registers]
        return df_bolsa_arg_mod
    except Exception as e:
        logging.error(
            "Error en la transformación de datos de Bolsacereales: %s", e)
        return None

# Función para extraer y transformar datos de Yahoo Finance


def transform_yahoo_finance_data(fecha_desde, fecha_hasta, df_usd_hist):
    # Tickers Yahoo Finance
    futures_tickers = ['ZS=F', 'ZC=F', 'ZW=F', 'ZL=F']
    logging.info("Extrayendo datos desde Yahoo Finance")
    try:
        futures_data = yf.download(
            futures_tickers, start=fecha_desde, end=fecha_hasta, timeout=20)

        if futures_data.empty:
            logging.error("No se encontraron datos en Yahoo Finance.")
            return None

        # Verificar que haya datos para todos los tickers
        for ticker in futures_tickers:
            if ticker not in futures_data['Close'].columns:
                logging.error(
                    "No se encontraron datos para el ticker %s.", ticker)
                return None

        # Si todo está correcto, continuar con las transformaciones
        df_yf = pd.DataFrame({
            'SOJA FUTURO': futures_data['Close']['ZS=F'],
            'MAIZ FUTURO': futures_data['Close']['ZC=F'],
            'TRIGO FUTURO': futures_data['Close']['ZW=F'],
            'ACEITE DE SOJA FUTURO': futures_data['Close']['ZL=F']
        })
        df_yf = df_yf.reset_index()
        df_yf = pd.melt(
            df_yf, id_vars=['Date'], var_name='producto', value_name='precio_cierre_dolar')
        df_yf['Puerto'] = 'CHICAGO'
        df_yf['Date'] = df_yf['Date'].dt.tz_localize(None)
        df_joined = pd.merge(df_yf, df_usd_hist,
                             left_on='Date', right_on='Fecha cotizacion', how='left')
        df_joined['Venta'] = df_joined['Venta'].str.replace(
            ',', '.').astype(float, errors='ignore')
        df_joined['precio_cierre_pesos'] = np.where(
            (df_joined['precio_cierre_dolar'] != 0),
            df_joined['precio_cierre_dolar'] * df_joined['Venta'], 0
        )
        df_joined[['precio_cierre_pesos', 'precio_cierre_dolar']] = df_joined[[
            'precio_cierre_pesos', 'precio_cierre_dolar']].fillna(0)
        df_yf_mod = df_joined[['Date', 'producto', 'Puerto',
                               'precio_cierre_pesos', 'precio_cierre_dolar']]
        df_yf_mod.rename(columns={'Date': 'Fecha'}, inplace=True)
        df_yf_mod = df_yf_mod.sort_values(by='Fecha', ascending=True)
        df_yf_mod['fuente'] = 'yahoo_finance'
        return df_yf_mod
    except Exception as ex:
        logging.error(
            "Error en la extracción o transformación de datos de Yahoo Finance: %s", ex)
        return None

# Función para transformar datos de Excel EDP


def transform_excel_data(df_excel, df_usd_hist, fecha_desde, fecha_hasta):
    try:
        fecha_hasta = datetime.strptime(fecha_hasta, "%Y-%m-%d").date()

        df_excel = df_excel.iloc[2:, :-3]
        df_excel = df_excel.rename(
            columns={df_excel.columns[0]: 'Fecha'})
        df_excel_melt = pd.melt(df_excel,
                                id_vars=['Fecha'],
                                value_vars=[
                                    'Producto_1', 'Producto_2', 'Producto_3'],
                                var_name='producto',
                                value_name='precio_cierre_dolar')
        df_excel_melt['Puerto'] = 'PRECIO PROVEEDOR'
        df_excel_melt['precio_cierre_dolar'] = df_excel_melt['precio_cierre_dolar'].astype(
            float, errors='ignore')
        df_excel_melt['Fecha'] = pd.to_datetime(df_excel_melt['Fecha']).dt.date
        df_usd_hist['Fecha cotizacion'] = pd.to_datetime(df_usd_hist['Fecha cotizacion']).dt.date
        df_joined = pd.merge(df_excel_melt, df_usd_hist,
                             left_on='Fecha', right_on='Fecha cotizacion', how='left')
        df_joined['Venta'] = df_joined['Venta'].str.replace(
            ',', '.').astype(float, errors='ignore')
        df_joined['precio_cierre_pesos'] = np.where(
            (df_joined['precio_cierre_dolar'] != 0),
            df_joined['precio_cierre_dolar'] * df_joined['Venta'], 0
        )
        df_joined[['precio_cierre_pesos', 'precio_cierre_dolar']] = df_joined[[
            'precio_cierre_pesos', 'precio_cierre_dolar']].fillna(0)
        df_excel_mod = df_joined[[
            'Fecha', 'producto', 'Puerto', 'precio_cierre_pesos', 'precio_cierre_dolar']]
        df_excel_mod['producto'] = df_excel_mod['producto'].str.upper()
        df_excel_mod = df_excel_mod.sort_values(by='Fecha', ascending=True)
        df_excel_mod['fuente'] = 'excel'
        df_excel_mod = df_excel_mod.loc[(df_excel_mod['Fecha'] >= fecha_desde) & (
            df_excel_mod['Fecha'] <= fecha_hasta)]
        return df_excel_mod
    except Exception as ex:
        logging.error(
            "Error en la transformación de datos de Excel EDP: %s", ex)
        return None

# Función para manejar la conexión, creación de la tabla e inserción de datos en la base de datos


def load_data_to_db(dataframe):
    # Obtener las credenciales de la base de datos desde AWS Secrets Manager
    try:
        secret_db = json.loads(get_secret(
            'aqui_va_secret_name', 'aqui_va_region_name'))
        logging.info(
            'Obtención exitosa del -secret_db- desde AWS Secrets Manager.')
    except Exception as ex:
        logging.error(
            'Error al obtener el secreto desde AWS Secrets Manager: %s', ex)
    try:
        with psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=secret_db['username'],
            password=secret_db['password'],
            sslrootcert='SSLCERTIFICATE'
        ) as conexion_db:

            with conexion_db.cursor() as cursor:
                logging.info('Conexión exitosa a la base de datos en AWS RDS.')

                # Inserción de datos en la tabla
                columnas_query_insert = ', '.join(dict_columnas.keys())
                query_insert = f"""INSERT INTO public.cotizaciones_cubo ({
                    columnas_query_insert}) VALUES %s;"""
                valores = [tuple(row)
                           for row in dataframe.itertuples(index=False)]

                try:
                    execute_values(cursor, query_insert, valores)
                    conexion_db.commit()
                    logging.info(
                        'Datos insertados exitosamente en la tabla "cotizaciones_cubo".')
                except Exception as ex:
                    logging.error(
                        'Error al insertar datos en la tabla "cotizaciones_cubo": %s', ex)
                    conexion_db.rollback()

    except Exception as ex:
        logging.error('Error al conectar a la base de datos: %s', ex)


# Función principal del ETL


def run_etl():
    # Consulta ultima fecha actualizada tabla
    fecha_desde_bolsa = get_last_date('bolsadecereales')
    fecha_desde_yf = get_last_date('yahoo_finance')
    fecha_desde_excel = get_last_date('excel')
    fecha_desde_usd = min(fecha_desde_bolsa, fecha_desde_yf, fecha_desde_excel)
    # URLs fuentes de datos
    URL_BOLSACER = f"""https://www.bolsadecereales.com/admin/phpexcel/Examples/reportes_csv.php?reporte=camara&desde={
        fecha_desde_bolsa}&hasta={fecha_actual}&puerto="""

    USD_HIST_URL = f"""https://www.bna.com.ar/Cotizador/DescargarPorFecha?__RequestVerificationToken=8p5FjG4oyCGaF4zeTghY7pmIoiYaao-obhbWnBYJb6szDWghAt50-X7K8_IT164whABJvC8iAMOAYFOnJN80-DF21RNhKBAb5FQbmKkkjS-Yk0oAkrir1qNe7nNARQ2yz5jFwQrph2hhhXoSD_-RGzWjEFzSzyMy5JpM9oawVlY1&RadioButton=on&filtroEuroDescarga=0&filtroDolarDescarga=1&fechaDesde={
        fecha_desde_usd}&fechaHasta={fecha_actual}&id=billetes&descargar="""

    URL_EXCEL = 'aqui_va_url.xlsx'

    df_usd_hist = extract_data(USD_HIST_URL)
    if df_usd_hist is not None and not df_usd_hist.empty:
        df_usd_hist = transform_bna_data(df_usd_hist)
        df_bolsa_arg = extract_data(URL_BOLSACER)
        if df_bolsa_arg is not None and not df_bolsa_arg.empty:
            df_bolsa_arg_mod = transform_bolsacereales_data(
                df_bolsa_arg, df_usd_hist)
            load_data_to_db(df_bolsa_arg_mod)
        df_yf = transform_yahoo_finance_data(
            fecha_desde_yf, fecha_actual, df_usd_hist)
        if df_yf is not None and not df_yf.empty:
            load_data_to_db(df_yf)
        df_excel = extract_data(URL_EXCEL, is_excel=True)
        if df_excel is not None and not df_excel.empty:
            df_excel_mod = transform_excel_data(
                df_excel, df_usd_hist, fecha_desde_excel, fecha_actual)
            load_data_to_db(df_excel_mod)
    else:
        raise ValueError(
            'Error en la extracción o transformación de datos de BNA')

    logging.info('ETL finalizado exitosamente.')


# EJECUCION DEL ETL
run_etl()
