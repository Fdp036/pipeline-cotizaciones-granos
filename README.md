# ETL-Cotizaciones

Repositorio del pipeline ETL desarrollado para Tablero Comercial/de Compras/Insumos

ETL (Extract, Transform, Load) escrito en Python que extrae datos de varias fuentes, los transforma y los carga en una base de datos PostgreSQL en AWS RDS.

## Importación de librerías

* Registro de eventos (logging)
* Variables de entorno e interacciones con el sistema de archivos (os, json)
* Manipulación de fechas y horas (datetime, timedelta)
* Almacenamiento de datos binarios en memoria (BytesIO)
* Manipulación y análisis de datos (pandas, numpy)
* Solicitudes web (requests)
* Servicios de AWS (boto3, awsglue)
* Argumentos de línea de comandos (sys)
* Interacciones con bases de datos PostgreSQL (psycopg2)

```python
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
```

## Seteo de variables

1. Invoca los parámetros seteados previamente durante la configuración del job en AWS Glue (`db_host`, `db_port`, `db_name`) utilizando `getResolvedOptions`.
2. Configura el registro de logs con un nivel predeterminado de `INFO` y un formato personalizado.
3. Establece variables globales:
	* `fecha_actual`: la fecha actual en formato `YYYY-MM-DD`.
	* `db_host`, `db_port`, `db_name`: asigna a variables los argumentos pasados en el job de AWS Glue.
4. Define un diccionario (`dict_columnas`) que asigna nombres de columnas a sus respectivos tipos de datos para una tabla de base de datos.

```python
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
```

## Función `get_secret`

Se utiliza para obtener credenciales almacenadas en AWS Secrets Manager. Se le pasarán dos parámetros: `secret_name` y `region_name`.

```python
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
```

## Función `get_last_date`

Se utiliza para obtener la última fecha registrada en una tabla de base de datos para una fuente de datos determinada, y devuelve esa fecha más un día.

1. Obtiene las credenciales de la base de datos desde AWS Secrets Manager utilizando la función `get_secret`.
2. Establece una conexión a la base de datos utilizando la biblioteca `psycopg2` y las credenciales obtenidas en el paso anterior.
3. Ejecuta una consulta SQL para obtener la última fecha registrada en la tabla `cotizaciones_cubo` para la fuente de datos especificada (`fuente`).
4. Obtiene el resultado de la consulta y lo devuelve como una fecha.
5. Si no se produce ningún error, la función devuelve la última fecha registrada más un día (utilizando el objeto `timedelta`).

```python
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
```

## Función `extract_data`

La función `extract_data` se utiliza para descargar y leer datos desde una URL especificada, ya sea en formato Excel o CSV.

### Parámetros

* `url`: la URL desde la que se van a extraer los datos.
* `is_excel`: un booleano que indica si el archivo que se va a descargar es un Excel (`.xlsx` o `.xls`). Por defecto es `False`.
* `sep`: el separador de columnas que se va a utilizar si el archivo es un CSV. Por defecto es `;`.
* `headers`: un diccionario de cabeceras HTTP que se van a enviar en la solicitud. Por defecto es `None`.
  
### Funcionamiento
  
1. La función inicia registrando un mensaje de información en el log, indicando que se está extrayendo datos desde la URL especificada.
2. Se intenta realizar una solicitud GET a la URL especificada utilizando la biblioteca `requests`. Se establece un tiempo de espera de 60 segundos.
3. Si la solicitud es exitosa, se verifica que el estado de la respuesta sea 200 (OK) utilizando el método `raise_for_status()`.
4. Si todo va bien, se obtiene el contenido de la respuesta y se almacena en la variable `content`.
5. Si `is_excel` es `True`, se utiliza la biblioteca `pandas` para leer el contenido como un archivo Excel utilizando `pd.read_excel()`. Si `is_excel` es `False`, se utiliza `pd.read_csv()` para leer el contenido como un archivo CSV, utilizando el separador especificado en `sep`.
6. Si se produce un error durante la solicitud o la lectura del archivo, se registra un mensaje de error en el log con el mensaje de error y se devuelve `None`.

```python
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
```

## Función `transform_bna_data`

Su propósito es transformar los datos de una dataframe (`df_usd_hist`) que contiene información histórica de cotizaciones del dólar estadounidense del Banco de la Nacion Argentina (BNA).

### Parámetros

* `df_usd_hist`: el dataframe que contiene los datos históricos de cotizaciones del dólar estadounidense del BNA.

### Funcionamiento

1. La función intenta eliminar una columna llamada `Unnamed: 3` del dataframe, ignorando cualquier error que pueda ocurrir si la columna no existe.
2. La función convierte la columna `Fecha cotizacion` del dataframe a un formato de fecha utilizando la función `pd.to_datetime()`. El formato de fecha utilizado es `%d/%m/%Y`, que corresponde a un formato de fecha con día, mes y año en ese orden.
3. La función devuelve el dataframe transformada.

### Manejo de errores

* Si se produce cualquier error durante la ejecución de la función, se registra un mensaje de error en el log con el mensaje de error y se levanta la excepción original utilizando `raise ex`. Esto permite que el error sea manejado por la capa superior de la aplicación.

```python
def transform_bna_data(df_usd_hist):

    try:

        df_usd_hist = df_usd_hist.drop(columns=['Unnamed: 3'], errors='ignore')

        df_usd_hist['Fecha cotizacion'] = pd.to_datetime(

            df_usd_hist['Fecha cotizacion'], format='%d/%m/%Y')

        return df_usd_hist

    except Exception as ex:

        logging.error("Error en la transformación de datos de BNA: %s", ex)

        raise ex
```

## Función `transform_bolsacereales_data`

Su propósito es transformar y limpiar los datos de la Bolsa de Cereales. 

### Parámetros

* `df_bolsa_arg`: dataframe que contiene los datos de la Bolsa de Cereales.
* `df_usd_hist`: dataframe que contiene los datos históricos del tipo de cambio del dólar estadounidense.

### Funcionamiento

1. La función intenta eliminar las filas del dataframe `df_bolsa_arg` donde el puerto sea "QUEQUEN".
2. La función renombra algunas columnas del dataframe `df_bolsa_arg` para que tengan nombres más descriptivos.
3. La función convierte la columna "Fecha" del dataframe `df_bolsa_arg` a un formato de fecha estándar.
4. La función une la dataframe `df_bolsa_arg` con el dataframe `df_usd_hist` en función de la columna "Fecha" y la columna "Fecha cotizacion", respectivamente. La unión se realiza de manera izquierda.
5. La función convierte la columna "Venta" del dataframe unida a un formato numérico, reemplazando las comas por puntos.
6. La función calcula el precio de cierre en dólares para las filas donde el precio de cierre en pesos es distinto de cero y el precio de cierre en dólares es cero o nulo. La fórmula utilizada es `precio_cierre_pesos / Venta`.
7. La función calcula el precio de cierre en pesos para las filas donde el precio de cierre en dólares es distinto de cero y el precio de cierre en pesos es cero o nulo. La fórmula utilizada es `precio_cierre_dolar * Venta`.
8. La función reemplaza los valores nulos en las columnas "precio_cierre_pesos" y "precio_cierre_dolar" por cero.
9. La función selecciona solo las columnas relevantes del dataframe unida y las ordena por la columna "Fecha" en orden ascendente.
10. La función agrega una columna "fuente" con el valor "bolsadecereales" para indicar la fuente de los datos.
11. La función elimina las filas del dataframe donde el producto sea "MAIZ" o "TRIGO ART. 12" y el puerto sea "BUENOS AIRES".
12. La función devuelve el dataframe transformada y limpia.

### Manejo de errores

* Si se produce cualquier error durante la ejecución de la función, se registra un mensaje de error en el log con el mensaje de error y se devuelve `None`.

```python
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
```

## Función `transform_yahoo_finance_data`

Su propósito es extraer y transformar datos de Yahoo Finance para futuros de commodities.

### Parámetros

* `fecha_desde`: la fecha de inicio para la extracción de datos.
* `fecha_hasta`: la fecha de fin para la extracción de datos.
* `df_usd_hist`: el dataframe que contiene los datos históricos del tipo de cambio del dólar estadounidense.

### Funcionamiento

1. La función define una lista de tickers de futuros de commodities en Yahoo Finance: `ZS=F` (soja), `ZC=F` (maíz), `ZW=F` (trigo) y `ZL=F` (aceite de soja).
2. La función intenta descargar los datos de los tickers definidos en el paso anterior desde Yahoo Finance utilizando la biblioteca `yfinance`. La fecha de inicio y fin se establecen en `fecha_desde` y `fecha_hasta`, respectivamente.
3. Si no se encuentran datos en Yahoo Finance, la función registra un mensaje de error y devuelve `None`.
4. La función verifica que haya datos para todos los tickers definidos. Si no hay datos para algún ticker, la función registra un mensaje de error y devuelve `None`.
5. Si todo está correcto, la función crea un nuevo dataframe `df_yf` con los datos de cierre de los futuros de commodities.
6. El dataframe `df_yf` se resetea y se convierte en un dataframe con formato largo utilizando la función `pd.melt`.
7. La columna `Puerto` se agrega al dataframe `df_yf` con el valor "CHICAGO" y se elimina la información de zona horaria de la columna `Date`.
8. El dataframe `df_yf` se une con el dataframe `df_usd_hist` en función de la columna `Date` y la columna `Fecha cotizacion`, respectivamente. La unión se realiza de manera izquierda.
9. La columna `Venta` de la dataframe unida se convierte en un formato numérico, reemplazando las comas por puntos.
10. Se calcula el precio de cierre en pesos para cada futuro de commodity multiplicando el precio de cierre en dólares por el tipo de cambio del dólar estadounidense.
11. Los valores nulos en las columnas `precio_cierre_pesos` y `precio_cierre_dolar` se reemplazan por cero.
12. Se seleccionan solo las columnas relevantes de la dataframe unida y se ordena por la columna `Fecha` en orden ascendente.
13. Se agrega una columna `fuente` al dataframe con el valor "yahoo_finance" para indicar la fuente de los datos.
14. Se devuelve el dataframe transformado y limpio.

### Manejo de errores

* Si se produce cualquier error durante la ejecución de la función, se registra un mensaje de error en el log con el mensaje de error y se devuelve `None`.

```python
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
```

## Función `transform_excel_data`

Su propósito es transformar y limpiar datos de un dataframe de Excel .

### Parámetros

* `df_excel`: el dataframe que contiene los datos originales de Excel .
* `df_usd_hist`: el dataframe que contiene los datos históricos del tipo de cambio del dólar estadounidense.
* `fecha_desde`: la fecha de inicio para filtrar los datos.
* `fecha_hasta`: la fecha de fin para filtrar los datos.

### Funcionamiento

1. La función elimina las dos primeras filas del dataframe `df_excel` y luego renombra la primera columna a "Fecha".
2. Se utiliza la función `pd.melt` para transformar el dataframe en un formato largo, con las columnas "Fecha", "producto" y "precio_cierre_dolar".
3. Se agrega una columna "Puerto" al dataframe con el valor "PRECIO PROVEEDOR".
4. La columna "precio_cierre_dolar" se convierte a un tipo de dato numérico y se reemplazan las comas por puntos.
5. Se une el dataframe resultante con el dataframe `df_usd_hist` en función de la columna "Fecha" y "Fecha cotizacion", respectivamente.
6. La columna "Venta" se convierte a un tipo de dato numérico y se reemplazan las comas por puntos.
7. Se calcula el precio de cierre en pesos para cada producto multiplicando el precio de cierre en dólares por el tipo de cambio del dólar estadounidense.
8. Los valores nulos en las columnas "precio_cierre_pesos" y "precio_cierre_dolar" se reemplazan por cero.
9. Se seleccionan solo las columnas relevantes del dataframe unido y se ordena por la columna "Fecha" en orden ascendente.
10. Se agrega una columna "fuente" al dataframe con el valor "excel" para indicar la fuente de los datos.
11. Se filtran los datos para que solo se incluyan las filas donde la columna "Fecha" está entre `fecha_desde` y `fecha_hasta`.
12. Se devuelve el dataframe transformado y limpio.

### Manejo de errores

* Si se produce cualquier error durante la ejecución de la función, se registra un mensaje de error en el log con el mensaje de error y se devuelve `None`.

```python
def transform_excel_data(df_excel, df_usd_hist, fecha_desde, fecha_hasta):

    try:

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

            "Error en la transformación de datos de Excel : %s", ex)

        return None
```

## Función `load_data_to_db`

Su propósito es insertar datos en una base de datos. 

### Parámetros

* `dataframe`: el dataframe que contiene los datos que se van a insertar en la base de datos.

### Funcionamiento

1. La función intenta obtener las credenciales de la base de datos desde AWS Secrets Manager utilizando la función `get_secret`.
2. Si se obtienen las credenciales correctamente, se registra un mensaje de éxito en el log.
3. Si se produce un error al obtener las credenciales, se registra un mensaje de error en el log con el mensaje de error.
4. La función intenta establecer una conexión con la base de datos utilizando las credenciales obtenidas.
5. Si se establece la conexión correctamente, se registra un mensaje de éxito en el log.
6. La función intenta insertar los datos del dataframe en la tabla "cotizaciones_cubo" utilizando la función `execute_values`.
7. Si la inserción se realiza correctamente, se registra un mensaje de éxito en el log.
8. Si se produce un error durante la inserción, se registra un mensaje de error en el log con el mensaje de error y se realiza un rollback de la transacción.
9. Si se produce un error al conectar a la base de datos, se registra un mensaje de error en el log con el mensaje de error.

```python
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
```

## Función `run_etl`

La función `run_etl` es el punto de entrada para la ejecución del ETL (Extract, Transform, Load) de los datos. Esta función realiza las siguientes tareas:

1. Obtiene la última fecha actualizada de las tablas "bolsadecereales", "yahoo_finance" y "excel" utilizando la función `get_last_date`.
2. Calcula la fecha desde la cual se deben extraer los datos utilizando la función `min`.
3. Construye las URL para extraer los datos de las fuentes de datos utilizando las fechas obtenidas.
4. Extrae los datos de las fuentes utilizando la función `extract_data`.
5. Transforma los datos extraídos utilizando las funciones `transform_bna_data`, `transform_bolsacereales_data`, `transform_yahoo_finance_data` y `transform_excel_data`.
6. Carga los datos transformados en la base de datos utilizando la función `load_data_to_db`.
7. Registra un mensaje de éxito en el log si la ejecución del ETL se realiza correctamente.

Si se produce un error en alguna de las etapas del ETL, se registra un mensaje de error en el log y se lanza una excepción `ValueError` con el mensaje de error.

```python
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

  

    URL_excel = 'aqui_va_url.xlsx'

  

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

        df_excel = extract_data(URL_excel, is_excel=True)

        if df_excel is not None and not df_excel.empty:

            df_excel_mod = transform_excel_data(

                df_excel, df_usd_hist, fecha_desde_excel, fecha_actual)

            load_data_to_db(df_excel_mod)

    else:

        raise ValueError(

            'Error en la extracción o transformación de datos de BNA')

  

    logging.info('ETL finalizado exitosamente.')
```

### Por último, se invoca a la función `run_etl()` y se ejecuta el pipeline.
