import pg8000
import requests
import os


def lambda_handler(event, context):
    # Establecer los parámetros de conexión
    conn = pg8000.connect(
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT'],
        database=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD']
    )

    # Crear un cursor para ejecutar consultas
    cursor = conn.cursor()
    headers = {
    "X-CMC_PRO_API_KEY": os.environ['API_KEY'],
    }
    response = requests.get('https://sandbox-api.coinmarketcap.com/v1/cryptocurrency/listings/latest',headers=headers)
    datos = response.json()
    bitcoin_price =  float(datos['data'][0]['quote']['BTC']['price'])
    
    # Ejecutar una consulta
    cursor.execute("INSERT INTO bitcoin_prices (price) VALUES (%s)", (bitcoin_price,))
    conn.commit()

    # Cerrar el cursor y la conexión
    cursor.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': 'Precio del Bitcoin extraído y almacenado exitosamente.'
    }
