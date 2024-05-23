from dotenv import load_dotenv
import os
from utils import load_csv_from_s3, convert_to_decimal
import boto3
import psycopg2
from botocore.exceptions import ClientError


# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Conexión a DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('tblConsume')

# Conexión a PostgreSQL
conn = psycopg2.connect(
    dbname=os.getenv("DB_POSTGRES_DATABASE"),
    user=os.getenv("DB_POSTGRES_USERNAME"),
    password=os.getenv("DB_POSTGRES_PASSWORD"),
    host=os.getenv("DB_POSTGRES_HOSTNAME"),
    port=os.getenv("DB_POSTGRES_PORT")
)

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

def lambda_handler(event, context):
    s3_client = boto3.client(
        's3',
        aws_access_key_id = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key
    )

    # Obtener información del evento de S3
    s3_event = event['Records'][0]
    bucket_name = s3_event['s3']['bucket']['name']
    object_key = s3_event['s3']['object']['key']

    # Cargar el archivo CSV desde S3 a un DataFrame usando Polars
    df_csv = load_csv_from_s3(bucket_name, object_key, s3=s3_client)

    # Convertir valores numéricos a Decimal en cada diccionario
    dict_list = [convert_to_decimal(item) for item in df_csv.to_dicts()]

    # Lista de registros que fallan en el insert a Dynamo
    failed_records = []

    #Crear la lista de registros para insertar a Dynamo
    dynamo_records = []

    for record in dict_list:
        dynamo_record = {
            "ID": str(record["ID"]),
            "country": {
                "country_iso3": record.get("COUNTRY_ISO3"),
                "country_name": record.get("COUNTRY_NAME")
            },
            "load_date": str(record.get("Current_Timestamp")),
            "modification_date": str(record.get("Current_Timestamp")),
            "file_name": record.get("File_Name")
        }
        dynamo_records.append(dynamo_record)

    #Insertar los datos en batch a Dynamo    
    with table.batch_writer() as batch:
        for dynamo_record in dynamo_records:
            try:
                batch.put_item(Item=dynamo_record)
            except ClientError as e:
                failed_records.append({'item': dynamo_record, 'error': str(e)})

    if failed_records:
        # Manejar los registros fallidos
        print(f"Errores al procesar los registros: {failed_records}")


    return {
        'statusCode': 200,
        'body': '{0} procesado exitosamente'.format(object_key)
    }
