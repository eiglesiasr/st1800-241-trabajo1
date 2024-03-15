#%% Librerias
import boto3 
import pandas as pd 
import urllib.request 
import json 
from io import StringIO 

  

############################################################################### 

#%% funciones

def dioxidos(session, url, bucket, file): 
    # Fetch the JSON data from the URL 
    with urllib.request.urlopen(url) as response: 
        data = json.loads(response.read().decode()) 
    # Flatten the JSON object into a DataFrame 
    df = pd.json_normalize(data, 'datos',['latitud','longitud', 'codigoSerial', 'nombre', 'nombreCorto']) 

    csv_buffer = StringIO() 
    df.to_csv(csv_buffer, index=False) 
    # AWS credentials and bucket name 
    bucket_name = bucket 
    file_name = file  # File name to be saved in S3 

    s3=session.client('s3') 
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue()) 

     

############################################################################### 

#%% resultados 
aws_access_key_id='____________________'
aws_secret_access_key='________________________________________'
aws_session_token='_____________________________________________________________'

session = boto3.Session( 
    aws_access_key_id=aws_access_key_id, 
    aws_secret_access_key=aws_secret_access_key, 
    aws_session_token=aws_session_token 
) 

dioxidos(session = session, 
          url = 'https://datosabiertos.metropol.gov.co/sites/default/files/uploaded_resources/Datos_SIATA_Aire_so2.json', 
          bucket = bucket, 
          file = 'raw/so2/so2.csv'    
    ) 


dioxidos(session = session, 
          url = 'https://datosabiertos.metropol.gov.co/sites/default/files/uploaded_resources/Datos_SIATA_Aire_no2.json', 
          bucket = bucket, 
          file = 'raw/no2/no2.csv'    
    ) 

#%% Guardar en S3

