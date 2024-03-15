import requests
import boto3
import json 
from io import StringIO 
import pandas as pd

#########################Parametros de Conexion AWS######################
aws_access_key_id='____________________'
aws_secret_access_key='________________________________________'
aws_session_token='_____________________________________________________________' 

s3_bucket='eiglesiasrtrabajo1'
s3_target_file_name='raw/DataPoblacionAntioquia/dataPoblacionAntioquia.csv'
###############################################################


##########Obteniendo la información del API de datos Gov Colombia############
offset=0
url='https://www.datos.gov.co/resource/evm3-92yw.json?$offset={offset}'

df=pd.DataFrame()
while True:
    r = requests.get(url=url.format(offset=offset))
    if r.text=='[]\n':
        break
    a=json.loads(r.text)
    res=pd.json_normalize(a)
    df=df.append(res)
    
    offset+=1000
###############################################################
    
###########Transformando DataFrame a CSV en memoria para enviar a S3#####################
csv_buffer = StringIO() 
df.to_csv(csv_buffer, index=False) 
#########################################################################################

############Envio de Datos a S3########################################################
session = boto3.Session(aws_access_key_id,aws_secret_access_key,aws_session_token)
s3=session.client('s3')
s3.put_object(Body=csv_buffer.getvalue(),Bucket=s3_bucket,Key=s3_target_file_name)
#########################################################################################