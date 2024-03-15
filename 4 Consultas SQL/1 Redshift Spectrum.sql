create external schema proyecto1_spectrum_schema
from data catalog
database 'proyecto1_spectrumdb'
iam_role 'arn:aws:iam::891377224401:role/LabRole'
create external database if not exists;

drop table if exists proyecto1_spectrum_schema.co2_trusted_spectrum;
CREATE EXTERNAL TABLE proyecto1_spectrum_schema.co2_trusted_spectrum(
  variableconsulta varchar(30), 
  fecha varchar(30), 
  calidad float8, 
  valor float8, 
  latitud float8, 
  longitud float8, 
  codigoserial INT8, 
  nombre  varchar(30), 
  nombrecorto  varchar(30))
STORED AS PARQUET
LOCATION
  's3://eiglesiasrtrabajo1/trusted/co2/';

drop table if exists proyecto1_spectrum_schema.so2_trusted_spectrum;
CREATE EXTERNAL TABLE proyecto1_spectrum_schema.so2_trusted_spectrum(
  variableconsulta varchar(30), 
  fecha varchar(30), 
  calidad float8, 
  valor float8, 
  latitud float8, 
  longitud float8, 
  codigoserial INT8, 
  nombre  varchar(30), 
  nombrecorto  varchar(30))
STORED AS PARQUET
LOCATION
  's3://eiglesiasrtrabajo1/trusted/so2/';

drop table if exists proyecto1_spectrum_schema.PoblacionAntioquia_spectrum;
CREATE EXTERNAL TABLE proyecto1_spectrum_schema.PoblacionAntioquia_spectrum(
  codigomunicipio varchar(50), 
  nombremunicipio varchar(50), 
  edad INT, 
  hombres_cabecera float8, 
  mujeres_cabecera float8, 
  hombres_centropoblado float8, 
  mujeres_centropoblado float8, 
  hombresruraldisperso float8, 
  mujeresruraldisperso float8
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION  's3://eiglesiasrtrabajo1/raw/DataPoblacionAntioquia/'
table properties ('skip.header.line.count'='1');
;

SELECT *
FROM  proyecto1_spectrum_schema.PoblacionAntioquia_spectrum;