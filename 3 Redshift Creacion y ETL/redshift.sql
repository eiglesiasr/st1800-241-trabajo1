drop table if exists PoblacionAntioquia;

create table PoblacionAntioquia (
codigomunicipio varchar(50) not null,
nombremunicipio varchar(50) not null,
edad NUMERIC,
hombres_cabecera NUMERIC,
mujeres_cabecera NUMERIC,
hombres_centropoblado NUMERIC,
mujeres_centropoblado NUMERIC,
hombresruraldisperso NUMERIC,
mujeresruraldisperso NUMERIC
);


copy PoblacionAntioquia
from 's3://eiglesiasrtrabajo1/raw/DataPoblacionAntioquia/dataPoblacionAntioquia.csv'
iam_role 'arn:aws:iam::891377224401:role/LabRole'
delimiter ','
IGNOREHEADER 1
CSV;