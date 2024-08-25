CREATE DATABASE SPOTIFY_DB
CREATE SCHEMA FILE_FORMATS
CREATE SCHEMA AWS_STAGE
CREATE SCHEMA PIPE

create storage integration aws_s3_int
    type = external_stage
    storage_provider = s3
    storage_aws_role_arn = 'put your arn'
    enabled = true
    storage_allowed_locations = ( 's3://bucket name' )
     comment = 'cretaing connection to aws for spotify project';

describe storage integration aws_s3_int

create schema file_formats

create file format SPOTIFY_DB.file_formats.csv_format
TYPE = 'csv'
alter file format SPOTIFY_DB.file_formats.csv_format
set skip_header = 1

create or replace stage aws_stage.csv_folder
url = 's3://bucket name/folder-name/'
storage_integration = aws_s3_int
file_format = SPOTIFY_DB.file_formats.csv_format

LIST @aws_stage.csv_folder/songs

CREATE OR REPLACE TABLE SPOTIFY_DB.PUBLIC.ALBUM_DATA (
album_id    STRING,
album_name  STRING,
external_urls STRING,
release_dates DATE,
total_tracks NUMBER);

CREATE OR REPLACE TABLE SPOTIFY_DB.PUBLIC.ARTIST_DATA (
artist_id    STRING,
artist_name  STRING,
external_urls STRING);

CREATE OR REPLACE TABLE SPOTIFY_DB.PUBLIC.SONGS_DATA (
song_id    STRING,
song_name  STRING,
song_duration FLOAT,
popularity NUMBER);

copy into SPOTIFY_DB.PUBLIC.ALBUM_DATA
from @aws_stage.csv_folder/album_data/

select * from SPOTIFY_DB.PUBLIC.ALBUM_DATA

copy into SPOTIFY_DB.PUBLIC.ARTIST_DATA
from @aws_stage.csv_folder/artist_data/

select * from SPOTIFY_DB.PUBLIC.ARTIST_DATA

copy into SPOTIFY_DB.PUBLIC.SONGS_DATA
from @aws_stage.csv_folder/songs_data/

select * from SPOTIFY_DB.PUBLIC.SONGS_DATA

create pipe SPOTIFY_DB.PIPE.ALBUM_PIPE 
auto_ingest = true
as
copy into SPOTIFY_DB.PUBLIC.ALBUM_DATA
from @aws_stage.csv_folder/album_data/;

create pipe SPOTIFY_DB.PIPE.ARTIST_PIPE 
auto_ingest = true
as
copy into SPOTIFY_DB.PUBLIC.ARTIST_DATA
from @aws_stage.csv_folder/artist_data/;

create pipe SPOTIFY_DB.PIPE.SONGS_PIPE  
auto_ingest = true
as
copy into SPOTIFY_DB.PUBLIC.SONGS_DATA
from @aws_stage.csv_folder/songs_data/;

describe pipe SPOTIFY_DB.PIPE.SONGS_PIPE  
