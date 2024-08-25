import spotipy
import json
from spotipy.oauth2 import SpotifyClientCredentials
import os
import boto3
from datetime import datetime 

def lambda_handler(event,context):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    client_credentials_manager = SpotifyClientCredentials(client_id = client_id ,client_secret=client_secret)
    sp =spotipy.Spotify(client_credentials_manager = client_credentials_manager)
   
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbLZ52XmnySJg"
    playlist_URI = playlist_link.split("/")[-1]
    spotify_data = sp.playlist_tracks(playlist_URI)
    # print(spotify_data)
    
    client = boto3.client('s3')
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    client.put_object(
    Bucket = "spotify-bucket-etl-ashutosh",
    Key = "raw-data/to-be-processed-data/" + filename,
    Body =json.dumps(spotify_data) 
          )
          
    glue = boto3.client('glue',region_name = 'ap-south-1')
    gluejobname = "spotify-spark-transformation"
    
    try:
        runid = glue.start_job_run(JobName = gluejobname)
        status = glue.get_job_run(JobName = gluejobname,RunId = runid['JobRunId'])
        print("Job Status:" , status['JobRun']['JobRunState'])
    except Exception as e:
            print(e)

