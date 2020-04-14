import sys
import os
import boto3
import requests
import base64
import json
import logging
import time
import pymysql
from datetime import datetime
import passcode
import pandas as pd
import jsonpath

# Spotify
client_id = '2224a1f633a34acf96ecbfb3e25d1914'
client_secret = '413a1fb5687c4cd187bcd8d9c480e1cd'

# AWS - MySQL
aws_end_point = passcode.aws_end_point
port = passcode.port
database = passcode.database
username = passcode.username
password = passcode.password

def main():
    
    # AWS - database connection
    try:
        conn = pymysql.connect(aws_end_point, db=database, user=username, passwd=password, port=port, use_unicode=True)
        cursor = conn.cursor()
    except:
        logging.error("DB Connection Fail!")
        sys.exit(1)

    headers = conn_api(client_id, client_secret)

    # Extract Artist ID from RDS
    cursor.execute("SELECT id FROM artists")

    # jsonpath - jsonpath로 자동으로 찾아줌
    top_track_keys = {
        "id": "id",
        "name": "name",
        "popularity": "popularity",
        "external_url": "external_urls.spotify"
    }

    # list of dictionaries
    top_tracks = []
    for (id, ) in cursor.fetchall():

        url = "https://api.spotify.com/v1/artists/{id}/top-tracks".format(id=id)
        params = {
            "country": "US"
        }
        r = requests.get(url, params=params, headers=headers)
        raw = json.loads(r.text)
        
        for i in raw["tracks"]:
            top_track = {}
            for k, v in top_track_keys.items():
                top_track.update({k: jsonpath.jsonpath(i, v)})
                top_track.update({"artist_id": id})
                top_tracks.append(top_track)

    # Track ID
    track_ids = [i['id'][0] for i in top_tracks]

    # Create Pandas's DF then S3 Parquet - Top Tracks
    top_tracks = pd.DataFrame(top_tracks)
    top_tracks.to_parquet("top-tracks.parquet", engine="pyarrow", compression="snappy")

    # # Create JSON File
    # with open("top_tracks.json", "w") as f:
    #     for i in top_tracks:
    #         json.dump(i, f)
    #         f.write(os.linesep)

    dt = datetime.utcnow().strftime("%Y-%m-%d")

    # Define AWS-s3 and upload parquet
    s3 = boto3.resource('s3')
    object = s3.Object('bk-spotify', 'top-tracks/{}/top-tracks.parquet'.format(dt))
    data = open("top-tracks.parquet", "rb")
    object.put(Body=data)



    # Combine list of Track ID by 100
    tracks_batch = [track_ids[i: i+100] for i in range(0, len(track_ids), 100)]

    # list of dictionaries
    audio_features = []
    for i in tracks_batch:
        ids = ",".join(i)
        url = "https://api.spotify.com/v1/audio-features/?ids={}".format(ids)
        r = requests.get(url, headers=headers)
        raw = json.loads(r.text)
        audio_features.extend(raw['audio_features'])

    # Create Pandas's DF then S3 Parquet - Audio Features
    audio_features = pd.DataFrame(audio_features)
    audio_features.to_parquet("audio-features.parquet", engine="pyarrow", compression="snappy")

    # Define AWS-s3 and upload parquet
    s3 = boto3.resource('s3')
    object = s3.Object('bk-spotify', 'audio-features/{}/audio-features.parquet'.format(dt))
    data = open("top-tracks.parquet", "rb")
    object.put(Body=data)


###########################################################################
### Connect API Authorization - Followed by Spotify Web API Instruction ###
###########################################################################
def conn_api(client_id=client_id, client_secret=client_secret):

    # https://developer.spotify.com/documentation/general/guides/authorization-guide/

    # 1. Have your application request authorization
    encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')
    end_point = 'https://accounts.spotify.com/api/token'
    
    headers = {
        "Authorization": "Basic {}".format(encoded)
    }

    payload = {
        "grant_type": "client_credentials"
    }

    r = requests.post(end_point, headers=headers, data=payload)

    if r.status_code != 200:
        print("Conncetion Failed.")

    # 2. Use the access token to access the Spotify Web API
    access_token = json.loads(r.text)["access_token"]

    headers = {
        "Authorization": "Bearer {}".format(access_token)
    }

    return headers

if __name__ == '__main__':
    main()