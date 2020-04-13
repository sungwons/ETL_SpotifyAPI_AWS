import sys
import boto3
import requests
import base64
import json
import logging
import time
import pandas as pd
import pymysql
import passcode

# Spotify
client_id = passcode.client_id
client_secret = passcode.client_secret

# AWS - MySQL
aws_end_point = passcode.aws_end_point
port = passcode.port
database = passcode.database
username = passcode.username
password = passcode.password

def main():

     # AWS - MySQL Connection
    try:
        conn = pymysql.connect(aws_end_point, db=database, user=username, passwd=password, port=port, use_unicode=True)
        cursor = conn.cursor()
    except:
        logging.error("MySQL Connection Fail!")
        sys.exit(1)

    # AWS - DynamoDB Connection
    # For Credential Issue, AWS CLI (aws configure) - IAM Setting needed to cennect table
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-2', endpoint_url="http://dynamodb.us-east-2.amazonaws.com")
    except:
        logging.error("Couldn't connect to dynamodb.")
        sys.exit(1)

    table = dynamodb.Table('top_tracks')
   
    cursor.execute("SELECT id FROM artists")

    for (artist_id, ) in cursor.fetchall():
        URL = "https://api.spotify.com/v1/artists/{}/top-tracks".format(artist_id)

        params = {
            'country': 'US'
        }

        r = requests.get(URL, params=params, headers=conn_api())

        raw = json.loads(r.text)

        for track in raw['tracks']:
    
            data = {
                'artist_id': artist_id
            }

            data.update(track)

            table.put_item(
                Item=data
            )

    print("Success")

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


if __name__ == "__main__":
    main()