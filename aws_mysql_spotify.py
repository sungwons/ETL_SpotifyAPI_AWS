import sys
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

#################################
### Search Artist Information ###
#################################    
def search_api():

    # AWS - database connection
    try:
        conn = pymysql.connect(aws_end_point, db=database, user=username, passwd=password, port=port, use_unicode=True)
        cursor = conn.cursor()
    except:
        logging.error("DB Connection Fail!")
        sys.exit(1)

    headers = conn_api(client_id, client_secret)

    # Load singer.csv 
    singers = pd.read_csv("singers.csv")
    singers = singers.values.tolist()

    params = []

    for i in range(len(singers)):
        params.append({"q": singers[i][0], "type": "artist"})


    search_address = 'https://api.spotify.com/v1/search'

    artist_list = []
    i = 0

    for param in params:
        r = requests.get(search_address, params=param, headers=headers)

        if r.status_code != 200:
            
            logging.error(json.loads(r.text))
            print("Connection Faild!")

            # Too many request
            if r.status_code == 429:
                retry_after = json.loads(r.text)["Retry-After"]
                time.sleep(int(retry_after))

                r = requests.get(search_address, params=param, headers=headers)

            # Unauthorized
            elif r.status_code == 401:
                headers = conn_api(client_id, client_secret)

                r = requests.get(search_address, params=param, headers=headers)
            
            else:
                sys.exit(1)

        try:
            list = json.loads(r.text)
            artist = list['artists']
            item = artist['items']

            art = {}

            if list['artists']['items'][0]['name'] == param['q']:
                art.update(
                    {
                    'artist_id': item[0]['id'],
                    'name': item[0]['name'],
                    'followers': item[0]['followers']['total'],
                    'popularity': item[0]['popularity'],
                    'url': item[0]['external_urls']['spotify'],
                    'image_url': item[0]['images'][0]['url'],
                    }
                )
            # Upload Artist Info to AWS MySQL
            for i in art:
                query = """
                    INSERT INTO artists VALUES ('{id}', '{Name}', {Followers}, {Popularity}, '{URL}', '{Image_URL}')
                    ON DUPLICATE KEY UPDATE id='{id}', name='{Name}', followers={Followers}, popularity={Popularity}, url='{URL}', image_url='{Image_URL}'
                    """.format(id=art['artist_id'], Name=art['name'], Followers=art['followers'], Popularity=art['popularity'], URL=art['url'], Image_URL=art['image_url'])
                cursor.execute(query)  

        except:
            print("Couldn't find artist info: {}".format(param['q']))
            continue

    # Collect Artist ID Only
    cursor.execute("SELECT id FROM artists;")

    artist_id = []
    for (id, ) in cursor.fetchall():
        artist_id.append(id)

    conn.commit()  

    return artist_id

###################################
### Find Artist's Top 10 Tracks ###
################################### 
def toptrack_api():

    # Bring header
    headers = conn_api(client_id, client_secret)

    # Bring artist_id
    artist_list = search_api()

    # Top Track shows 
    top_ten_songs = []
    j = 0

    for artist_id in artist_list:
        # Album_address
        album_address = "https://api.spotify.com/v1/artists/{id}/top-tracks".format(id=artist_id)

        # Parameters
        p = {
            "country": "US"
        }

        r = requests.get(album_address, headers=headers, params=p)

        album = json.loads(r.text)

        # Album Count
        album_count = len(album["tracks"])

        # Top Track Name
        top_track_name = album["tracks"][0]["name"]

        # Top Track URL
        top_track_url = album["tracks"][0]["external_urls"]["spotify"]

        # Artist Name
        artist_name = album["tracks"][0]["artists"][0]["name"]

        for i in range(album_count):
            top_ten_songs.append([])
            top_ten_songs[j].append(i+1)
            top_ten_songs[j].append(artist_name)
            top_ten_songs[j].append(album["tracks"][i]["name"])
            top_ten_songs[j].append(album["tracks"][i]["popularity"])
            top_ten_songs[j].append(album["tracks"][i]["external_urls"]["spotify"])
            j += 1

    df = pd.DataFrame(top_ten_songs, columns=["Rank", "Artist", "Song", "Popularity", "URL"])
    df.to_csv('./Result/{} Top 10.csv'.format(time.strftime('%Y-%m-%d', time.localtime(time.time()))), index=False)


if __name__ == '__main__':

    toptrack_api()
