import sys
import requests
import base64
import json
import logging
import time
import pandas as pd

client_id = '2224a1f633a34acf96ecbfb3e25d1914'
client_secret = '413a1fb5687c4cd187bcd8d9c480e1cd'

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

    if r.status_code == 200:
        print("Conncetion Successed!")
    else:
        print("Connection Failed!")

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
    
    headers = conn_api(client_id, client_secret)

    p = {
        "q": "McKay",
        "type": "artist" 
    }

    search_address = 'https://api.spotify.com/v1/search'

    try:
        r = requests.get(search_address, params=p, headers=headers)
    except:
        logging.error(r.text)
        sys.exit(0)

    if r.status_code != 200:
        
        logging.error(json.loads(r.text))
        print("Connection Faild!")

        # Too many request
        if r.status_code == 429:
            retry_after = json.loads(r.text)["Retry-After"]
            time.sleep(int(retry_after))

            r = requests.get(search_address, params=p, headers=headers)

        # Unauthorized
        elif r.status_code == 401:
            headers = conn_api(client_id, client_secret)

            r = requests.get(search_address, params=p, headers=headers)
        
        else:
            sys.exit(1)

    try:
        list = json.loads(r.text)

        artist = list['artists']
        item = artist['items']
        artist_id = item[0]['id']
    except:
        print("No Found: {}".format(p['q']))

    if artist_id != "":
        print("{}'s ID: {}".format(p["q"], artist_id))
    else:
        print("Artist's ID not found.")
        logging.error(list)
        sys.exit(1)
    
    return artist_id

###################################
### Find Artist's Top 10 Tracks ###
################################### 
def toptrack_api():

    # Bring header
    headers = conn_api(client_id, client_secret)

    # Bring artist_id
    artist_id = search_api()

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

    # Artist id
    artist_name = album["tracks"][0]["artists"][0]["id"]

    # Top Track shows only Top Songs
    top_ten_songs = []    
    for i in range(album_count):
        top_ten_songs.append([])
        top_ten_songs[i].append("Top {}".format(i+1))
        top_ten_songs[i].append(artist_name)
        top_ten_songs[i].append(album["tracks"][i]["name"])
        top_ten_songs[i].append(album["tracks"][i]["popularity"])
        top_ten_songs[i].append(album["tracks"][i]["external_urls"]["spotify"])

    # # Show Top Ten Songs
    # for i in range(len(top_ten_songs)):
    #     print(top_ten_songs[i])

    df = pd.DataFrame(top_ten_songs, columns=["Rank", "Artist", "Song", "Popularity", "URL"])
    df.to_csv('./Result/{} - Top 10.csv'.format(artist_name), index=False)

    ### Load in JSON format
    # top_track_info = {}

    # for i in range(10):
    #     top_track_info[album["tracks"][i]["name"]] = album["tracks"][i]["external_urls"]["spotify"]
        
    # print(top_track_info)


if __name__ == '__main__':

    toptrack_api()
