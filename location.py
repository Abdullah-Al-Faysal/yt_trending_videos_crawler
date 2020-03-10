import urllib.request
import datetime
import json
from connection import *
from functions import *

import logging

logging.basicConfig(filename='yttracker.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)

# establish db connection
connection = get_connection()


# get channels info and push to db
def channels(channel_id, api_key):
    try:
        # get api request url
        request_url = 'https://www.googleapis.com/youtube/v3/channels?id=' + channel_id + '&key=' + api_key + '&part=snippet'

        # get response from api
        with urllib.request.urlopen(request_url) as url:
            data = json.loads(url.read().decode())
            try:
                description = data['items'][0]['snippet']['description']  # get the description of channel
            except:
                description = ''
            try:
                location = data['items'][0]['snippet']['country']  # get location/country
            except:
                location = 'N/A'
            print(location)
            language = get_language(description)  # get language of channnel description
            print(language)

            with connection.cursor() as cursor:
                cursor.execute("update channels set location = %s, language = %s where channel_id = %s",
                               (location, language, channel_id))
                connection.commit()

    except Exception as err:
        logger.error(err)


with connection.cursor() as cursor:
    cursor.execute("select channel_id from channels where location is null")
    channel_list = cursor.fetchall()
    for channel in channel_list:
        channels(channel['channel_id'], 'AIzaSyDMCbpdwXTwu3Izn5GmulR2M5C8kWXNKgU')
