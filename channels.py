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

            # get channel info
            channel_id = data['items'][0]['id']  # get channel id
            channel_title = data['items'][0]['snippet']['title']  # get the name displayed in channel
            thumbnails_medium_url = data['items'][0]['snippet']['thumbnails']['medium'][
                'url']  # get medium thumbnail url
            # description = data['items'][0]['snippet']['description']				            # get the description of channel
            description = ''
            joined_date = normalize_metadate(data['items'][0]['snippet']['publishedAt'])  # get joined date
            added_to_db_time = '{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())  # get added to db time
            # playlist_id = get_playlist_id(channel_id)                                           # get playlist id from channel id
            try:
                location = data['items'][0]['snippet']['country']  # get location/country
            except:
                location = 'N/A'
            language = get_language(description)  # get language of channnel description

            with connection.cursor() as cursor:
                cursor.execute("""INSERT INTO channels (channel_id, channel_title, thumbnails_medium_url, description, joined_date, added_to_db_time, location, language) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                               (channel_id,
                                channel_title,
                                thumbnails_medium_url,
                                description,
                                joined_date,
                                added_to_db_time,
                                location,
                                language
                                ))
                connection.commit()

    except Exception as err:
        logger.error(err)
