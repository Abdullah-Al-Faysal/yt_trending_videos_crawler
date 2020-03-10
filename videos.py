from urllib.parse import urlparse
import urllib.request
import json
import datetime
from connection import *
from functions import *

import logging

logging.basicConfig(filename='yttracker.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)

# establish db connection
connection = get_connection()


# get videos data
def videos(video_id, api_key):
    try:
        # get api request url
        request_url = 'https://www.googleapis.com/youtube/v3/videos?id=' + video_id + '&key=' + api_key + '&part=snippet,contentDetails'

        # get response from api
        with urllib.request.urlopen(request_url) as url:
            data = json.loads(url.read().decode())

            video_id = data['items'][0]['id']  # get video id
            video_title = data['items'][0]['snippet']['title']  # get the title of the video
            categoryId = data['items'][0]['snippet']['categoryId']  # get category ID
            category = get_category(categoryId)  # get category
            # try:
            #     tags = ",".join(data['items'][0]['snippet']['tags'])                                # get the tags
            # except:
            #     tags = 'Not found'
            published_date = normalize_metadate(data['items'][0]['snippet']['publishedAt'])  # get date published
            thumbnails_medium_url = data['items'][0]['snippet']['thumbnails']['medium'][
                'url']  # get medium thumbnail url
            # description = ''                                # get description
            description = data['items'][0]['snippet']['description']  # get description
            channel_id = data['items'][0]['snippet']['channelId']  # get channel ID
            added_to_db_time = '{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())  # get added to db time
            # duration = data['items'][0]['contentDetails']["duration"]                               # get duration
            # definition = data['items'][0]['contentDetails']["definition"]                           # get definition
            # caption = data['items'][0]['contentDetails']["caption"]                                 # get caption
            # licensedContent = data['items'][0]['contentDetails']["licensedContent"]                 # get licensedContent

            # push to db
            with connection.cursor() as cursor:
                cursor.execute("""INSERT INTO videos (video_id, video_title, category, published_date, thumbnails_medium_url, description, channel_id, added_to_db_time) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                               (video_id,
                                video_title,
                                category,
                                published_date,
                                thumbnails_medium_url,
                                description,
                                channel_id,
                                added_to_db_time
                                ))
                connection.commit()

    except Exception as err:
        logger.error(err)
