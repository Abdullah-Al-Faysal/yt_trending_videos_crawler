from urllib.parse import urlparse
import urllib.request
import json
import datetime

from connection import *
from channels import *
from videos import *
from functions import *

# establish db connection
connection = get_connection()
api_key = 'AIzaSyDMCbpdwXTwu3Izn5GmulR2M5C8kWXNKgU'
today = datetime.date.today().strftime('%Y-%m-%d')

ch_list_today = []

with connection.cursor() as cursor:
    cursor.execute("select channel_id from channels_daily where date(extracted_date) = %s", today)
    rows = cursor.fetchall()
    for row in rows:
        channel_id = row['channel_id']
        ch_list_today.append(channel_id)

with connection.cursor() as cursor:
    cursor.execute("select channel_id from channels")
    records = cursor.fetchall()

    for record in records:
        channel_id = record['channel_id']
        if channel_id not in ch_list_today:
            playlist_id = get_playlist_id(channel_id)
            request_url_ch = 'https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&playlistId=' + playlist_id + '&key=' + api_key + '&maxResults=50'
            videoIdList_ch = []

            # get the list of videos
            def get_video_list_ch(startUrl, nextToken):
                try:
                    request_url_ch = startUrl + '&pageToken=' + nextToken
                    with urllib.request.urlopen(request_url_ch) as url:
                        data = json.loads(url.read().decode())
                        items = data['items']
                        for item in items:
                            try:
                                video_id = item['snippet']['resourceId']['videoId']
                                videoIdList_ch.append(video_id)
                            except:
                                pass
                        if 'nextPageToken' in data.keys():
                            nextToken = data['nextPageToken']
                            get_video_list_ch(startUrl, nextToken)
                        else:
                            return videoIdList_ch
                except:
                    print('Bad Request')


            get_video_list_ch(request_url_ch, '')

            for video_id in videoIdList_ch:
                videos(video_id, api_key)
                request_url_cmnts = 'https://www.googleapis.com/youtube/v3/commentThreads?key=' + api_key + '&textFormat=plainText&part=snippet&videoId=' + video_id + '&maxResults=50'
