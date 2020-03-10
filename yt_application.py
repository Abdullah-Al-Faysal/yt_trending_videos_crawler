from urllib.parse import urlparse
import urllib.request
import json

from connection import *
from channels import *
from videos import *
from functions import *

# establish db connection
connection = get_connection()


def get_api_key(tracker_id):
    try:
        with connection.cursor() as cursor:
            cursor.execute("select `tracker_creater` from `tracker` where tracker_id = %s", tracker_id)
            user_id = cursor.fetchone()['tracker_creater']
        with connection.cursor() as cursor:
            cursor.execute("select `youtube_data_api_key_value` from `youtube_data_api_key` where user_id = %s",
                           user_id)
            api_key = cursor.fetchone()['youtube_data_api_key_value']
        return (api_key)
    except:
        api_key = 'AIzaSyB44EAVoBcaplwD1ah-B8OJP92zdv4WPN8'
    return api_key


with connection.cursor() as cursor:
    cursor.execute("select * from tracker_relationship")
    records = cursor.fetchall()
    for record in records:
        content_type = record['content_type']
        tracker_id = record['tracker']
        api_key = get_api_key(tracker_id)
        if content_type == 'channel':
            channel_id = record['content_id']
            playlist_id = get_playlist_id(channel_id)
            channels(channel_id, api_key)
            channels_daily(channel_id, api_key)
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
                videos_daily(video_id, api_key)
                request_url_cmnts = 'https://www.googleapis.com/youtube/v3/commentThreads?key=' + api_key + '&textFormat=plainText&part=snippet&videoId=' + video_id + '&maxResults=50'
                get_youtube_comments(request_url_cmnts, '')
                get_related_videos(video_id, api_key)

        if content_type == 'video':
            video_id = record['content_id']
            videos(video_id, api_key)
            videos_daily(video_id, api_key)
            request_url_cmnts = 'https://www.googleapis.com/youtube/v3/commentThreads?key=' + api_key + '&textFormat=plainText&part=snippet&videoId=' + video_id + '&maxResults=50'
            get_youtube_comments(request_url_cmnts, '')
            get_related_videos(video_id, api_key)

        # with connection.cursor() as cursor:
        #     cursor.execute("select video_id from videos")
        #     records = cursor.fetchall()
        #     for record in records:
        #         video_id = record['video_id']
        #         videos_daily(video_id, api_key)
