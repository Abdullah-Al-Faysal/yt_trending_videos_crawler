from urllib.parse import urlparse
import urllib.request
import json

from connection import *
from channels import *
from videos import *
from functions import *

# establish db connection
connection = get_connection()

def get_api_key(user_id):
    try:
        with connection.cursor() as cursor:
            cursor.execute("select `youtube_data_api_key_value` from `youtube_data_api_key` where user_id = %s", user_id)
            api_key = cursor.fetchone()['youtube_data_api_key_value']
        return(api_key)    
    except:
        api_key = 'AIzaSyB44EAVoBcaplwD1ah-B8OJP92zdv4WPN8'
    return api_key

with connection.cursor() as cursor:
    cursor.execute("select * from yt_crawler_pipeline")
    records = cursor.fetchall()
    for record in records:
        content_type = record['content_type']
        user_id = record['user_id']
        sid = record['source_id']
        api_key = get_api_key(user_id)

        # -----------Content type: Channel-----------

        if content_type == 'channel':
            channel_id = record['content_id']
            playlist_id = get_playlist_id(channel_id)

            channels(channel_id, api_key)
            channels_daily(channel_id, api_key)
            cursor.execute("UPDATE `yt_crawler_pipeline` SET `status` = %s where `content_id` = %s", ('crawled', channel_id))
            connection.commit()

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
                # get video info
                videos_sid(video_id, api_key, sid)
                videos_daily(video_id, api_key)

        # -----------Content type: keyword-----------

        if content_type == 'keyword':
            keyword = record['content_id']

            request_url_kw = 'https://www.googleapis.com/youtube/v3/search?q=' + keyword + '&part=snippet&type=video&maxResults=50&key=' + api_key
            videoIdList_kw = []

            # get the list of videos
            def get_video_list_kw(request_url_kw):
                try:
                    with urllib.request.urlopen(request_url_kw) as url:
                        data = json.loads(url.read().decode())
                        items = data['items']
                        for item in items:
                            try:
                                video_id = item['id']['videoId']
                                videoIdList_kw.append(video_id)
                            except:
                                pass
                except:
                    print('Bad Request')
        
            get_video_list_kw(request_url_kw)

            for video_id in videoIdList_kw:
                # get video info
                videos_sid(video_id, api_key, sid)
                videos_daily(video_id, api_key)

        # -----------Content type: Video-------------

        if content_type == 'video':
            video_id = record['content_id']
            videos_sid(video_id, api_key, sid)
            videos_daily(video_id, api_key)
            cursor.execute("UPDATE `yt_crawler_pipeline` SET `status` = %s where `content_id` = %s", ('crawled', video_id))
            connection.commit()