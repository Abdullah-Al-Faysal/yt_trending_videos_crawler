import urllib.request
import datetime
import json
import dateparser
from functions import *
import logging

logging.basicConfig(filename='yttv.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)

YT_CHANNEL_BASE_URL = "https://www.youtube.com/channel/"

regions = ["IN", "PK", "AE", "BD", "PH", "IR", "EG", "NP", "LK", "SY", "GB", "CN", "JO", "AF", "PS", "ZA", "LB", "ET",
           "YE", "ID", "SS", "SA", "SO", "IQ", "US"]

region_trending_videos_urls = [
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=IN&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=PK&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=AE&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=BD&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=PH&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=IR&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=EG&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=NP&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=LK&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=SY&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=GB&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=CN&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=JO&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=AF&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=PS&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=ZA&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=LB&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=ET&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=YE&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=ID&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=SS&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=SA&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=SO&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=IQ&key=[YOUR_API_KEY]",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=US&key=[YOUR_API_KEY]",
]


def get_daily_trending_videos():
    all_region_trending_videos = []
    for url in region_trending_videos_urls:
        region_trending_videos = get_trending_videos(url)
        if region_trending_videos is not None:
            all_region_trending_videos.append(region_trending_videos)
    return None


def get_trending_videos(request_url):
    print("hit")
    try:
        next_page_token = True
        all_trending_videos = []
        while next_page_token is not None:
            response = get_page_videos_data(request_url)
            next_page_token = response['nextPageToken']
            request_url += ("&" + next_page_token)
            all_trending_videos += get_video_data(response['items'])
            print(len(all_trending_videos))
            # print(all_trending_videos)
            return all_trending_videos
    except Exception as err:
        logger.error(err)
        print(err)
        return None


def get_page_videos_data(request_url):
    try:
        with urllib.request.urlopen(request_url) as url:
            response = json.loads(url.read().decode())
            # print(response)
            return response

    except Exception as err:
        logger.error(err)
        return None


def get_video_data(videos):
    page_videos = []
    for video in videos:
        # print(video['statistics']['viewCount'])
        video_data = {"video_id": video['id'], "video_title": video['snippet']['title']}
        category_id = video['snippet']['categoryId']
        video_data["category"] = get_category(category_id)
        video_data["published_date"] = normalize_metadate(video['snippet']['publishedAt'])
        video_data["channel_id"] = video['snippet']['channelId']
        video_data["channel_title"] = video['snippet']['channelTitle']
        video_data["channel_url"] = YT_CHANNEL_BASE_URL + video_data["channel_title"]
        video_data["views"] = video['statistics']['viewCount']
        video_data["likes"] = video['statistics']['likeCount']
        video_data["dislikes"] = video['statistics']['dislikeCount']
        video_data["comments"] = video['statistics']['commentCount']
        video_data["extracted_date"] = '{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
        page_videos.append(video_data)
    print(len(page_videos))
    return page_videos


if __name__ == "__main__":
    get_trending_videos("https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=IN&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE")

