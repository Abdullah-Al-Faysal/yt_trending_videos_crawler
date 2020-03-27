import urllib.request
import datetime
import json
import dateparser
from functions import normalize_metadate, get_category, get_language
import logging

logging.basicConfig(filename='C:\__ Work Station\Py_Projects\YT_TV_Crawler\yttv.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)s - %(funcName)20s()] %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filemode='w')
logger = logging.getLogger(__name__)

YT_CHANNEL_BASE_URL = "https://www.youtube.com/channel/"

regions = ["IN", "PK", "AE", "BD", "PH", "IR", "EG", "NP", "LK", "SY", "GB", "CN", "JO", "AF", "PS", "ZA", "LB", "ET",
           "YE", "ID", "SS", "SA", "SO", "IQ", "US"]

region_trending_videos_urls = [
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=IN&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=PK&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=AE&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=BD&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=PH&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=IR&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=EG&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=NP&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=LK&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=SY&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=GB&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=CN&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=JO&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=AF&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=PS&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=ZA&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=LB&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=ET&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=YE&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=ID&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=SS&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=SA&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=SO&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=IQ&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE",
    "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=US&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE"
]

YT_API_CHANNEL_DATA_BASE_URL = "https://www.googleapis.com/youtube/v3/channels?part=snippet%2Cstatistics&maxResults=50&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE&id="
UNIQUE_CHANNELS = {}


def get_daily_trending_videos():
    try:
        all_region_trending_videos = []
        for url in region_trending_videos_urls:
            region_trending_videos = get_trending_videos(url)
            print("region total >> ", len(region_trending_videos))
            if region_trending_videos is not None and len(region_trending_videos) > 0:
                all_region_trending_videos += region_trending_videos
        print("all region total >> ", len(all_region_trending_videos))
        # print(all_region_trending_videos[0])
        return all_region_trending_videos
    except Exception as err:
        print("all region error >> ", err)
        logger.error(err)
        return []


def get_trending_videos(request_url):
    try:
        print("region hit >> ")
        first_page_request_url = request_url
        next_page_token = True
        all_trending_videos = []
        while next_page_token is not None:
            response = get_yt_api_request_data(request_url)
            all_trending_videos += get_video_data(response['items'])
            next_page_token = get_value_if_key_exists_or_default(response, "nextPageToken")
            if next_page_token is None:
                break
            request_url = first_page_request_url + "&pageToken=" + next_page_token
        return all_trending_videos
    except Exception as err:
        print("region error >> ", err)
        logger.error(err)
        return []


def get_yt_api_request_data(request_url):
    try:
        with urllib.request.urlopen(request_url) as url:
            response = json.loads(url.read().decode())
            return response
    except Exception as err:
        print("req error >> ", err)
        logger.error(err)
        return {}


def get_video_data(videos):
    try:
        page_videos = []
        for video in videos:
            video_data = {"video_id": video['id'], "video_title": video['snippet']['title']}
            category_id = video['snippet']['categoryId']
            video_data["category"] = get_category(category_id)
            video_data["published_date"] = normalize_metadate(video['snippet']['publishedAt'])
            video_data["channel_id"] = video['snippet']['channelId']
            video_data["channel_title"] = video['snippet']['channelTitle']
            video_data["channel_url"] = YT_CHANNEL_BASE_URL + video_data["channel_id"]
            UNIQUE_CHANNELS[video_data["channel_id"]] = ""
            video_data["views"] = get_int_if_key_exists_or_default(video['statistics'], "viewCount")
            video_data["likes"] = get_int_if_key_exists_or_default(video['statistics'], "likeCount")
            video_data["dislikes"] = get_int_if_key_exists_or_default(video['statistics'], "dislikeCount")
            video_data["comments"] = get_int_if_key_exists_or_default(video['statistics'], "commentCount")
            video_data["extracted_date"] = '{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
            video_data["language"] = get_language(video['snippet']['description'])
            channel_data = get_channel_data(video_data["channel_id"])
            video_data.update(channel_data)
            page_videos.append(video_data)
        return page_videos
    except Exception as err:
        print("item error >> ", err)
        logger.error(err)
        return []


def get_channel_data(channel_id):
    try:
        request_url = YT_API_CHANNEL_DATA_BASE_URL + channel_id
        channel_details = {}
        response = get_yt_api_request_data(request_url)
        channel = response["items"][0]
        channel_details["location"] = get_value_if_key_exists_or_default(channel["snippet"], "country", "N/A")
        UNIQUE_CHANNELS[channel_id] = channel_details["location"]
        channel_details["channel_language"] = get_language(channel["snippet"]["description"])
        channel_details["channel_views"] = get_int_if_key_exists_or_default(channel['statistics'], "viewCount")
        channel_details["channel_subscribers"] = get_int_if_key_exists_or_default(channel['statistics'],
                                                                                  "subscriberCount")
        channel_details["channel_videos"] = get_int_if_key_exists_or_default(channel['statistics'], "videoCount")
        channel_details["channel_comments"] = get_int_if_key_exists_or_default(channel['statistics'], "commentCount")
        return channel_details
    except Exception as err:
        print("channel error >> ", err)
        logger.error(err)
        return {}


def get_int_if_key_exists_or_default(dictionary, key, default_value=None):
    return int(dictionary[key]) if check_if_key_exists(dictionary, key) else default_value


def get_value_if_key_exists_or_default(dictionary, key, default_value=None):
    return dictionary[key] if check_if_key_exists(dictionary, key) else default_value


def check_if_key_exists(dictionary, key):
    return key in dictionary

