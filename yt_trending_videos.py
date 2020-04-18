from urllib.request import urlopen, Request
from fake_useragent import UserAgent
import urllib.error
import datetime
import json
from functions import normalize_metadate, get_category, get_language
from url_builder import UrlBuilder
import logging

logging.basicConfig(filename='C:\__ Work Station\Py_Projects\YT_TV_Crawler\logs\yttv.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)s - %(funcName)20s()] %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filemode='a')
logger = logging.getLogger(__name__)

url_builder = UrlBuilder("AIzaSyDbbSUKCqcBLk0-Z5Fe99o8TKwLh-zUSHE")
YT_CHANNEL_BASE_URL = "https://www.youtube.com/channel/"
target_regions = ["IN", "PK", "AE", "BD", "PH", "IR", "EG", "NP", "LK", "SY", "GB", "CN", "JO", "AF", "PS", "ZA", "LB",
                  "ET", "YE", "ID", "SS", "SA", "SO", "IQ", "US"]
fake_user_agent = UserAgent()
UNIQUE_CHANNELS = {}


def get_daily_trending_videos():
    try:
        all_region_trending_videos = []
        for region in target_regions:
            video_data_request_url = url_builder.build_video_url(region)
            region_trending_videos = get_trending_videos(region, video_data_request_url)
            if region_trending_videos is not None and len(region_trending_videos) > 0:
                all_region_trending_videos += region_trending_videos
        logMessage = "all region total >> " + str(len(all_region_trending_videos))
        print(logMessage)
        logger.info(logMessage)
        return all_region_trending_videos
    except Exception as err:
        print("all region error >> ", err)
        logger.error(err)
        return []


def get_trending_videos(region, request_url):
    try:
        print("region hit >> ")
        first_page_request_url = request_url
        next_page_token = True
        all_trending_videos = []
        while next_page_token is not None:
            response = get_yt_api_request_data(request_url)
            all_trending_videos += get_video_data(region, response['items'])
            next_page_token = get_value_if_key_exists_or_default(response, "nextPageToken")
            if next_page_token is None:
                break
            request_url = first_page_request_url + "&pageToken=" + next_page_token
        logMessage = "region total >> " + str(len(all_trending_videos))
        print(logMessage)
        logger.info(logMessage)
        return all_trending_videos
    except Exception as err:
        print("region error >> ", err)
        logger.error(err)
        return []


def get_yt_api_request_data(request_url):
    try:
        custom_headers = {
            'User-Agent': fake_user_agent.chrome
        }
        with urlopen(Request(request_url, headers=custom_headers)) as url:
            response = json.loads(url.read().decode())
            return response
    except urllib.error.HTTPError as http_err:
        error_json = json.loads(http_err.read().decode())
        print("Req error >> ", error_json, request_url)
        logger.error(request_url)
        logger.error(error_json)
        return {}
    except Exception as err:
        print("req error >> ", err, request_url) #json.dumps(err)
        logger.error(request_url)
        logger.error(err)
        return {}


def get_video_data(region, videos):
    try:
        page_videos = []
        for video in videos:
            video_data = {"video_id": video['id'], "video_title": video['snippet']['title'], "trending_at": region}
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
        request_url = url_builder.build_channel_url(channel_id)
        channel_details = {}
        response = get_yt_api_request_data(request_url)
        # print("channel response >> ", response)
        if response is not None and "items" in response and response["items"] is not None:
            channel = response["items"][0]
            channel_details["location"] = get_value_if_key_exists_or_default(channel["snippet"], "country", "N/A")
            UNIQUE_CHANNELS[channel_id] = channel_details["location"]
            channel_details["channel_language"] = get_language(channel["snippet"]["description"]) if check_if_key_exists(
                channel["snippet"], "description") else "N/A"
            channel_details["channel_views"] = get_int_if_key_exists_or_default(channel['statistics'], "viewCount")
            channel_details["channel_subscribers"] = get_int_if_key_exists_or_default(channel['statistics'],
                                                                                      "subscriberCount")
            channel_details["channel_videos"] = get_int_if_key_exists_or_default(channel['statistics'], "videoCount")
            channel_details["channel_comments"] = get_int_if_key_exists_or_default(channel['statistics'], "commentCount")
            return channel_details
        else:
            print("channel res >> ", channel_id, response)
            logger.error("channel res >> " + str(channel_id))
            logger.error(response)
            return {}
    except Exception as err:
        print("channel error >> ", err, channel_id)
        logger.error(channel_id)
        logger.error(err)
        return {}


def get_int_if_key_exists_or_default(dictionary, key, default_value=None):
    return int(dictionary[key]) if check_if_key_exists(dictionary, key) else default_value


def get_value_if_key_exists_or_default(dictionary, key, default_value=None):
    return dictionary[key] if check_if_key_exists(dictionary, key) else default_value


def check_if_key_exists(dictionary, key):
    return key in dictionary
