from data_access_layer import save_videos
from json_export import export_as_json
from yt_trending_videos import get_daily_trending_videos, get_trending_videos
from url_builder import UrlBuilder

if __name__ == "__main__":
    # url_builder = UrlBuilder("AIzaSyA4eYXd8rTRfpTlOORg3a7vGMi9vPsOfjA")
    # print(url_builder.build_video_url("AE"))
    # print(url_builder.build_channel_url("UCBNkm8o5LiEVLxO8w0p2sfQ"))
    # results = get_trending_videos("IN", "https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=IN&key=AIzaSyDW8NL-Tdf4BknvHhu4GWE8D_9JjlIfnZ8")
    results = get_daily_trending_videos()
    save_videos(results)
    export_as_json(results)
