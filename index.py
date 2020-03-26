from data_access_layer import save_videos
from yt_trending_videos import get_daily_trending_videos, get_trending_videos

if __name__ == "__main__":
    # result = get_trending_videos("https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&chart=mostPopular&maxResults=50&regionCode=IN&key=AIzaSyDNTGIwf-TsAweg6-yFHWR4ZTkFPYlaeVE")
    results = get_daily_trending_videos()
    print(len(results))
    save_videos(results)
