import pymysql
import logging

logging.basicConfig(filename='C:\__ Work Station\Py_Projects\YT_TV_Crawler\yttv.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)s - %(funcName)20s()] %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filemode='w')
logger = logging.getLogger(__name__)


def get_connection():
    try:
        connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='r00t',
                                     db='walter_yt_data',
                                     charset='utf8mb4',
                                     use_unicode=True,
                                     cursorclass=pymysql.cursors.DictCursor)
        return connection
    except Exception as err:
        print("db connection error >> ", err)
        logger.error(err)
        return None


def save_videos(all_videos):
    try:
        connection = get_connection()
        counter = 0
        query = """INSERT INTO script_export_videos (video_id, video_title, category, published_date, views, likes, 
                dislikes, comments,  extracted_date, language, channel_id, channel_title, channel_url, channel_language,
                channel_views, channel_subscribers, channel_videos, channel_comments, location) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        with connection.cursor() as cursor:
            for video in all_videos:
                result = cursor.execute(query, (
                    video["video_id"],
                    video["video_title"],
                    video["category"],
                    video["published_date"],
                    video["views"],
                    video["likes"],
                    video["dislikes"],
                    video["comments"],
                    video["extracted_date"],
                    video["language"],
                    video["channel_id"],
                    video["channel_title"],
                    video["channel_url"],
                    video["channel_language"],
                    video["channel_views"],
                    video["channel_subscribers"],
                    video["channel_videos"],
                    video["channel_comments"],
                    video["location"]
                ))
                connection.commit()
                counter += 1
                print(counter)
        print("Insert Counter >> ", counter)
        return None
    except Exception as err:
        print("DB Write error >> ", err)
        logger.error(err)
        return None
    finally:
        connection.close()
