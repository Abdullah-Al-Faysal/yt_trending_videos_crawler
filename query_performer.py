import logging
from data_access_layer import get_connection

logging.basicConfig(filename='C:\__ Work Station\Py_Projects\YT_TV_Crawler\logs\query_performer.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)s - %(funcName)20s()] %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filemode='a')
logger = logging.getLogger(__name__)

target_regions = ["IN", "PK", "AE", "BD", "PH", "IR", "EG", "NP", "LK", "SY", "GB", "CN", "JO", "AF", "PS", "ZA", "LB",
                  "ET", "YE", "ID", "SS", "SA", "SO", "IQ", "US"]


def region_tv_intersection_walter_data():
    print("Region TV Intersect Walter Data >> ")
    queries = get_intersect_queries_walter_data()
    return execute_queries(queries)


def region_tv_intersection_yt_api_data():
    print("Region TV Intersect YT API Data >> ")
    queries = get_intersect_queries_yt_api_data()
    return execute_queries(queries)


def region_tv_intersection_inner_join_yt_api_data():
    print("Region TV Intersect YT API Data >> ")
    queries = get_intersect_inner_join_yt_api_data()
    return execute_queries(queries)


def execute_queries(queries):
    try:
        connection = get_connection()
        query_results = []
        with connection.cursor() as cursor:
            for query in queries:
                rows_affected = cursor.execute(query)
                result = cursor.fetchone()
                connection.commit()
                print("Query result >> ", result)
                query_results.append(result)
        return query_results
    except Exception as err:
        print("Query Execute Many error >> ", err)
        logger.error(err)
        return None
    finally:
        connection.close()


def get_intersect_queries_walter_data():
    queries = [
        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=IN'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=PK'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=BD'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=PH'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=IR'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=EG'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=NP'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=LK'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=SY'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=GB'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=CN'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=JO'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AF'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=PS'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=ZA'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=LB'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=ET'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=YE'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=ID'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=SS'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=SA'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=SO'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=IQ'))''',

        '''(SELECT Count(page_url)
        FROM walter_yt_trending_data 
        WHERE page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=AE') 
        AND page_url IN (SELECT page_url FROM walter_yt_trending_data  WHERE starting_page_url = 'https://www.youtube.com/feed/trending?gl=US'))'''
    ]
    return queries


def get_intersect_queries_yt_api_data():
    queries = [
        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'IN'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'PK'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'BD'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'PH'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'IR'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'EG'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'NP'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'LK'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'SY'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'GB'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'CN'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'JO'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AF'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'PS'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'ZA'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'LB'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'ET'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'YE'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'ID'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'SS'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'SA'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'SO'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'IQ'))''',

        '''(SELECT Count(video_id)
        FROM script_export_videos 
        WHERE video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'AE') 
        AND video_id IN (SELECT video_id FROM script_export_videos  WHERE trending_at = 'US'))'''
    ]
    return queries


def get_intersect_inner_join_yt_api_data():
    queries = [
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'IN';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'PK';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'BD';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'PH';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'IR';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'EG';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'NP';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'LK';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'SY';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'GB';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'CN';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'JO';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'AF';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'PS';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'ZA';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'LB';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'ET';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'YE';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'ID';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'SS';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'SA';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'SO';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'IQ';)''',
        '''(SELECT Count(t1.video_id) FROM script_export_videos as t1
           INNER JOIN script_export_videos as t2 ON t1.video_id = t2.video_id
           WHERE  t1.trending_at = 'AE' and t2.trending_at = 'US';)'''
    ]
    return queries


def get_region_tv_count(query):
    try:
        connection = get_connection()
        with connection.cursor() as cursor:
            rows_affected = cursor.execute(query)
            result = cursor.fetchall()
            connection.commit()
            print(result)
        return result
    except Exception as err:
        print("Region TV Count Query error >> ", err)
        logger.error(err)
        return []
    finally:
        connection.close()


def get_region_tv_count_walter_data():
    region_tvs = {}
    ordered_region_tvs = {}
    query = get_region_tv_count_query_walter_data()
    results = get_region_tv_count(query)
    for result in results:
        region_tvs[result["starting_page_url"].split('=')[1]] = result["total"]

    for region in target_regions:
        ordered_region_tvs[region] = region_tvs[region]

    print(ordered_region_tvs)
    return ordered_region_tvs


def get_region_tv_count_query_walter_data():
    return 'SELECT starting_page_url, count(*) as total FROM walter_yt_trending_data group by starting_page_url'


def get_region_tv_count_query_yt_api_data():
    return 'SELECT trending_at, count(*) as total FROM script_export_videos group by trending_at'


def get_region_tv_count_query_wce_data():
    return 'SELECT location, count(*) as total FROM wce_export_videos group by location'


if __name__ == "__main__":
    region_tv_intersection_walter_data()
    region_tv_intersection_yt_api_data()
    region_tv_intersection_inner_join_yt_api_data()
