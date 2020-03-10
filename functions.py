import dateparser
from langdetect import detect


# get playlist id from channel id
def get_playlist_id(channel_id):
    cil = list(channel_id)
    cil[1] = 'U'
    playlist_id = ''.join(cil)
    return playlist_id


# normalize date
def normalize_metadate(datetime):
    dateObj = dateparser.parse(datetime)
    date = dateObj.strftime('%Y-%m-%d %H:%M:%S') if dateObj is not None else ''
    return date


# get category from id
def get_category(id):
    category_list = {
        '1': 'Film & Animation',
        '2': 'Autos & Vehicles',
        '10': 'Music',
        '15': 'Pets & Animals',
        '17': 'Sports',
        '18': 'Short Movies',
        '19': 'Travel & Events',
        '20': 'Gaming',
        '21': 'Videoblogging',
        '22': 'People & Blogs',
        '23': 'Comedy',
        '24': 'Entertainment',
        '25': 'News & Politics',
        '26': 'Howto & Style',
        '27': 'Education',
        '28': 'Science & Technology',
        '29': 'Nonprofits & Activism',
        '30': 'Movies',
        '31': 'Anime/Animation',
        '32': 'Action/Adventure',
        '33': 'Classics',
        '34': 'Comedy',
        '35': 'Documentary',
        '36': 'Drama',
        '37': 'Family',
        '38': 'Foreign',
        '39': 'Horror',
        '40': 'Sci-Fi/Fantasy',
        '41': 'Thriller',
        '42': 'Shorts',
        '43': 'Shows',
        '44': 'Trailers'
    }
    return category_list[id]


# detect language of channel description
def get_language(description):
    try:
        post_lang = detect(description)
    except:
        post_lang = 'N/A'
    return post_lang

# get video id from url
# def get_video_id(video_url):
#     query = urlparse(video_url)
#     if query.hostname == 'youtu.be':
#         return query.path[1:]
#     if query.hostname in ('www.youtube.com', 'youtube.com', 'm.youtube.com'):
#         if query.path == '/watch':
#             p = query.query
#             return p[2:]
#         if query.path[:7] == '/embed/':
#             return query.path.split('/')[2]
#         if query.path[:3] == '/v/':
#             return query.path.split('/')[2]
#     return None


# input video url 
# video_url = input('Enter youtube video URL: ')                                        

# get video id from url
# video_id = get_video_id(video_url) 


# randomly get api key through api key rotation
# with open('api.json') as f:
#     api_data = json.load(f)

# apis = api_data[0]["api_keys"]

# for api in apis:
#     api_key = api["key"]

# channel_id
# channel_id = urlparse(user_input).path.strip('/channel/')
