import logging

logging.basicConfig(filename='C:\__ Work Station\Py_Projects\YT_TV_Crawler\yttv.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)s - %(funcName)20s()] %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filemode='a')
logger = logging.getLogger(__name__)

YT_API_KEY = "AIzaSyA4eYXd8rTRfpTlOORg3a7vGMi9vPsOfjA"


def build_video_url(region_code, part="snippet,statistics", chart="mostPopular", max_results=50):
    try:
        base_url = "https://www.googleapis.com/youtube/v3/videos?"
        url_params = {
            "part": part,
            "chart": chart,
            "max_results": str(max_results),
            "region_code": region_code,
            "key": YT_API_KEY
        }
        generated_url = build_url(base_url, url_params)
        return generated_url
    except Exception as err:
        print("Video url build error >> ", err)
        logger.error(err)
        return ""


def build_channel_url(channel_id, part="snippet,statistics", max_results=50):
    try:
        base_url = "https://www.googleapis.com/youtube/v3/channels?"
        url_params = {
            "id": channel_id,
            "part": part,
            "max_results": str(max_results),
            "key": YT_API_KEY
        }
        generated_url = build_url(base_url, url_params)
        return generated_url
    except Exception as err:
        print("Channel url build error >> ", err)
        logger.error(err)
        return ""


def build_url(base_url, url_params):
    try:
        url = base_url
        for (param, value) in url_params.items():
            if "," in value:
                parts = value.split(',')
                value = '%2C'.join(parts)
            url += ("" if url[-1] == '?' else "&") + snake_case_to_lower_camel_case(param) + "=" + value
        return url
    except Exception as err:
        print("url build error >> ", err)
        logger.error(err)
        return ""


def snake_case_to_lower_camel_case(text):
    parts = text.split("_")
    result = parts[0] + (''.join(x.title() for x in parts[1:]) if len(parts) > 1 else '')
    # print("camel cased >> ", result)
    return result


# if __name__ == "__main__":
#     print(build_video_url("AE"))
#     print(build_channel_url("UCBNkm8o5LiEVLxO8w0p2sfQ"))

