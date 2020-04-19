from csv import DictReader
import pycountry
from datetime import datetime
import json
import os
import sys, traceback
import logging
from data_access_layer import get_connection
import re

logging.basicConfig(filename='C:\__ Work Station\Py_Projects\YT_TV_Crawler\logs\wce.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)s - %(funcName)20s()] %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filemode='a')
logger = logging.getLogger(__name__)


def load_data_into_dicts():
    try:
        input_file_path = get_file_path("yttv_uae_filtered.csv")
        with open(input_file_path, 'r', encoding="utf8") as read_obj:
            dict_reader = DictReader(read_obj)
            records = list(dict_reader)
            print(records)
        return records
    except Exception as err:
        print("Csv Read error >> ", err)
        logger.error(err)
        return []


def export_dicts_to_json():
    output_file_path = get_file_path("yttv_uae.json")
    records = load_data_into_dicts()
    write_to_json(output_file_path, records)
    return None


def write_to_json(output_file_path, records):
    try:
        with open(output_file_path, 'w', encoding="utf8") as json_file:
            json.dump(records, json_file, indent=4)
        return None
    except Exception as err:
        print("Write Json error >> ", err)
        logger.error(err)
        return None


def load_data_from_json(input_file_path):
    try:
        with open(input_file_path, 'r', encoding="utf8") as json_file:
            records = json.load(json_file)
            # print(records, len(records))
        return records
    except Exception as err:
        print("Json Read error >> ", err)
        logger.error(err)
        return []


def filter_data():
    try:
        records = load_data_from_json(get_file_path("yttv_uae.json"))
        filtered_records = []
        total_records = len(records)
        print(total_records)
        i = 0
        while i < total_records:
            print(records[i]["id"], " >> ", i)
            records[i]["video_id"] = records[i]["crawled_url"].split("v=")[1]
            records[i]["channel_id"] = records[i]["channel_url"].split("channel/")[1]
            records[i]["views"] = de_humanize_large_number(records[i]["views"])
            records[i]["likes"] = de_humanize_large_number(records[i]["likes"])
            records[i]["dislikes"] = de_humanize_large_number(records[i]["dislikes"])
            records[i]["subscribers"] = convert_si_to_number(records[i]["subscribers"])
            records[i]["published_at"] = get_mysql_date_string(records[i]["published_at"])
            records[i]["extracted_date"] = get_mysql_date_string(records[i]["extracted_date"], "%Y/%m/%d %H:%M")
            jump = 1
            while i+jump < total_records and records[i + jump]["video_title"] == "":
                jump += 1
            if jump >= 3:
                country_code_map = {country.name: country.alpha_2 for country in pycountry.countries}
                records[i]["location"] = country_code_map.get(records[i + 2]["location"], "N/A")
            else:
                records[i]["location"] = "N/A"
            filtered_records.append(records[i])
            i += jump
        print(filtered_records)
        write_to_json(get_file_path("yttv_uae_filtered.json"), filtered_records)
        return filtered_records
    except Exception as err:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("Filter Data error >> ", repr(err))
        traceback.print_exception(exc_type, exc_value, exc_traceback)
        logger.exception(err)
        return None


def write_wce_data_to_db():
    try:
        connection = get_connection()
        records = load_data_from_json(get_file_path("yttv_uae_filtered.json"))
        counter = 0
        query = """INSERT INTO wce_export_videos (video_id, video_title, trending_at, category, published_date, views, likes, 
                dislikes, comments,  extracted_date, language, channel_id, channel_title, channel_url, channel_language,
                channel_views, channel_subscribers, channel_videos, channel_comments, location) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        with connection.cursor() as cursor:
            for video in records:
                # print(video["published_at"], print(video["id"]))
                result = cursor.execute(query, (
                    video["video_id"],
                    video["video_title"],
                    "AE",
                    "N/C",
                    video["published_at"],
                    video["views"],
                    video["likes"],
                    video["dislikes"],
                    None,
                    video["extracted_date"],
                    "N/C",
                    video["channel_id"],
                    video["channel_title"],
                    video["channel_url"],
                    "N/C",
                    None,
                    video["subscribers"],
                    None,
                    None,
                    video["location"]
                ))
                connection.commit()
                counter += 1
        print("WCE Records Insert Counter >> ", counter)
        return None
    except Exception as err:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("WCE Records DB Write error >> ", err)
        traceback.print_exception(exc_type, exc_value, exc_traceback)
        logger.exception(err)
        return None
    finally:
        connection.close()


def de_humanize_large_number(value, separator=','):
    try:
        # ^[0-9\,]*$
        if value == "" or value.startswith("Sign"):
            return None
        return int("".join(value.split(separator)))
    except Exception as err:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("Dehumanize error >> ", err, value)
        traceback.print_exception(exc_type, exc_value, exc_traceback)
        logger.exception(err)
        return None


def convert_si_to_number(x):
    try:
        # ^[0-9]*[\.]?[0-9]*[KMB]?$
        if x == "" or x.startswith("Loading"):
            return None
        total_stars = 0
        if 'K' in x:
            if len(x) > 1:
                total_stars = float(x.replace('K', '')) * 1000
        elif 'M' in x:
            if len(x) > 1:
                total_stars = float(x.replace('M', '')) * 1000000
        elif 'B' in x:
            total_stars = float(x.replace('B', '')) * 1000000000
        else:
            total_stars = int(x)
        return int(total_stars)
    except Exception as err:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("Convert Si to Num error >> ", err, x)
        traceback.print_exception(exc_type, exc_value, exc_traceback)
        logger.exception(err)
        return None


def get_mysql_date_string(date_string, parse_format="%b %d, %Y"):
    if date_string == "":
        return None
    datetime_obj = datetime.strptime(date_string, parse_format)
    return datetime_obj.strftime('%Y-%m-%d %H:%M:%S')


def get_file_path(file_name, relative_location="files/", base_location=os.path.dirname(__file__)):
    file_path = os.path.join(base_location, relative_location, file_name)
    return file_path


if __name__ == "__main__":
    print(get_file_path("yttv_uae_filtered.json"))
    # filter_data()
    # write_wce_data_to_db()
