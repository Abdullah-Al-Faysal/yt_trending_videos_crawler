from datetime import datetime
import json
import logging
import os.path

logging.basicConfig(filename='C:\__ Work Station\Py_Projects\YT_TV_Crawler\logs\yttv.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)s - %(funcName)20s()] %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filemode='a')
logger = logging.getLogger(__name__)


def export_as_json(data):
    try:
        file_Base_location = 'C:\\Users\\aafaysal\\[ MEGA ]\\UALR\\Spring 2019\\Thesis\\daily_json_dumps\\'
        daily_file_name = datetime.now().strftime("%Y-%m-%d-%H-%M") + ".json"
        compiled_file_name = "compiled.json"
        daily_file_path = os.path.join(file_Base_location + daily_file_name)
        compiled_file_path = os.path.join(file_Base_location + compiled_file_name)
        with open(daily_file_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)

        with open(compiled_file_path, 'a+') as json_file:
            if os.path.getsize(compiled_file_path) == 0:
                json_file.seek(0)
                json.dump([], json_file, indent=4)
            json_file.seek(0)
            prev_data = json.load(json_file)
            data = prev_data + data
            json_file.seek(0)
            json_file.truncate()
            json.dump(data, json_file, indent=4)
        return None
    except Exception as err:
        print("json export error >> ", err)
        logger.error(err)
        return None
