import pymysql


def get_connection():
    connection = pymysql.connect(host='localhost',
                                 user='yttrackers',
                                 password='YtSummer2018',
                                 db='youtube_tracker',
                                 charset='utf8',
                                 use_unicode=True,
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection
