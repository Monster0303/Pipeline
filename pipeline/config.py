HOST = '127.0.0.1'
USER = 'test'
PASSWD = '123qwe'
DATABASE = 'pipeline'
PARAMS = "charset=utf8mb4"

URL = f'mysql+pymysql://{USER}:{PASSWD}@{HOST}/{DATABASE}?{PARAMS}'

DATABASE_DEBUG = False
