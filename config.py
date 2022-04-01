
# FIXME: This should probably be in a config file
DATABASE_FILE = 'dtd.db'
CREDENTIALS = ('benjyjilks@gmail.com', '2n3gfJUdxGizAHF%')

HSP_API_URL = 'https://hsp-prod.rockshore.net/api/v1'
HSP_SERVICE_METRICS_API_URL = HSP_API_URL + '/serviceMetrics'
HSP_SERVICE_DETAILS_API_URL = HSP_API_URL + '/serviceDetails'

DARWIN_TOKEN = 'c77dcba0-aed9-426f-97b5-e52274822e42'
DARWIN_API_URL = 'https://lite.realtime.nationalrail.co.uk/OpenLDBWS/wsdl.aspx?ver=2016-02-16'

DISABLE_MASTODON = False
MASTODON_ACCESS_TOKEN = 'LbbDbxIR3FQsKjMKzfiI-0sTKkm5Nj2Ynj_wDou1gBM'
MASTODON_API_BASE_URL = 'http://0.0.0.0:3000/'

OPEN_WEATHER_MAP_API_KEY = 'a995e19bfab2d323ad9b624ebdd82054'

DOWNLOAD_CHUNK_SIZE = 1024 * 1024 # 1MB
MAX_NUMBER_OF_QUEUED_BATCH_STATEMENTS = 5
RECORD_CHUNK_SIZE = 1_00_000
SQL_BATCH_SIZE = 10_00_000
MAX_QUEUE_SIZE = int(SQL_BATCH_SIZE / RECORD_CHUNK_SIZE) * MAX_NUMBER_OF_QUEUED_BATCH_STATEMENTS

DISABLE_DOWNLOAD = False
BACKUP_DOWNLOADED_TO_LOCAL = False
LOCAL_FEED_STORAGE_BASE = './dtd_storage'

STANDARD_DATE_FORMAT = '%A %B %m %Y'
STANDARD_TIME_FORMAT = '%I:%M %p'
STANDARD_DATE_TIME_FORMAT = '%c'

DAYS_OF_WEEK = [
    'Monday',
    'Tuesday',
    'Wednesday',
    'Thursday',
    'Friday',
    'Saturday',
    'Sunday',
]

