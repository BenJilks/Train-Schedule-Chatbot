
# FIXME: This should probably be in a config file
DATABASE_FILE = 'dtd.db'
DTD_EXPIRY = 60 * 60 * 24 * 365 # 1 year
CREDENTIALS = ('benjyjilks@gmail.com', '2n3gfJUdxGizAHF%')

HSP_API_URL = 'https://hsp-prod.rockshore.net/api/v1'
HSP_SERVICE_METRICS_API_URL = HSP_API_URL + '/serviceMetrics'
HSP_SERVICE_DETAILS_API_URL = HSP_API_URL + '/serviceDetails'

DARWIN_TOKEN = 'c77dcba0-aed9-426f-97b5-e52274822e42'
DARWIN_API_URL = 'https://lite.realtime.nationalrail.co.uk/OpenLDBWS/wsdl.aspx?ver=2016-02-16'

DOWNLOAD_CHUNK_SIZE = 1024 * 1024 # 1MB
MAX_NUMBER_OF_QUEUED_BATCH_STATEMENTS = 5
RECORD_CHUNK_SIZE = 1_00_000
SQL_BATCH_SIZE = 10_00_000
MAX_QUEUE_SIZE = int(SQL_BATCH_SIZE / RECORD_CHUNK_SIZE) * MAX_NUMBER_OF_QUEUED_BATCH_STATEMENTS

DISABLE_DOWNLOAD = False
BACKUP_DOWNLOADED_TO_LOCAL = False
LOCAL_FEED_STORAGE_BASE = './dtd_storage'

