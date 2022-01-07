
# FIXME: This should probably be in a config file
DATABASE_FILE = 'dtd.db'
DTD_EXPIRY = 60 * 60 * 24 * 365 # 1 year
CREDENTIALS = ('benjyjilks@gmail.com', '2n3gfJUdxGizAHF%')

DOWNLOAD_CHUNK_SIZE = 1024 * 1024 # 1MB
MAX_NUMBER_OF_QUEUED_BATCH_STATEMENTS = 5
RECORD_CHUNK_SIZE = 1_00_000
SQL_BATCH_SIZE = 10_00_000
MAX_QUEUE_SIZE = int(SQL_BATCH_SIZE / RECORD_CHUNK_SIZE) * MAX_NUMBER_OF_QUEUED_BATCH_STATEMENTS

DISABLE_DOWNLOAD = False
LOCAL_DTD_STORAGE = { 
    '2.0/fares': './dtd_fares',
    '3.0/timetable': './dtd_timetable',
}

