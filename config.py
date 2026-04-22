BASE_URL = "https:/wuzzuf.net"
LISTING_PATH = "/search/jobs?start={test_page_number}"
test_page_number = 10
PROGRESS_FILE = "output/.progress.json"
OUTPUT_DIR = "output"
OUTPUT_FILE = "wuzzuf_jobs.csv"

MAX_RETRIES = 3
RETRY_DELAY = 5

TOTAL_PAGES = 5
BATCH_SIZE = 30
CONCURRENCY = 3