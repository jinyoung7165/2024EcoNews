import os
from dotenv import load_dotenv
from crawl import crawl

# load .env
load_dotenv()

# define Lambda handler
def handler(event=None, context=None):
    os.chdir('/tmp')
    # crawl success -> return today_date
    today_date = crawl()
    return {
        "date": today_date
    }