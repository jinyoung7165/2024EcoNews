import datetime
import pandas as pd
import time
from threading import Thread
from multiprocessing import Manager, Process
from bs4 import BeautifulSoup
import requests
import re

from psql_method import PostgresDB

from dotenv import load_dotenv

import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

# load .env
load_dotenv()


label = ["링크", "언론사", "이미지", "제목", "날짜", "본문"]
filename = 'naver_news.csv'

def current_page_items(pageIdx, return_list): #전체페이지에서 각 기사의 링크, 메타데이터 저장해둠
    try:
        page_url = "https://news.naver.com/main/list.nhn?mode=LSD&mid=sec&sid1=101&date={}&page={}".format(today.strftime("%Y%m%d"), pageIdx)
        all_list = requests.get(page_url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'})
        all_html = BeautifulSoup(all_list.text, 'html.parser')
        time.sleep(1.5)
        
        all_items = all_html.select("div.list_body.newsflash_body > ul.type06_headline > li > dl")
        all_items.extend(all_html.select("div.list_body.newsflash_body > ul.type06 > li > dl"))
        
        def get_press_url_thread(item, return_list):
            item_press = item.select("dd > span.writing")[0].text
            photo_or_not = item.select("dt")
            url_headline = photo_or_not[1].find("a") if len(photo_or_not) > 1 \
                else photo_or_not[0].find("a") #대표 사진 없는 기사
            item_url = url_headline.get("href")
            item_image = photo_or_not[0].select_one("a > img").attrs["src"] if len(photo_or_not) > 1 \
                else None #대표 사진 없는 기사
            if item_image: item_image = item_image[:item_image.find('?type=')]
            return_list.append([item_url, item_press, item_image]) #링크, 언론사, 사진

        ths = []
        for item in all_items:
            th = Thread(target=get_press_url_thread, args=(item, return_list))
            th.start()
            ths.append(th)
        for th in ths:
            th.join()

    except Exception as e:
        print(e)
        return False

def get_news_content_thread(idx, return_list, return_len): #각 기사에서 뉴스 전문 가져옴
    ths = []
    for idx_thread in range(idx, return_len, return_len//2 - 1):
        th = Thread(target=get_news_content, args=(idx_thread, return_list))
        th.start()
        ths.append(th)
    for th in ths:
        th.join()
            
def get_news_content(idx, return_list):
    try:
        news = requests.get(return_list[idx][0], headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'})
        news_html = BeautifulSoup(news.text, "html.parser")
        time.sleep(1.5)
       
        # html태그제거 및 텍스트 다듬기
        br = '<br/>'
        pattern1 = '<[^>]*>'
        pattern2 = """[\n\n\n\n\n// flash 오류를 우회하기 위한 함수 추가\nfunction _flash_removeCallback() {}"""
        pattern3 = '[a-zA-Z0-9+-_.]+@[a-zA-Z0-9-]+[a-zA-Z0-9-.]+' #email 제거
        pattern4 = r'[\n]' #엔터 제거
        pattern5 = r'\([^)]*\)' #괄호와 괄호 안 글자 제거
        pattern6 = r'\[[^]]*\]' #괄호와 괄호 안 글자 제거
        pattern7 = r'\【[^】]*\】'
        pattern8 = r'\◀[^▶]*\▶'
        lgt = "(?:&[gl]t;)+"
        amp = "(?:&amp;)+"
        
        # 뉴스 제목 가져오기
        item_title = news_html.select_one("#ct > div.media_end_head.go_trans > div.media_end_head_title > h2")
        if item_title == None:
            item_title = news_html.select_one("#content > div.end_ct > div > h2")

        # 날짜 가져오기
        try:
            html_date = news_html.select_one("div#ct> div.media_end_head.go_trans > div.media_end_head_info.nv_notrans > div.media_end_head_info_datestamp > div > span")
            item_date = html_date.attrs['data-date-time']
        except AttributeError:
            item_date = news_html.select_one("#content > div.end_ct > div > div.article_info > span > em")
            item_date = re.sub(pattern=pattern1,repl='',string=str(item_date))

        # 본문 가져오기
        text_area = news_html.select("article#dic_area")
        
        if len(text_area) == 0: text_area = news_html.select("#articeBody")
        content = ''.join(str(text_area))
        item_title = re.sub(pattern=pattern1, repl='', string=str(item_title))
        item_title = re.sub(pattern=lgt, repl='', string=item_title)
        item_title = re.sub(pattern=amp, repl='', string=item_title)
        
        content = re.sub(pattern=br, repl='. ', string=content)
        content = re.sub(pattern=pattern1, repl='', string=content)
        content = content.replace(pattern2, '')
        
        content = content.replace('[','', 1)
        content = content.rstrip(']')
        
        content = re.sub(pattern=pattern3, repl='', string=content)
        content = re.sub(pattern=lgt, repl='', string=content)
        content = re.sub(pattern=amp, repl='', string=content)
        content = re.sub(pattern=pattern4, repl='', string=content)
        content = re.sub(pattern=pattern5, repl='', string=content)
        content = re.sub(pattern=pattern6, repl='', string=content)
        content = re.sub(pattern=pattern7, repl='', string=content)
        content = re.sub(pattern=pattern8, repl='', string=content)
        
        content = '.'.join(c for c in content.split('. ') if len(c)>1)
        return_list[idx] =  return_list[idx]+[item_title, item_date, content]
        
    except Exception as e:
        print(e)
        return False
        
def convert_csv(return_list):
    result = pd.DataFrame(return_list, columns = label)
    result.to_csv(filename, encoding="utf-8-sig")
    
def save_in_postgres(postgresDb, return_list):
    cursor = postgresDb.cursor
    insert_query = """
        INSERT INTO doc (link, press, image, title, date, main, keydate)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    for i in range(len(return_list)):
        doc = return_list[i]
        cursor.execute(insert_query, (doc[0], doc[1], doc[2], doc[3], doc[4], doc[5], today.strftime("%Y%m%d")))

    postgresDb.db.commit() # 변경사항을 커밋
    cursor.close()

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]
            
def crawl(today):
    print(today, "오늘의 crawl 시작")
    return_list = Manager().list()

    plimit = 20
    print("process limit: ", plimit)
    # 멀티프로세싱 
    processes = []
    
    for i in range(1, 11): # 20*10 -> 200개. local 124.08130264282227
        current_page_items(i, return_list)
    
    # 각 기사에서 url 통해 본문 가져오기
    for i in range(len(return_list)//2 - 1):
        process = Process(target=get_news_content_thread, args=(i, return_list, len(return_list)))
        processes.append(process)
      
    for process_chuck in chunks(processes, plimit):
        # 멀티프로세스 시작
        for process in process_chuck:
            process.start()
        # 멀티프로세스 종료
        for process in process_chuck:
            process.join()
            
    postgresDb = PostgresDB()
    convert_csv(list(return_list))
    save_in_postgres(postgresDb, return_list)
    postgresDb.db.close()  # 연결 종료
    return today
    
if __name__ == '__main__': # 30일 동안 50페이지*20건씩 뉴스 수집
    start = time.time()
    
    end_date = datetime.datetime.now()
    today = datetime.datetime.now() # 하루 전날부터 실행
    
    for i in range(3): # 50 * 30
        target = crawl(today)
        today -= datetime.timedelta(days=1)
        
    print(time.time() - start)