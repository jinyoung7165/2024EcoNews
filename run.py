import datetime, time, collections, sys, os, requests, pickle
from dotenv import load_dotenv
from gensim.models import FastText

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from preprocess.tokenizer import Tokenizer
from weighting.doc_tfidf import DocTfidf
from weighting.sentence import Sentence
from remote.psql_method import PostgresDB, RunDB
from summary.summary import Summary
# load .env
load_dotenv()

def main():
    postgresDb = PostgresDB()
    
    tokenizer = Tokenizer()
    fastvec = FastText.load('model_fast')
    
    doc_word_dict = collections.defaultdict(list)
    
    delta = datetime.timedelta(days=1) # 1일 후
    end_date = datetime.datetime.now()
    today = end_date - datetime.timedelta(days=2) #이틀 전부터 실행
    for _ in range(3):
        today_name = today.strftime("%Y%m%d")
        file_path = '/tmp/{}.pickle'.format(today_name)
        
        now_t = time.time()
        today_word_arr = {}
        if os.path.exists(file_path): # 이미 해당 날짜의 sentence 처리 결과 있으면 파일 불러옴
            print("exists")
            with open(file_path, 'rb') as fr:
                today_word_arr = pickle.load(fr)
        else:
            print("not exists")
            sentence = Sentence(postgresDb, tokenizer, fastvec, today_name) # 새로운 날짜 처리
            sentence.doc_process()
            with open(file_path, 'wb') as fw:
                today_word_arr = sentence.docs_word_arr
                pickle.dump(today_word_arr, fw)
    
        doc_word_dict.update(today_word_arr) # { date/idx : ['단어1','단어2'].... }
        print(time.time()- now_t)
        today += delta # 하루씩 증가

    now_t = time.time()
    doc_tfidf = DocTfidf(fastvec, doc_word_dict)
    join_vector = doc_tfidf.final_word_process()
    print("joinvector", time.time()-now_t)
    
    now_t = time.time()
    hot_topic = doc_tfidf.hot_topic() # 40개
    print("hottopic", time.time()-now_t)
    
    
    now_t = time.time()
    run_db = RunDB(postgresDb, join_vector, hot_topic)
    run_db.setting()
    print("rundb", time.time()-now_t)

    doc_main_dict = run_db.doc_dict # summary에 넘겨줄 요약 대상 {"20230201/id" : "본문 내용"}
    
    now_t = time.time()
    summary = Summary(postgresDb, hot_topic, doc_main_dict)
    summary.setting()
    print("summary", time.time()-now_t)
    
    # requests.get(os.environ.get('econews_update_uri'))
    # print("today econews update finished")

if __name__ == '__main__':
    target = main()