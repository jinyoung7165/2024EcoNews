import psycopg2
import datetime, os, re

class PostgresDB:
    def __init__(self, db=None):
        self.today = datetime.datetime.now().strftime("%Y%m%d")
        if db is None: #crawler에서 호출
            self.db = psycopg2.connect(
            dbname = os.environ.get('postgres_db'),
            user = os.environ.get('postgres_username'),
            password = os.environ.get('postgres_password'),
            host = os.environ.get('postgres_host'),
            port = os.environ.get('postgres_port')
            )
            self.cursor = self.db.cursor()
        else:  # run.py에서 주입
            self.db = db
            self.cursor = db.cursor()
        print("PostgreSQL connection complete!")

    def __del__(self):
        self.cursor.close()
        self.db.close()
        
    
    def get_all_docs(self, keydate): # doc의 해당 날짜에 대한 모든 문서 [[id, main, title], [id, main, title]]
        self.cursor.execute("SELECT id, main, title FROM doc WHERE keydate = %s", (keydate,))
        date_docs = self.cursor.fetchall()
        date_docs_list = []
        for doc in date_docs:
            id = str(doc[0])  # id
            main = re.sub('[^A-Z a-z 0-9 가-힣 .]', '', doc[1]) # main
            title = re.sub('[^A-Z a-z 0-9 가-힣 .]', '', doc[2]) # title
            main += "." + title + "." + title + "." + title #제목 가중치 3배
            date_docs_list.append([id, main])
        self.db.commit()
        return date_docs_list

class RunDB:
    def __init__(self, db: PostgresDB, join_vector, hot_topic):
        self.db = db
        self.join_vector = join_vector
        self.hot_topic = hot_topic

    def setting(self):
        self.total_weight = sum([tup[1] for tup in self.hot_topic]) # hot_topic 20개의 총 빈도수 합
        self.hot_topic_words = [tup[0] for tup in self.hot_topic] # hot_topic 20개 단어 list

        self.inverted_joinv = self.join_vector.T # column, index 바꾼 df
        self.joinv_words = self.inverted_joinv.index.to_list() # df에 있는 단어들
        self.joinv_doc_name = self.join_vector.index.to_list() # ['2023-01-20_doc/0', '2023-01-20_doc/1']
        self.doc_dict = dict()

        hots = []
        for word in self.joinv_words:
            if word in self.hot_topic_words:
                hots.append(self.hottopic_documents(word))
                
        # hot tb 안에 documents 넣는다.
        self.insert_hot_topics(hots)
        print("hot topic insertion complete!")

        # 각 doc에 keyword 삽입
        for doc in self.joinv_doc_name:
            self.insert_each_doc_keyword(doc)
        print("each document keyword insertion complete!")
        
    @staticmethod
    def convert_list_to_semi_colon_string(lst):
        return ';'.join(map(str, lst))
    
    def insert_hot_topics(self, hots):
        cursor = self.db.cursor
        '''
        hots = [{'word': '건설', 'weight': 0.029048656499636893, 'doc': ['2904', '2857']...
        '''
        for hot in hots:
            cursor.execute("INSERT INTO hot (word, weight, keydate, doc_ids) VALUES (%s, %s, %s, %s)",
                        (hot['word'], hot['weight'], self.db.today, self.convert_list_to_semi_colon_string(hot['doc'])))
        self.db.db.commit()

    def hottopic_documents(self, word): #해당 단어의 결합 벡터가 0.1이상인 문서를 db에 저장
        idx = self.hot_topic_words.index(word)
        weight = self.hot_topic[idx][1] / self.total_weight # 전체 단어 빈도 수에 비한 현재 word의 빈도 수 -> wordcloud

        doc_joinv = dict() # "2022-01-20_doc/1": 0.5 문서명-결합벡터 저장
        doc_len = len(self.joinv_doc_name) #전체 문서 수
        df_idx = self.joinv_words.index(word) #전체 df에서 현재 단어의 위치
        for i in range(doc_len):
            if (self.inverted_joinv.iat[df_idx, i]> 0.1):
                doc_joinv[self.joinv_doc_name[i]] = self.inverted_joinv.iat[df_idx, i]

        # 해당 단어에 대한 결합벡터 높은 기사 순으로 doc 저장
        temp = sorted(doc_joinv.items(), key=lambda x:x[1], reverse=True)
        hot = {
            "word" : word,
            "weight" : weight,
            "doc" : [t[0].split("/")[1] for t in temp] # 날짜 0 / id 1
        }
        return hot

    def insert_each_doc_keyword(self, doc):
        # 핫 토픽 단어를 가진 document만 "2023-02-02_doc/0"형태로 collection으로 저장 
        for word in self.hot_topic_words:
            if word in self.join_vector.columns:
                if self.join_vector.loc[doc, word] > 0.0:
                    self.insert_keyword(doc)
                    break
            else:
                print(f"Warning: '{word}' not found in join_vector columns.")
                
    def insert_keyword(self, doc): # 해당 문서에 존재하는 단어들 중 keyword 추출        
        keydate, id = doc.split('/') #collection, _id
        cursor = self.db.cursor
        cursor.execute("SELECT * FROM doc WHERE id = %s AND keyword IS NOT NULL", (id,))
        if cursor.fetchone(): return  # 이미 keyword가 존재하는 doc이면 return
        
        word_joinv = dict() # 해당 문서에 존재하는 단어-결합벡터 저장
        len_word_in_df = len(self.joinv_words) # df에 있는 전체 단어 수
        df_idx = self.joinv_doc_name.index(doc)
        for i in range(len_word_in_df):
            if(self.join_vector.iat[df_idx, i] > 0.0): #결합벡터가 0보다 크면 keyword 후보로 등록
                word_joinv[self.join_vector.columns[i]] = self.join_vector.iat[df_idx, i]

        del word_joinv['total'] # total은 지우기
        temp = sorted(word_joinv.items(), key=lambda x:x[1], reverse=True)
        keyword_num = len(temp)

        # 키워드를 3개 이상 가지고 있으면 top3까지 저장 / 그렇지 않으면 1개만(top1)만 저장
        if(keyword_num >= 3):
            docu = {
                "keyword": [temp[0][0], temp[1][0], temp[2][0]]
            }
        else:
            docu = {
                "keyword": [temp[0][0]]
            }
        
        # 리스트를 세미콜론으로 구분된 문자열로 변환
        keyword_string = self.convert_list_to_semi_colon_string(docu['keyword'])
        
        cursor.execute("UPDATE doc SET keyword = %s WHERE id = %s", (keyword_string, id,))
        self.db.db.commit()
        
        cursor.execute("SELECT main FROM doc WHERE id = %s", (id,))
        main_f = cursor.fetchone()
        
        main = re.sub('[^A-Z a-z 0-9 가-힣 · % .]', '', main_f[0])
        self.doc_dict[doc] = main  # {'2023-02-20/_id' : '본문'} 형식으로 저장해서 summary에게 넘겨줌