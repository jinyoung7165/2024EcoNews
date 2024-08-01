import requests, json, os
from preprocess.tokenizer import SummaryStopword
from remote.psql_method import PostgresDB

class Summary:
    def __init__(self, db:PostgresDB, hot_topic, doc_main_arr):
        self.hot_topic = hot_topic
        self.doc_main_arr = doc_main_arr # 요약할 기사들의 본문(사전)
        self.db = db # db
        self.stopword = SummaryStopword().stopwords

    def setting(self): 
        cursor = self.db.cursor
        for key, value in self.doc_main_arr.items():
            keydate, id = key.split('/') # 날짜, id
            
            cursor.execute("SELECT * FROM doc WHERE id = %s AND summary IS NOT NULL", (id,))
            if cursor.fetchone(): continue #이미 summary가 존재하는 doc이면 continue
            docu = {
                'summary': self.summarize_text(self.preprocess(value))
            }
            cursor.execute("UPDATE doc SET summary = %s WHERE id = %s", (docu['summary'], id,))
            self.db.db.commit()
        print("summary update complete!")

    # 해당 줄에 stopword 포함 시 제거
    def preprocess(self, text):
        lines = text.split('.')
        newlines = []
        for line in lines:
            flag = True
            for word in line.split():
                if word in self.stopword:
                    flag = False
                    break
            if flag: newlines.append(line)
                    
        return '. '.join(newlines)

    # 요약 함수
    def summarize_text(self, text):
        client_id = os.environ.get('summary_id')
        client_secret = os.environ.get('summary_secret')

        # 2000개 단어 기준으로 chunk 쪼개기
        text_chunks = [text[i:i+2000] for i in range(0, len(text), 2000)]
        headers = {
            "Content-Type": "application/json; utf-8",
            "X-NCP-APIGW-API-KEY-ID": client_id,
            "X-NCP-APIGW-API-KEY": client_secret,
        }

        summaries = []
        
        # chunk 단위로 요약 api와 연동
        for chunk in text_chunks:
            data = {
                "document": {
                    "content": chunk,
                },
                "option": {
                    "language": "ko", # 한국어
                    "model": "news", # 뉴스 타겟
                    "summaryCount" : "3", # 3문장
                    "tone": "0" # 원문 톤
                },
            }

            res = requests.post("https://naveropenapi.apigw.ntruss.com/text-summary/v1/summarize",
                                headers=headers, data=json.dumps(data))

            rescode = res.status_code
            summary = ""
            if(rescode == 200):
                summary = json.loads(res.text)["summary"]
                summaries.append(summary)
                
            if len(summaries) == 0:
                summaries.append("Insufficient valid sentence")

        final_summary = "\n".join(summaries)

        return final_summary