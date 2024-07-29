import datetime
from gensim.models import Word2Vec, FastText
from functools import partial
from multiprocessing import Manager, Pool

import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from remote.s3_method import S3
from preprocess.doc_text import DocToText
from preprocess.tokenizer import Tokenizer

def preprocess(doc, whole_word, tokenizer: Tokenizer):
    whole_word.append(tokenizer.sentence_tokenizer(doc))

def custom_train(docToText: DocToText, tokenizer: Tokenizer):
    whole_word = Manager().list()
    delta = datetime.timedelta(days=1) # 1일 후
    end_date = datetime.datetime.now()- datetime.timedelta(days=18) #2023-03-03
    now_date = end_date - datetime.timedelta(days=45) # 45일 전.2023-01-17
    print(end_date, now_date)
    while True:
        docToText.csv_to_text("{}".format(now_date.date()), "naver_news.csv")
        docs = list(docToText.main) # ["첫번째 문서 두번째 문장", "두번째 문서 두번째 문장",]
        
        with Pool(processes=8) as pool:
            pool.map(partial(preprocess, whole_word=whole_word, tokenizer=tokenizer), docs) #해당 일자의 문서 수*[토큰들] 2중 리스트
        if(now_date.date() == end_date.date()):
            break
        now_date += delta # 1일씩 증가해주기
    print(len(whole_word))  #24902건 나옴 [[], [], [], ...]
        # skip-gram이 좋은 것 같다.'외교부'는 '이란'이 가장 동일하게 나옴
    model = Word2Vec(sentences = list(whole_word), vector_size=128, min_count = 2, 
                        window = 5, workers = 3, sg = 1)
    model.save("model_word")
    
    model2 = FastText(sentences = list(whole_word), vector_size=128, min_count = 2, 
                    window = 5, workers = 3, sg = 1)
    model2.save("model_fast")
               
if __name__ == '__main__':
    s3 = S3() #s3 connection 1번     
    docToText = DocToText(s3)
    tokenizer = Tokenizer()
    word2vec = custom_train(docToText, tokenizer)
    #하루치 26초(470문서), 70초(1179문서)