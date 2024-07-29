from sklearn.feature_extraction.text import TfidfVectorizer
from numpy import dot
from numpy.linalg import norm
from collections import defaultdict
from functools import lru_cache
from multiprocessing import Process, Manager

import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from weighting.arr_util import ArrUtil
from preprocess.doc_text import DocToText
from preprocess.tokenizer import Tokenizer

@lru_cache(maxsize=3)
def sentence(docToText: DocToText, model, date, filename):
    docToText.csv_to_text(date, filename)
    docs = list(docToText.main) # ["첫번째 문서 두번째 문장 중복 문장", "두번째 문서 두번째 문장",]
    docs_word_arr = defaultdict(list) #문서별 가진 단어 배열
    #문서별 문장 list 저장 # {0: ["첫번째", "문서", "두번째", "문장", "중복", "문장"]}
    docs_process(docs, docs_word_arr, model, date)
    return docs_word_arr


def docs_process(docs, docs_word_arr, model, date): # -> 모든 문서에 대해 문장유사도 df뽑을꺼 필요없는 문장 제거
    return_list = Manager().list()
    procs = []
    plimit = 20
    # 한 문서에서 문장 리스트 뽑음
    for idx, doc in enumerate(docs): #idx:문서 번호 - 전처리 후 문장 0개면 pass할 거라 문서번호까지 df에 나타내자
        proc = Process(target=doc_process, args=(idx, doc, return_list, model, date))
        procs.append(proc)
        
    for process_chuck in chunks(procs, plimit):
        # 멀티프로세스 시작
        for process in process_chuck:
            process.start()
        # 멀티프로세스 종료
        for process in process_chuck:
            process.join()
    
    for doc_words in return_list:
        key, *val = doc_words
        docs_word_arr[key] = val
    
def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]
        
def doc_process(idx, doc, return_list, model, date):
    word_lines = [] # ["첫번째 문서", "두번째 문장"]
    line_word = [] #문장별 단어 배열 #[["첫번째", "문서"], ["두번째", "문장]]
    
    row = doc.split('.')
    preprocess(row, word_lines, line_word)
    line_count = len(word_lines) #문장 수
    if not line_count: return
    
    df1 = statistical_similarity(line_count, tfidf(word_lines)) #통계적 유사도
    df2 = semantic_similarity(line_count, line_word, model) #의미적 유사도
    sum_df = df1.add(df2) #유사도 결합
    delete_count = int(line_count*0.14) if line_count*0.14 > 1 else 1 #제거할 줄 수
    delete_idx_arr = sum_df.sort_values(by=line_count, ascending=True).head(delete_count).index #제거할 줄의 idx
    
    doc_words = [] #해당 문서가 가진 단어 배열
    key = "{}/{}".format(date, idx)
    doc_words.append(key)
    for i in range(line_count):
        if i not in delete_idx_arr:
            doc_words.extend(line_word[i])
    return_list.append(doc_words)
                    
def preprocess(row, word_lines, line_word): #문서 내 각 열(row)의 문장(line) 형태소 분석 + 불용어 제거
    tokenizer = Tokenizer()
    for line in row: #한 줄씩 처리 line:"앵커 어쩌고입니다"
        after_stopword = tokenizer.sentence_tokenizer(line)
        if after_stopword:
            line_word.append(after_stopword)
            word_lines.append(' '.join(after_stopword))

def tfidf(word_lines): #한 문서의 wordline에 대한 tfidf arr 리턴
    tfidf = TfidfVectorizer().fit(word_lines)
    return tfidf.transform(word_lines).toarray()

def statistical_similarity(line_count, tfidf_arr): #문장 수, tfidf
    def cosine_similarity(sentence1, sentence2):
        norms = norm(sentence1) * norm(sentence2)
        if norms == 0: return 0
        return dot(sentence1, sentence2) / norms

    arr = []
    for i in range(line_count):
        for j in range(line_count):
            arr.append(cosine_similarity(tfidf_arr[i], tfidf_arr[j]))

    return ArrUtil().nparr_to_dataframe(arr, line_count, line_count)

def semantic_similarity(line_count, line_word, model): #문서 내 각 행의 단어들끼리 의미적 유사도 비교
    arr = [[0]*line_count for _ in range(line_count)]
    # i행의 단어 n개* j행의 단어 m개 비교 -> ij간 단어a->단어b최대 유사도의 mean
    for i in range(line_count):
        size_a = len(line_word[i]) #A문장 단어 수
        for j in range(line_count):
            if i == j: continue #같은 문장일 경우 비교x
            sum_a = 0 #i<->j행의 단어들에 대한 최대 유사도 합
            for a in line_word[i]:
                sum_a += word_comparison(a, line_word[j], model)
            arr[i][j] = sum_a / size_a
            
    return ArrUtil().nparr_to_dataframe(arr, line_count, line_count)

def word_comparison(a, line_word_j, model): #i행의 단어 a, j행의 단어들에 대한 최대 유사도 합
    max_sim = -float('inf') #word_a와 가장 유사한 word_b와의 유사도

    for word_b in line_word_j: #j행의 단어 b
        try: 
            max_sim = max(max_sim, model.wv.similarity(a, word_b))
        except KeyError: max_sim = max(max_sim, 0)
    return max_sim