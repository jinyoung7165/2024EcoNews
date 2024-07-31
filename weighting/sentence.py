from sklearn.feature_extraction.text import TfidfVectorizer
from numpy import dot
from numpy.linalg import norm
from collections import defaultdict
from functools import lru_cache

import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from weighting.arr_util import ArrUtil
from preprocess.tokenizer import Tokenizer
from remote.mongo_method import MongoDB

@lru_cache(maxsize=3)
class Sentence(ArrUtil):
    def __init__(self, mongodb: MongoDB, tokenizer: Tokenizer, model, date):
        self.tokenizer = tokenizer
        self.model = model
        self.date = date
    
        self.docs = mongodb.get_all_docs(date)  # [[id, "첫번째 문서 두번째 문장"],  ...]

        self.docs_word_arr = defaultdict(list) #문서별 가진 단어 배열
        #문서별 문장 list 저장 # {0: ["첫번째", "문서", "두번째", "문장", "중복", "문장"]}
        
    @lru_cache(maxsize=3)
    def doc_process(self): # -> 모든 문서에 대해 문장유사도 df뽑을꺼 필ㅇ없는 문장 제거
        # 한 문서에서 문장 리스트 뽑음
        for idx, doc in self.docs: #idx:문서 번호 - 전처리 후 문장 0개면 pass할 거라 문서번호까지 df에 나타내자
            self.word_lines = [] # ["첫번째 문서", "두번째 문장"]
            self.line_word = [] #문장별 단어 배열 #[["첫번째", "문서"], ["두번째", "문장]]
            
            row = doc.split('.')
            self.preprocess(row)
            self.line_count = len(self.word_lines) #문장 수
            if not self.line_count: continue
            
            df1 = self.statistical_similarity(self.tfidf()) #통계적 유사도
            df2 = self.semantic_similarity() #의미적 유사도
            sum_df = df1.add(df2) #유사도 결합
            delete_count = int(self.line_count*0.14) if self.line_count*0.14 > 1 else 1 #제거할 줄 수
            delete_idx_arr = sum_df.sort_values(by=self.line_count, ascending=True).head(delete_count).index #제거할 줄의 idx
            for i in range(self.line_count):
                key = "{}_doc/{}".format(self.date, idx) #collection date, _idx
                if i not in delete_idx_arr:
                    self.docs_word_arr[key].extend(self.line_word[i])
                    
    def preprocess(self, row): #문서 내 각 열(row)의 문장(line) 형태소 분석 + 불용어 제거
        for line in row: #한 줄씩 처리 line:"앵커 어쩌고입니다"
            after_stopword = self.tokenizer.sentence_tokenizer(line)
            if after_stopword:
                self.line_word.append(after_stopword)
                self.word_lines.append(' '.join(after_stopword))

    def tfidf(self): #한 문서의 wordline에 대한 tfidf arr 리턴
        tfidf = TfidfVectorizer().fit(self.word_lines)
        return tfidf.transform(self.word_lines).toarray()

    def statistical_similarity(self, tfidf_arr): #문장 수, tfidf
        def cosine_similarity(sentence1, sentence2):
            norms = norm(sentence1) * norm(sentence2)
            if norms == 0: return 0
            return dot(sentence1, sentence2) / norms

        arr = []
        for i in range(self.line_count):
            for j in range(self.line_count):
                arr.append(cosine_similarity(tfidf_arr[i], tfidf_arr[j]))
 
        return self.nparr_to_dataframe(arr, self.line_count, self.line_count)

    def semantic_similarity(self): #문서 내 각 행의 단어들끼리 의미적 유사도 비교
        arr = [[0]*self.line_count for _ in range(self.line_count)]
        # i행의 단어 n개* j행의 단어 m개 비교 -> ij간 단어a->단어b최대 유사도의 mean
        for i in range(self.line_count):
            size_a = len(self.line_word[i]) #A문장 단어 수
            for j in range(self.line_count):
                if i == j: continue #같은 문장일 경우 비교x
                sum_a = 0 #i<->j행의 단어들에 대한 최대 유사도 합
                for word_a in self.line_word[i]: #i행의 단어 a
                    max_sim = -float('inf') #word_a와 가장 유사한 word_b와의 유사도
                    
                    for word_b in self.line_word[j]: #j행의 단어 b
                        try: 
                            max_sim = max(max_sim, self.model.wv.similarity(word_a, word_b))
                        except KeyError: max_sim = max(max_sim, 0)
                    sum_a += max_sim
                    
                arr[i][j] = sum_a / size_a
        
        return self.nparr_to_dataframe(arr, self.line_count, self.line_count)