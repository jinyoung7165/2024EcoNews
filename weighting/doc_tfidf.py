from sklearn.feature_extraction.text import TfidfVectorizer
from collections import defaultdict, Counter
from functools import reduce

from weighting.arr_util import ArrUtil

class DocTfidf(ArrUtil): #전체 문서 기준
    def __init__(self, model, doc_word_dict: defaultdict):
        self.model = model
        self.doc_word_dict = doc_word_dict
        self.len_doc = len(self.doc_word_dict) # 문서 개수
        
    def final_word_process(self):
        df1 = self.statistical_similarity()
        df2 = self.semantic_similarity()
        sum_df = df1.add(df2)
        
        my_cols = self.word_list + ["total"]
        my_rows = list(self.doc_word_dict.keys())

        sum_df.columns = my_cols
        sum_df.index = my_rows
        
        return sum_df
    
    def statistical_similarity(self):
        tfidf_target_word = [] #각 문서의 모든 word로 이뤄진 문장 배열
        # 1차원 배열로 만들기(tf-idf를 위해서)
        for word in self.doc_word_dict.values():
            text = ' '.join(word)
            tfidf_target_word.append(text)
        #전체 문서의 wordline에 대한 tfidf arr 리턴
        tfidf = TfidfVectorizer(max_features=1000, min_df=5, max_df=0.5).fit(tfidf_target_word)
        self.word_list = sorted(list(tfidf.vocabulary_.keys())) #단어 set
        tfidf_arr = tfidf.transform(tfidf_target_word).toarray()
        
        self.len_word = len(self.word_list) # 단어 개수
        return self.nparr_to_dataframe(tfidf_arr, self.len_doc, self.len_word)
        
    def semantic_similarity(self):
        arr = [[0]*self.len_word for _ in range(self.len_doc)] #tfidf랑 결합할 의미 벡터
        for idx, words in enumerate(self.doc_word_dict.values()): #문서 순회
            doc_words = set(words) #해당 문서의 단어 집합
            for word in doc_words: #해당 문서의 단어
                sim_sum = 0 #해당 word에 대한 유사도 합
                if word in self.word_list: #해당 문서의 단어가 사전에 존재할 때
                    for key in self.word_list: #전체 단어 set
                        if word == key: continue
                        try:
                            sim_sum += self.model.wv.similarity(word, key)
                        except KeyError: continue

                    arr[idx][self.word_list.index(word)] = sim_sum / len(doc_words)
                    
        return self.nparr_to_dataframe(arr, self.len_doc, self.len_word)
    
    
    def hot_topic(self):
        all_words = list(reduce(lambda x,y: x+y, self.doc_word_dict.values()))
        result2=  Counter(list(word for word in all_words if word in self.word_list)).most_common(40)
        
        return result2