from konlpy.tag import Okt

class Tokenizer:
    def __init__(self):
        file = open('stopword.txt', 'r', encoding = 'utf-8')
        self.stopwords = []
        for line in file.readlines():
            self.stopwords.append(line.strip())

    def sentence_tokenizer(self, text):
        tokenizer = Okt()
        return [
            token for token, pos in tokenizer.pos(text, norm=True, stem=True)
            if pos in ['Noun', 'Alpha'] and # 명사, 영어
            token not in self.stopwords # 불용어 제거 이중 for문 한 문장으로 처리
            and len(token)>1
        ]
        
class SummaryStopword:
    def __init__(self):
        file = open('summary.txt', 'r', encoding = 'utf-8')
        self.stopwords = []
        for line in file.readlines():
            self.stopwords.append(line.strip())