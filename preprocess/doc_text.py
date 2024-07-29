class DocToText:
    def __init__(self, s3):
        self.s3 = s3

    def csv_to_text(self, date, filename): #본문 한번에 한 줄 string으로
        self.s3_file = self.s3.s3_download_file(date, filename) # 작은 따옴표..
        self.main = self.s3_file['본문'].str.replace("[^A-Z a-z 0-9 가-힣 .]", "", regex=True)
        self.title = self.s3_file['제목'].str.replace("[^A-Z a-z 0-9 가-힣 .]", "", regex=True)