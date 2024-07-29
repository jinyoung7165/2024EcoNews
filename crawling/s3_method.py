import boto3, os
import pandas
import io

class S3: # 객체 = S3()
    def __init__(self):
        #객체.s3
        self.s3 = self.s3_connection()
        
    def s3_connection(self):
        try:
            # s3 클라이언트 생성
            s3 = boto3.client(
                service_name = "s3",
                region_name = os.environ.get('aws_region_name'),
                aws_access_key_id = os.environ.get('aws_access_key_id'),
                aws_secret_access_key = os.environ.get('aws_secret_access_key'),
            )
        except Exception as e:
            print(e)
        else:
            print("s3 bucket connected!") 
            return s3
    
    # S3로 파일 업로드 함수
    def s3_upload_file(self, now_date, filename):
        try:
            self.s3.upload_file(filename, #local 파일이름
                        os.environ.get('s3_bucket_name'), #버킷 이름
                        "data/{}/{}".format(now_date, filename)) #저장될 이름
        except Exception as e:
            print(e)
            
    # S3로 파일 다운로드 함수
    def s3_download_file(self, now_date, filename):
        try:
            obj = self.s3.get_object(Bucket=os.environ.get('s3_bucket_name'), 
                                    Key="data/{}/{}".format(now_date, filename))
            df = pandas.read_csv(io.BytesIO(obj['Body'].read()), encoding="utf-8-sig")
            # print(df)
        except Exception as e:
            print(e)
        else:
            print("s3 file download success!") 
            return df
