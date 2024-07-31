FROM python:3

# apt init
ENV LANG=C.UTF-8
ENV TZ=Asia/Seoul
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends tzdata g++ git curl

# install java
ENV JAVA_HOME="/usr/lib/jvm/java-1.8-openjdk/jre"
RUN apt-get install -y g++ default-jdk

# apt cleanse
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# timezone
RUN ln -sf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

VOLUME /tmp

# make workspace for konlpy
RUN mkdir -p /workspace
WORKDIR /workspace

# install konlpy dependencies: jpype, konlpy, with mecab module
RUN pip install jpype1-py3 konlpy
RUN cd /workspace && \
    curl -s https://raw.githubusercontent.com/konlpy/konlpy/master/scripts/mecab.sh | bash -s

# app 디렉토리 생성
RUN mkdir -p /app

#RUN, CMD, ENTRYPOINT의 명령이 실행될 디렉터리
WORKDIR /app

COPY .env ./
COPY remote ./remote
COPY preprocess ./preprocess
COPY summary ./summary

COPY requirements.txt  ./
RUN pip install -r requirements.txt

COPY weighting/sentence.py ./weighting/
COPY weighting/arr_util.py ./weighting/
COPY weighting/doc_tfidf.py ./weighting/

COPY model_fast ./
COPY model_fast.wv.vectors_ngrams.npy ./
COPY summary.txt ./
COPY stopword.txt ./
COPY run.py ./

CMD ["python3", "run.py"]