# base image
FROM public.ecr.aws/lambda/python as base

# Set the working directory
WORKDIR /var/task

COPY .env ./
COPY remote/ ./remote

# install dependencies
COPY crawling/requirements.txt  ./
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# application code
COPY crawling/app.py ./
COPY crawling/crawl.py ./

CMD [ "app.handler" ]