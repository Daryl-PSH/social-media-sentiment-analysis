FROM python:3.8.5

RUN apt-get update

WORKDIR social_media

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY src/ src/

CMD ["python", "src/data_extractor/twitter/tweet_producer.py"]

