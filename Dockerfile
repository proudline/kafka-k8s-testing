FROM apache/spark-py

USER 0
ENV BOOTSTRAP_SERVER="kafka:9093"

WORKDIR /usr/app
ADD requirements.txt .
ADD gcs-connector-hadoop3-latest.jar .

RUN pip install -r requirements.txt
ADD secret.py .
ADD consumer.py .

CMD ["python3","consumer.py"]