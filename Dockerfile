FROM python:3.9.11-alpine

WORKDIR /app

ADD . /app

RUN pip install -r requirements.txt

EXPOSE 8050

ENTRYPOINT  ["python3"]

CMD ["main.py"]