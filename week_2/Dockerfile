FROM python:3.9

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install --upgrade pip && pip3 install -r requirements.txt

COPY ingest_flow.py ingest_flow.py

ENTRYPOINT [ "python", "ingest_flow.py" ]
