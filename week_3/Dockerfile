FROM python:3.9

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install --upgrade pip && pip3 install -r requirements.txt

COPY load_to_gcs.py load_to_gcs.py

ENTRYPOINT [ "python", "load_to_gcs.py" ]
