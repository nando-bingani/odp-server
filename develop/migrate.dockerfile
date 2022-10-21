FROM python:3.10

WORKDIR /srv
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY bin/migrate bin/
COPY migrate migrate/
COPY odp odp/

CMD ["bin/migrate"]
