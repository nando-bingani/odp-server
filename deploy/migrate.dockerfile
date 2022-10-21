FROM python:3.10

ARG CA_CERT_URL
RUN if [ -n "${CA_CERT_URL}" ]; then curl ${CA_CERT_URL} -k -o /usr/local/share/ca-certificates/saeon-ca.crt; fi
RUN if [ -n "${CA_CERT_URL}" ]; then update-ca-certificates; fi

WORKDIR /srv
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY bin/migrate bin/
COPY migrate migrate/
COPY odp odp/

CMD ["bin/migrate"]
