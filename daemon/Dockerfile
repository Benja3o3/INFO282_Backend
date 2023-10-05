FROM alpine:latest
RUN apk add --update py3-pip

WORKDIR /daemon/
COPY . /daemon/
COPY crontab /var/spool/cron/crontabs/root


RUN pip3 install -r /daemon/requirements.txt

RUN chmod 0655 /daemon/crontest.py

