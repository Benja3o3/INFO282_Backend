FROM alpine:3.18

RUN apk add --update py3-pip
WORKDIR /daemon/
COPY . /daemon/
COPY crontab /var/spool/cron/crontabs/root

RUN pip3 install -r /daemon/requirements.txt
CMD ["crond",  "-f"]

