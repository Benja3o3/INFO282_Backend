ARG BASE_IMG=alpine:3.18

FROM $BASE_IMG AS pidproxy

RUN apk --no-cache add alpine-sdk \
 && git clone https://github.com/ZentriaMC/pidproxy.git \
 && cd pidproxy \
 && git checkout 193e5080e3e9b733a59e25d8f7ec84aee374b9bb \
 && sed -i 's/-mtune=generic/-mtune=native/g' Makefile \
 && make \
 && mv pidproxy /usr/bin/pidproxy \
 && cd .. \
 && rm -rf pidproxy \
 && apk del alpine-sdk


FROM $BASE_IMG

COPY --from=pidproxy /usr/bin/pidproxy /usr/bin/pidproxy
RUN apk --no-cache add vsftpd tini

COPY vsftpd.conf /etc/vsftpd/vsftpd.conf

EXPOSE 21 21000-21010
VOLUME /ftp/ftp

RUN apk add --update py3-pip
WORKDIR /daemon/
COPY . /daemon/
COPY crontab /var/spool/cron/crontabs/root
RUN apk add build-base python3-dev

RUN pip3 install -r /daemon/requirements.txt
RUN chmod +x ./run.sh
RUN chmod +x ./vsftpd.sh
CMD ["./run.sh"]

