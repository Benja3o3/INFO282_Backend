FROM ubuntu:bionic
RUN apt-get update && apt-get install -y vsftpd

COPY vsftpd.conf /etc/vsftpd/vsftpd.conf

EXPOSE 20 21

CMD ["/usr/sbin/vsftpd"]
