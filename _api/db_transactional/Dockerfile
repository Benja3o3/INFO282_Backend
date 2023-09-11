FROM postgres:latest

ENV DATABASE_HOST = 127.0.0.1
ENV POSTGRES_USER=root
ENV POSTGRES_PASSWORD=root
ENV POSTGRES_DB=bienestar_transactional

RUN apt-get update
    
COPY transactional_fixtures.sql /docker-entrypoint-initdb.d/transactional_fixtures.sql