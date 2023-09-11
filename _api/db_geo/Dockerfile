FROM postgres:latest

ENV DATABASE_HOST = 127.0.0.1
ENV POSTGRES_USER=root
ENV POSTGRES_PASSWORD=root
ENV POSTGRES_DB=bienestar_geo

RUN apt-get update && \
    apt-get install -y postgis
    
COPY geo_fixtures.sql /docker-entrypoint-initdb.d/geo_fixtures.sql

