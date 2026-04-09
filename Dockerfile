# syntax=docker/dockerfile:1
FROM postgres:18
COPY schema.sql /docker-entrypoint-initdb.d/
