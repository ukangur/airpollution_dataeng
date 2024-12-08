FROM python:3.12.7-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir streamlit duckdb watchdog matplotlib numpy

COPY . /app

EXPOSE 8501