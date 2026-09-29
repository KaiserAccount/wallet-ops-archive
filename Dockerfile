FROM python:3.11-slim
ARG CACHEBUST=20260929T1940Z
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY archive_to_r2.py .
CMD ["python", "-u", "archive_to_r2.py"]
