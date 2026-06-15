FROM python:3.12-alpine

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY core.py .

EXPOSE 8010
ENTRYPOINT ["python", "core.py"]
