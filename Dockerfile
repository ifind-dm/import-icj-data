FROM python:3.9-slim

RUN apt-get update && apt-get install --no-install-recommends -y curl build-essential git

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt


COPY src/ ./

EXPOSE 8082

CMD ["python", "main.py"]
