FROM python:3.11-slim
WORKDIR /app

COPY processors/supplier/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY processors/supplier/main.py ./main.py

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
CMD ["python", "main.py"]
