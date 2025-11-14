FROM python:3.11-slim
WORKDIR /app

# Install deps
COPY publisher/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY common ./common
#COPY events ./events
COPY processors ./processors
COPY publisher/main.py ./main.py

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
CMD ["python", "main.py"]
