# Supplier Service (FastAPI)

Implements Supplier CRUD with validations and bidirectional sync to Product service.

## Run locally
```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
uvicorn main:app --host 0.0.0.0 --port 8001
