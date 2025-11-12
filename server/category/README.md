# Category Service (FastAPI)

Implements Category CRUD with validation and bidirectional sync to Product service.

## Run
```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8003
