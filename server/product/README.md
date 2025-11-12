# Product Service (FastAPI)

## Run locally
```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env   # or copy in Windows
uvicorn main:app --host 0.0.0.0 --port 8002
