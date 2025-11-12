# Image Service (FastAPI)

Manages product images. Each image has:
- `id` (UUID, auto-generated if omitted)
- `url` (must be a valid URL)
- `product_id` (optional UUID; attach to a single product)

## Run locally
```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
uvicorn main:app --host 0.0.0.0 --port 8004
