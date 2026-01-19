from fastapi import FastAPI

app = FastAPI(title="PDF → CSV BAU")

@app.get("/health")
def health():
    return {"status": "ok", "service": "bau"}
