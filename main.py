from fastapi import FastAPI

app = FastAPI(title="PDF → CSV BAU")

@app.get("/health")
def health():
    return {"status": "ok", "service": "bau"}

from fastapi.responses import RedirectResponse

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")
