import os
import io
import re
import csv
from typing import Dict, Tuple, List

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

try:
    import openpyxl
except Exception:
    openpyxl = None


# ================= CONFIG =================

APP_VERSION = "3.2-bau-final"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ART_XLSX_PATH = os.getenv("ART_XLSX_PATH", os.path.join(BASE_DIR, "Art1.xlsx"))
ART_VALUE_COLUMN = os.getenv("ART_VALUE_COLUMN", "Артикул")
ART_NAME_COLUMN = os.getenv("ART_NAME_COLUMN", "Товар")

COUNTER_FILE = os.getenv("COUNTER_FILE", os.path.join(BASE_DIR, "conversions.count"))

# ================= APP =================

app = FastAPI(title="Bau PDF → CSV", version=APP_VERSION)

STATIC_DIR = os.path.join(BASE_DIR, "static")
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# ================= HELPERS =================


def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def normalize_key(s: str) -> str:
    return normalize_space(s).lower()


def _norm_header(s: str) -> str:
    return normalize_space(str(s or "")).lower()


# ================= EXCEL MAP =================


def load_article_map() -> Tuple[Dict[str, str], str]:
    if openpyxl is None:
        return {}, "openpyxl_not_installed"

    if not os.path.exists(ART_XLSX_PATH):
        return {}, f"file_not_found:{ART_XLSX_PATH}"

    try:
        wb = openpyxl.load_workbook(ART_XLSX_PATH, data_only=True)
        ws = wb[wb.sheetnames[0]]
    except Exception as e:
        return {}, f"cannot_open:{e}"

    headers_raw = [
        normalize_space(ws.cell(1, c).value or "")
        for c in range(1, ws.max_column + 1)
    ]
    headers_norm = [_norm_header(h) for h in headers_raw]

    # ---- name column ----
    name_candidates = [
        ART_NAME_COLUMN,
        "товар",
        "наименование",
        "название",
    ]
    name_norm = [_norm_header(x) for x in name_candidates]

    name_col = None
    for i, hn in enumerate(headers_norm, start=1):
        if hn in name_norm:
            name_col = i
            break

    if name_col is None:
        name_col = 1

    # ---- value column ----
    value_candidates = [
        ART_VALUE_COLUMN,
        "BAUID",
        "Кастомный ID",
        "КастомныйID",
        "Custom ID",
        "CustomID",
        "ID",
        "Артикул",
    ]
    value_norm = [_norm_header(x) for x in value_candidates]

    val_col = None
    for i, hn in enumerate(headers_norm, start=1):
        if hn in value_norm:
            val_col = i
            break

    if val_col is None:
        val_col = 2 if ws.max_column >= 2 else 1

    mapping: Dict[str, str] = {}

    for r in range(2, ws.max_row + 1):
        name = ws.cell(r, name_col).value
        val = ws.cell(r, val_col).value

        if not name:
            continue

        name_s = normalize_space(str(name))
        if not name_s:
            continue

        if val is None:
            continue

        if isinstance(val, float) and val.is_integer():
            val_s = str(int(val))
        else:
            val_s = normalize_space(str(val))

        if val_s in ("", "0", "0.0"):
            continue

        mapping[normalize_key(name_s)] = val_s

    return mapping, "ok"


ARTICLE_MAP, ARTICLE_MAP_STATUS = load_article_map()

# ================= COUNTER =================


def inc_counter() -> int:
    try:
        if not os.path.exists(COUNTER_FILE):
            with open(COUNTER_FILE, "w") as f:
                f.write("0")

        with open(COUNTER_FILE, "r+") as f:
            val = int(f.read().strip() or "0")
            val += 1
            f.seek(0)
            f.write(str(val))
            f.truncate()
            return val
    except Exception:
        return -1


def get_counter() -> int:
    try:
        if not os.path.exists(COUNTER_FILE):
            return 0
        with open(COUNTER_FILE) as f:
            return int(f.read().strip() or "0")
    except Exception:
        return 0


# ================= PDF PARSER =================

LINE_RE = re.compile(r"(.+?)\s+(\d+)\s*$")


def parse_pdf_items(data: bytes) -> List[Tuple[str, int]]:
    if fitz is None:
        raise RuntimeError("PyMuPDF not installed")

    doc = fitz.open(stream=data, filetype="pdf")
    items: List[Tuple[str, int]] = []

    for page in doc:
        text = page.get_text("text")
        for line in text.splitlines():
            line = normalize_space(line)
            m = LINE_RE.match(line)
            if not m:
                continue
            name = m.group(1)
            qty = int(m.group(2))
            items.append((name, qty))

    return items


# ================= UI =================


@app.get("/", response_class=HTMLResponse)
def index():
    return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<title>Bau PDF → CSV</title>
<style>
body {{
    font-family: Arial, sans-serif;
    background:#f5f7fa;
    margin:0;
}}

.header {{
    background:#003a70;
    padding:16px 32px;
    display:flex;
    align-items:center;
}}

.header img {{
    height:48px;
    margin-right:16px;
}}

.header h1 {{
    color:white;
    font-size:22px;
    margin:0;
}}

.container {{
    max-width:800px;
    margin:40px auto;
    background:white;
    padding:32px;
    border-radius:8px;
    box-shadow:0 2px 10px rgba(0,0,0,.08);
}}

.footer {{
    text-align:center;
    margin-top:24px;
    color:#666;
    font-size:13px;
}}
</style>
</head>
<body>

<div class="header">
    <img src="/static/logo.png">
    <h1>Конвертер PDF → CSV для Бауцентра</h1>
</div>

<div class="container">
    <h2>Загрузите PDF</h2>

    <form action="/convert" method="post" enctype="multipart/form-data">
        <input type="file" name="file" required>
        <br><br>
        <button type="submit">Конвертировать</button>
    </form>

    <div class="footer">
        Версия {APP_VERSION} · Конвертаций: {get_counter()}
    </div>
</div>

</body>
</html>
"""


# ================= API =================


@app.get("/health")
def health():
    return {
        "status": "ok",
        "version": APP_VERSION,
        "article_map_size": len(ARTICLE_MAP),
        "article_map_status": ARTICLE_MAP_STATUS,
        "conversions": get_counter(),
        "art_xlsx_path": ART_XLSX_PATH,
    }


@app.post("/convert")
async def convert(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Upload PDF file")

    data = await file.read()

    try:
        rows = parse_pdf_items(data)
    except Exception as e:
        raise HTTPException(500, f"PDF parse error: {e}")

    out = io.StringIO()
    writer = csv.writer(out, delimiter=";")

    writer.writerow([
        "Наименование",
        "Артикул",
        "Количество",
    ])

    for name, qty in rows:
        key = normalize_key(name)
        article = ARTICLE_MAP.get(key, "")

        writer.writerow([
            name,
            article,
            qty,
        ])

    count = inc_counter()

    headers = {
        "X-Conversions": str(count),
        "Content-Disposition": f'attachment; filename="{os.path.splitext(file.filename)[0]}.csv"',
    }

    return StreamingResponse(
        iter([out.getvalue()]),
        media_type="text/csv",
        headers=headers,
    )
