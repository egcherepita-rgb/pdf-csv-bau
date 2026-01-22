import os
import io
import re
import csv
import json
from typing import Dict, Tuple, List

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse

try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

try:
    import openpyxl
except Exception:
    openpyxl = None


APP_VERSION = "3.1-bau"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ART_XLSX_PATH = os.getenv("ART_XLSX_PATH", os.path.join(BASE_DIR, "Art1.xlsx"))
ART_VALUE_COLUMN = os.getenv("ART_VALUE_COLUMN", "Артикул")
ART_NAME_COLUMN = os.getenv("ART_NAME_COLUMN", "Товар")

COUNTER_FILE = os.getenv("COUNTER_FILE", os.path.join(BASE_DIR, "conversions.count"))

CATEGORY_VALUE = int(os.getenv("CATEGORY_VALUE", "2"))

app = FastAPI(title="Bau PDF → CSV", version=APP_VERSION)


# ------------------ helpers ------------------

def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def normalize_key(s: str) -> str:
    return normalize_space(s).lower()


def _norm_header(s: str) -> str:
    return normalize_space(str(s or "")).lower()


# ------------------ excel mapping ------------------

def load_article_map() -> Tuple[Dict[str, str], str, str, str]:
    if openpyxl is None:
        return {}, "openpyxl_not_installed", "", ""

    if not os.path.exists(ART_XLSX_PATH):
        return {}, f"file_not_found:{ART_XLSX_PATH}", "", ""

    try:
        wb = openpyxl.load_workbook(ART_XLSX_PATH, data_only=True)
        ws = wb[wb.sheetnames[0]]
    except Exception as e:
        return {}, f"cannot_open:{e}", "", ""

    headers_raw = [normalize_space(ws.cell(1, c).value or "") for c in range(1, ws.max_column + 1)]
    headers_norm = [_norm_header(h) for h in headers_raw]

    # --- name column ---
    name_candidates = [
        ART_NAME_COLUMN,
        "товар",
        "наименование",
        "название",
    ]
    name_norm = [_norm_header(x) for x in name_candidates]

    name_col = None
    used_name = ""
    for i, hn in enumerate(headers_norm, start=1):
        if hn in name_norm:
            name_col = i
            used_name = headers_raw[i - 1]
            break

    if name_col is None:
        name_col = 1
        used_name = headers_raw[0] if headers_raw else ""

    # --- value column ---
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
    used_val = ""
    for i, hn in enumerate(headers_norm, start=1):
        if hn in value_norm:
            val_col = i
            used_val = headers_raw[i - 1]
            break

    if val_col is None:
        val_col = 2 if ws.max_column >= 2 else 1
        used_val = headers_raw[val_col - 1] if headers_raw else ""

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

    return mapping, "ok", used_name, used_val


ARTICLE_MAP, ARTICLE_MAP_STATUS, ARTICLE_NAME_COLUMN_USED, ARTICLE_VALUE_COLUMN_USED = load_article_map()


# ------------------ counter ------------------

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


# ------------------ pdf parse ------------------

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


# ------------------ api ------------------

@app.get("/health")
def health():
    return {
        "status": "ok",
        "version": APP_VERSION,
        "article_map_size": len(ARTICLE_MAP),
        "article_map_status": ARTICLE_MAP_STATUS,
        "article_value_column": ARTICLE_VALUE_COLUMN_USED,
        "article_name_column": ARTICLE_NAME_COLUMN_USED,
        "category_value": CATEGORY_VALUE,
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
        "Категория",
    ])

    for name, qty in rows:
        key = normalize_key(name)
        article = ARTICLE_MAP.get(key, "")

        writer.writerow([
            name,
            article,
            qty,
            CATEGORY_VALUE,
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
