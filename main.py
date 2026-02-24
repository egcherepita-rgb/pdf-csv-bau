import io
import os
import re
import time
import json
import threading
from uuid import uuid4
from collections import OrderedDict
from typing import List, Tuple, Dict, Optional

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response, HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

try:
    import openpyxl  # pip install openpyxl
except Exception:
    openpyxl = None


app = FastAPI(
    title="PDF → XLSX (Бауцентр)",
    version="1.0.0",
)

# Static files (logo, etc.)
app.mount("/static", StaticFiles(directory="static"), name="static")

# -------------------------
# Regex & parsing helpers (из рабочего main)
# -------------------------
RX_MONEY_LINE = re.compile(r"^\d+(?:[ \u00a0]\d{3})*(?:[.,]\d+)?\s*₽$")
RX_INT = re.compile(r"^\d+$")
RX_ANY_RUB = re.compile(r"₽")

# В одной строке: "... 450 ₽ 2 900 ₽"
RX_PRICE_QTY_SUM = re.compile(
    r"(?P<price>\d+(?:[ \u00a0]\d{3})*(?:[.,]\d+)?)\s*₽\s+"
    r"(?P<qty>\d{1,4})\s+"
    r"(?P<sum>\d+(?:[ \u00a0]\d{3})*(?:[.,]\d+)?)\s*₽",
    re.IGNORECASE,
)

# Размеры где угодно в строке: "30x10 мм", "600×300 мм" и т.п.
RX_DIMS_ANYWHERE = re.compile(
    r"\s*\d{1,4}[xх×]\d{1,4}(?:[xх×]\d{1,5})?\s*мм\.?\s*",
    re.IGNORECASE,
)

RX_WEIGHT = re.compile(r"\b\d+(?:[.,]\d+)?\s*кг\.?\b", re.IGNORECASE)


def normalize_space(s: str) -> str:
    s = (s or "").replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def normalize_key(name: str) -> str:
    """Ключ для маппинга справочника (устойчив к пробелам/размерам/х)."""
    s = normalize_space(name).lower()
    s = s.replace("×", "x").replace("х", "x")
    s = RX_DIMS_ANYWHERE.sub(" ", s)
    s = normalize_space(s)
    return s


def strip_dims_anywhere(name: str) -> str:
    name = normalize_space(name)
    name2 = RX_DIMS_ANYWHERE.sub(" ", name)
    return normalize_space(name2)


def is_noise(line: str) -> bool:
    """Строки, которые почти всегда не относятся к наименованию."""
    if not line:
        return True
    low = line.lower()

    # Похожие токены встречаются в шапках/подвалах
    bad = (
        "наименование", "кол-во", "количество", "цена", "сумма",
        "проект", "страница", "итого", "итог", "стоимость",
        "без ндс", "ндс", "руб", "₽",
    )
    # но '₽' мы не считаем шумом, иначе сломаем якоря
    if any(b in low for b in bad) and "₽" not in line:
        return True

    # чисто число (иногда номера строк/страниц)
    if RX_INT.fullmatch(line) and len(line) <= 3:
        return True

    return False


def is_header_token(line: str) -> bool:
    low = (line or "").lower()
    return low in {
        "наименование", "кол-во", "количество", "цена", "сумма",
        "артикул", "ед.", "ед", "шт", "шт.",
    }


def is_totals_block(line: str) -> bool:
    low = (line or "").lower()
    return ("итого" in low) or ("итог" == low)


def is_project_total_only(line: str, prev_line: str = "") -> bool:
    """
    Защита от строки вроде "Стоимость проекта: 89495 ₽",
    которую нельзя воспринимать как цену позиции (иначе парсер "съедет").
    """
    low = (line or "").lower()
    prev = (prev_line or "").lower()
    return ("стоимость проекта" in low) or ("стоимость проекта" in prev and "₽" in line)


def clean_name_from_buffer(buf: List[str]) -> str:
    """
    Превращает накопленный буфер строк в наименование позиции.
    Убирает размеры '... мм', вес, лишние пробелы.
    """
    if not buf:
        return ""
    s = " ".join([x for x in buf if x])
    s = normalize_space(s)

    # Убираем вес/размеры с 'мм' (в бау-режиме это важно для совпадения со справочником)
    s = RX_WEIGHT.sub(" ", s)
    s = strip_dims_anywhere(s)
    s = normalize_space(s)
    return s


# -------------------------
# Справочник артикулов (Art1.xlsx)
# -------------------------
def load_article_map() -> Tuple[Dict[str, str], str]:
    if openpyxl is None:
        return {}, "openpyxl_not_installed"

    path = os.getenv("ART_XLSX_PATH", "Art1.xlsx")
    if not os.path.exists(path):
        return {}, f"file_not_found:{path}"

    try:
        wb = openpyxl.load_workbook(path, data_only=True)
        ws = wb[wb.sheetnames[0]]
    except Exception as e:
        return {}, f"cannot_open:{e}"

    header = [normalize_space(ws.cell(1, c).value or "") for c in range(1, ws.max_column + 1)]
    товар_col = 1
    арт_col = 2
    for idx, h in enumerate(header, start=1):
        if h.lower() == "товар":
            товар_col = idx
        if h.lower() == "артикул":
            арт_col = idx

    m: Dict[str, str] = {}
    for r in range(2, ws.max_row + 1):
        товар = ws.cell(r, товар_col).value
        арт = ws.cell(r, арт_col).value
        if товар is None or арт is None:
            continue
        товар_s = normalize_space(str(товар))
        арт_s = normalize_space(str(арт))

        # В бау-выгрузке лучше оставлять пусто, чем "0"
        if not товар_s or not арт_s or арт_s == "0":
            continue

        m[normalize_key(товар_s)] = арт_s

    return m, "ok"


ARTICLE_MAP, ARTICLE_MAP_STATUS = load_article_map()

# -------------------------
# Парсер PDF → [(name, qty)]
# -------------------------
def parse_items(pdf_bytes: bytes) -> Tuple[List[Tuple[str, int]], Dict]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    total_pages = doc.page_count

    ordered: "OrderedDict[str, int]" = OrderedDict()
    buf: List[str] = []

    stats = {
        "pages": 0,
        "total_pages": total_pages,
        "processed_pages": 0,
        "items_found": 0,
        "anchors_inline": 0,
        "anchors_multiline": 0,
        "article_map_size": len(ARTICLE_MAP),
        "article_map_status": ARTICLE_MAP_STATUS,
    }

    for page in doc:
        stats["pages"] += 1
        stats["processed_pages"] += 1

        txt = page.get_text("text") or ""
        if "₽" not in txt:
            continue

        lines = [normalize_space(x) for x in txt.splitlines()]
        lines = [x for x in lines if x]
        if not lines:
            continue

        in_totals = False
        buf.clear()

        i = 0
        while i < len(lines):
            line = lines[i]
            prev = lines[i - 1] if i > 0 else ""

            if is_noise(line) or is_header_token(line):
                i += 1
                continue

            if is_project_total_only(line, prev_line=prev) or is_totals_block(line):
                in_totals = True
                buf.clear()
                i += 1
                continue

            if in_totals:
                i += 1
                continue

            # A) INLINE anchor: "... price ₽ qty sum ₽"
            m = RX_PRICE_QTY_SUM.search(line)
            if m:
                name = clean_name_from_buffer(buf)
                buf.clear()

                if name:
                    try:
                        qty = int(m.group("qty"))
                    except Exception:
                        qty = 0

                    if 1 <= qty <= 500:
                        ordered[name] = ordered.get(name, 0) + qty
                        stats["items_found"] += 1
                        stats["anchors_inline"] += 1

                i += 1
                continue

            # C) EMBEDDED anchor: строка с ₽, затем qty, затем sum ₽
            if RX_ANY_RUB.search(line):
                if i + 2 < len(lines) and RX_INT.fullmatch(lines[i + 1]) and RX_MONEY_LINE.fullmatch(lines[i + 2]):
                    try:
                        qty = int(lines[i + 1])
                    except Exception:
                        qty = 0

                    if 1 <= qty <= 500:
                        name = clean_name_from_buffer(buf + [line])
                        buf.clear()

                        if name:
                            ordered[name] = ordered.get(name, 0) + qty
                            stats["items_found"] += 1
                            stats["anchors_multiline"] += 1

                    i += 3
                    continue

            # B) MULTILINE anchor: price ₽ ... qty ... sum ₽
            if RX_MONEY_LINE.fullmatch(line):
                end = min(len(lines), i + 8)

                qty_idx = None
                for j in range(i + 1, end):
                    if RX_INT.fullmatch(lines[j]):
                        q = int(lines[j])
                        if 1 <= q <= 500:
                            qty_idx = j
                            break

                if qty_idx is None:
                    buf.append(line)
                    i += 1
                    continue

                sum_idx = None
                for j in range(qty_idx + 1, end):
                    if RX_MONEY_LINE.fullmatch(lines[j]):
                        sum_idx = j
                        break

                if sum_idx is None:
                    buf.append(line)
                    i += 1
                    continue

                name = clean_name_from_buffer(buf)
                buf.clear()

                if name:
                    qty = int(lines[qty_idx])
                    ordered[name] = ordered.get(name, 0) + qty
                    stats["items_found"] += 1
                    stats["anchors_multiline"] += 1

                i = sum_idx + 1
                continue

            buf.append(line)
            i += 1

    return list(ordered.items()), stats


# -------------------------
# XLSX output (Бауцентр): Артикул | ШТ | Площадь(пусто)
# -------------------------
def _to_int_if_digits(v: str):
    s = normalize_space(v)
    if s.isdigit():
        try:
            return int(s)
        except Exception:
            return s
    return s


def make_xlsx_bau(rows: List[Tuple[str, int]]) -> bytes:
    if openpyxl is None:
        raise RuntimeError("openpyxl не установлен (pip install openpyxl)")

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "items"

    ws.append(["Артикул", "ШТ", "Площадь"])

    for name, qty in rows:
        art = ARTICLE_MAP.get(normalize_key(name), "")
        ws.append([_to_int_if_digits(art) if art else "", int(qty), ""])  # площадь пустая

    # Немного удобства: ширина колонок
    ws.column_dimensions["A"].width = 16
    ws.column_dimensions["B"].width = 8
    ws.column_dimensions["C"].width = 12

    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()


# -------------------------
# Async jobs (как в рабочем main)
# -------------------------
_JOBS: Dict[str, Dict] = {}
_JOBS_LOCK = threading.Lock()


def _set_job(job_id: str, **kwargs):
    with _JOBS_LOCK:
        job = _JOBS.get(job_id, {})
        job.update(kwargs)
        _JOBS[job_id] = job


def _set_job_result(job_id: str, data: bytes):
    with _JOBS_LOCK:
        job = _JOBS.get(job_id, {})
        job["result"] = data
        _JOBS[job_id] = job


def _get_job(job_id: str) -> Optional[Dict]:
    with _JOBS_LOCK:
        return _JOBS.get(job_id)


# -------------------------
# API
# -------------------------
@app.get("/", response_class=HTMLResponse)
def home():
    return HTMLResponse(HOME_HTML)


@app.post("/extract")
async def extract(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Загрузите PDF файл (.pdf).")

    pdf_bytes = await file.read()

    try:
        rows, stats_ = parse_items(pdf_bytes)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Не удалось распарсить PDF: {e}")

    if not rows:
        raise HTTPException(
            status_code=422,
            detail=("Не удалось найти позиции. debug=" + json.dumps(stats_, ensure_ascii=False)),
        )

    xlsx_bytes = make_xlsx_bau(rows)
    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="bau.xlsx"'},
    )


@app.post("/extract_async")
async def extract_async(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Загрузите PDF файл (.pdf).")

    pdf_bytes = await file.read()
    original_filename = file.filename or "items.pdf"

    job_id = str(uuid4())
    _set_job(
        job_id,
        created_at=time.time(),
        status="processing",
        message="Старт обработки",
        processed_pages=0,
        total_pages=0,
        filename=(os.path.splitext(original_filename)[0] or "items") + ".xlsx",
    )

    def worker():
        try:
            _set_job(job_id, message="Читаю PDF…")
            rows, st = parse_items(pdf_bytes)

            _set_job(
                job_id,
                processed_pages=int(st.get("processed_pages", 0) or 0),
                total_pages=int(st.get("total_pages", 0) or 0),
                message="Формирую XLSX…",
            )

            if not rows:
                _set_job(job_id, status="error", message="Не удалось найти позиции", stats=st)
                return

            xlsx_bytes = make_xlsx_bau(rows)
            _set_job_result(job_id, xlsx_bytes)

            _set_job(
                job_id,
                status="done",
                message="Готово",
                stats=st,
            )
        except Exception as e:
            _set_job(job_id, status="error", message=f"Ошибка: {e}")

    t = threading.Thread(target=worker, daemon=True)
    t.start()

    return JSONResponse({"job_id": job_id})


@app.get("/job/{job_id}")
def job_status(job_id: str):
    job = _get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    # result не отдаём тут, только статус
    safe = {k: v for k, v in job.items() if k != "result"}
    return JSONResponse(safe)


@app.get("/download/{job_id}")
def download(job_id: str):
    job = _get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    if job.get("status") != "done":
        raise HTTPException(status_code=409, detail="job not done")

    data = job.get("result")
    if not data:
        raise HTTPException(status_code=500, detail="no result")

    filename = job.get("filename") or "bau.xlsx"
    return Response(
        content=data,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/health")
def health():
    return {
        "ok": True,
        "article_map_status": ARTICLE_MAP_STATUS,
        "article_map_size": len(ARTICLE_MAP),
    }


# -------------------------
# UI (centered, no scroll) + logo /static/logo.png
# -------------------------
HOME_HTML = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>PDF → XLSX (Бауцентр)</title>
  <style>
    :root { --bg:#0b0f17; --card:#121a2a; --text:#e9eefc; --muted:#a8b3d6; --border:rgba(255,255,255,.08); --btn:#4f7cff; }
    html, body { height: 100%; overflow: hidden; }
    *, *::before, *::after { box-sizing: border-box; }
    body {
      margin:0;
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      background: radial-gradient(1200px 600px at 20% 10%, #18234a 0%, var(--bg) 55%);
      color: var(--text);
    }
    .hero {
      position: fixed;
      top: 22px;
      left: 0;
      width: 100%;
      text-align: center;
      z-index: 5;
      pointer-events: none;
    }
    .hero-title {
      font-weight: 900;
      letter-spacing: .6px;
      font-size: clamp(24px, 3.5vw, 44px);
      margin: 0;
    }
    .hero-logo {
      margin-top: 12px;
      height: 56px;
      width: auto;
      opacity: .95;
    }
    .wrap {
      height: 100vh;
      display:flex;
      align-items:center;
      justify-content:center;
      padding: 28px;
      padding-top: 170px;
    }
    .card {
      width:min(900px, 100%);
      background: rgba(18,26,42,.92);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 22px;
      box-shadow: 0 18px 60px rgba(0,0,0,.45);
    }
    h1 { margin:0; font-size: 22px; letter-spacing: .2px; }
    .hint { margin: 8px 0 0; color: var(--muted); font-size: 14px; }
    .row {
      margin-top: 18px;
      display:flex;
      gap: 12px;
      align-items:center;
      justify-content:center;
      flex-wrap:wrap;
      width: 100%;
    }
    .file {
      display:flex;
      align-items:center;
      justify-content:center;
      gap:10px;
      padding: 10px 12px;
      border: 1px dashed var(--border);
      border-radius: 14px;
      background: rgba(255,255,255,.02);
    }
    button {
      padding: 10px 14px;
      border: 0;
      border-radius: 14px;
      cursor: pointer;
      font-weight: 800;
      background: var(--btn);
      color: #0b1020;
    }
    button:disabled { opacity: .55; cursor:not-allowed; }
    .status { margin-top: 14px; font-size: 14px; color: var(--muted); white-space: pre-wrap; text-align:center; }
    .status.ok { color: #79ffa8; }
    .status.err { color: #ff7b8a; }
    .tiny { margin-top: 12px; text-align:center; color: var(--muted); font-size: 12px; }
  </style>
</head>
<body>
  <div class="hero">
    <h1 class="hero-title">PDF → XLSX (Бауцентр)</h1>
    <img class="hero-logo" src="/static/logo.png" alt="logo" />
  </div>

  <div class="wrap">
    <div class="card">
      <h1>Загрузите PDF со спецификацией</h1>
      <div class="hint">На выходе: XLSX с колонками <b>Артикул</b>, <b>ШТ</b>, <b>Площадь</b> (пустая).</div>

      <div class="row">
        <div class="file">
          <input id="file" type="file" accept="application/pdf" />
        </div>
        <button id="btn" disabled>Конвертировать</button>
      </div>

      <div id="status" class="status"></div>
      <div class="tiny">Если сервис не видит позиции — проверьте, что в PDF есть цены/кол-во/сумма (₽).</div>
    </div>
  </div>

<script>
const fileEl = document.getElementById('file');
const btn = document.getElementById('btn');
const statusEl = document.getElementById('status');

fileEl.addEventListener('change', () => {
  btn.disabled = !fileEl.files || fileEl.files.length === 0;
});

function setStatus(text, cls='') {
  statusEl.className = 'status ' + cls;
  statusEl.textContent = text;
}

async function poll(jobId) {
  while (true) {
    const r = await fetch('/job/' + jobId);
    const j = await r.json();
    if (j.status === 'done') {
      setStatus('Готово. Скачиваю файл…', 'ok');
      window.location = '/download/' + jobId;
      return;
    }
    if (j.status === 'error') {
      setStatus('Ошибка: ' + (j.message || 'неизвестно'), 'err');
      return;
    }
    const p = j.processed_pages || 0;
    const t = j.total_pages || 0;
    setStatus((j.message || 'Обработка…') + (t ? ` (${p}/${t})` : ''), '');
    await new Promise(res => setTimeout(res, 400));
  }
}

btn.addEventListener('click', async () => {
  if (!fileEl.files || fileEl.files.length === 0) return;
  btn.disabled = true;
  setStatus('Загрузка…');

  const fd = new FormData();
  fd.append('file', fileEl.files[0]);

  const r = await fetch('/extract_async', { method: 'POST', body: fd });
  if (!r.ok) {
    const txt = await r.text();
    setStatus('Ошибка: ' + txt, 'err');
    btn.disabled = false;
    return;
  }
  const j = await r.json();
  setStatus('Обработка…');
  await poll(j.job_id);
  btn.disabled = false;
});
</script>
</body>
</html>
"""
