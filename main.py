
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
        art_out = _to_int_if_digits(art) if art else "НЕ ЗАВЕДЕН"
        ws.append([art_out, int(qty), ""])  # площадь пустая

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
  <title>Бауцентр — Конвертация PDF → Excel</title>
  <style>
    :root{
      --bg1:#0b1833;
      --bg2:#071126;
      --card:#ffffff;
      --text:#111827;
      --muted:#6b7280;
      --border:#e5e7eb;
      --blue:#3b82f6;
      --blue2:#2563eb;
      --shadow: 0 24px 70px rgba(0,0,0,.35);
    }
    html,body{height:100%; overflow:hidden;}
    *{box-sizing:border-box;}
    body{
      margin:0;
      font-family: system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
      background:
        radial-gradient(1100px 700px at 50% 18%, #153a77 0%, rgba(21,58,119,0) 60%),
        radial-gradient(900px 600px at 20% 20%, rgba(59,130,246,.30) 0%, rgba(59,130,246,0) 65%),
        linear-gradient(180deg, var(--bg1), var(--bg2));
      display:flex;
      align-items:center;
      justify-content:center;
      padding: 34px 18px;
      color: var(--text);
    }

    .top-pill{
      position: fixed;
      top: 28px;
      left: 50%;
      transform: translateX(-50%);
      background: rgba(255,255,255,.10);
      border: 1px solid rgba(255,255,255,.14);
      backdrop-filter: blur(10px);
      border-radius: 16px;
      padding: 10px 18px;
      display:flex;
      align-items:center;
      gap:10px;
      color:#fff;
      box-shadow: 0 10px 30px rgba(0,0,0,.25);
    }
    .top-pill img{ height: 22px; width:auto; display:block; }
    .top-pill .brand{ font-weight: 800; letter-spacing:.2px; }

    .card{
      width: min(760px, 100%);
      background: var(--card);
      border-radius: 18px;
      box-shadow: var(--shadow);
      padding: 34px 34px 28px;
      text-align:center;
    }
    .title{
      font-size: 22px;
      font-weight: 900;
      margin: 6px 0 6px;
    }
    .subtitle{
      margin: 0 0 18px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.35;
    }
    .subtitle b{ color:#374151; }

    .drop{
      border: 1.5px dashed var(--border);
      border-radius: 14px;
      padding: 22px 18px;
      background: #f8fafc;
    }
    .drop .mini{
      color: var(--muted);
      font-size: 12px;
      margin-top: 6px;
    }
    .icon{
      width: 34px;
      height: 34px;
      margin: 0 auto 10px;
      border-radius: 10px;
      border: 1px solid var(--border);
      display:flex;
      align-items:center;
      justify-content:center;
      background:#fff;
    }
    .icon svg{ width:18px; height:18px; }

    .controls{
      margin-top: 14px;
      display:flex;
      align-items:center;
      justify-content:center;
      gap: 10px;
      flex-wrap: wrap;
    }
    .btn{
      appearance:none;
      border: 1px solid var(--border);
      background: #fff;
      color:#111827;
      padding: 10px 16px;
      border-radius: 12px;
      font-weight: 800;
      cursor:pointer;
      box-shadow: 0 6px 18px rgba(17,24,39,.06);
    }
    .btn.primary{
      border-color: rgba(37,99,235,.25);
      background: linear-gradient(180deg, var(--blue), var(--blue2));
      color:#fff;
      box-shadow: 0 10px 22px rgba(37,99,235,.25);
    }
    .btn:disabled{
      opacity:.55;
      cursor:not-allowed;
      box-shadow:none;
    }

    .status{
      margin-top: 14px;
      font-size: 13px;
      color: var(--muted);
      white-space: pre-wrap;
    }
    .status.ok{ color:#0f766e; }
    .status.err{ color:#b91c1c; }
    input[type="file"]{ display:none; }
  </style>
</head>
<body>
  <div class="top-pill">
    <img src="/static/logo.png" alt="Бауцентр" />
    <div class="brand">Бауцентр</div>
  </div>

  <div class="card">
    <div class="title">Конвертация PDF → Excel</div>
    <div class="subtitle">
      Загрузите PDF (отчёт/корзина из 3D конфигуратора) — получите Excel (.xlsx).<br/>
      Формат: <b>Артикул</b> / <b>ШТ</b> / <b>Площадь</b> (Площадь пустая).
    </div>

    <div class="drop">
      <div class="icon" aria-hidden="true">
        <svg viewBox="0 0 24 24" fill="none">
          <path d="M12 3v10m0-10 4 4m-4-4-4 4" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M4 15v3a3 3 0 0 0 3 3h10a3 3 0 0 0 3-3v-3" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
        </svg>
      </div>

      <div class="mini" id="fileName">Перетащите PDF сюда или выберите файл на устройстве.</div>

      <div class="controls">
        <label class="btn" for="pdfInput">Выбрать PDF</label>
        <button class="btn primary" id="downloadBtn" disabled>Скачать Excel</button>
      </div>
      <input id="pdfInput" type="file" accept="application/pdf" />
    </div>

    <div class="status" id="status"></div>
  </div>

<script>
(function(){
  const input = document.getElementById('pdfInput');
  const status = document.getElementById('status');
  const fileName = document.getElementById('fileName');
  const downloadBtn = document.getElementById('downloadBtn');
  const drop = document.querySelector('.drop');

  let jobId = null;

  function setStatus(text, cls){
    status.className = 'status' + (cls ? (' ' + cls) : '');
    status.textContent = text || '';
  }

  async function poll(){
    if(!jobId) return;
    try{
      const r = await fetch('/job/' + jobId);
      const j = await r.json();
      if(j.status === 'done'){
        setStatus('Готово. Можно скачать Excel.', 'ok');
        downloadBtn.disabled = false;
        downloadBtn.onclick = () => { window.location.href = '/download/' + jobId; };
        return;
      }
      if(j.status === 'error'){
        setStatus(j.error || 'Ошибка обработки.', 'err');
        jobId = null;
        return;
      }
      setStatus(j.message || 'Обработка…');
      setTimeout(poll, 400);
    }catch(e){
      setStatus('Ошибка связи с сервером.', 'err');
    }
  }

  async function start(file){
    if(!file) return;
    downloadBtn.disabled = true;
    downloadBtn.onclick = null;
    setStatus('Загрузка…');
    fileName.textContent = file.name;

    const fd = new FormData();
    fd.append('file', file);

    const r = await fetch('/extract', { method:'POST', body: fd });
    if(!r.ok){
      const t = await r.text();
      setStatus(t || 'Ошибка загрузки.', 'err');
      return;
    }
    const j = await r.json();
    jobId = j.job_id;
    setStatus('Обработка…');
    poll();
  }

  input.addEventListener('change', (e)=> start(e.target.files && e.target.files[0]));

  // drag & drop
  ['dragenter','dragover'].forEach(evt => drop.addEventListener(evt, (e)=>{
    e.preventDefault(); e.stopPropagation();
    drop.style.background = '#eef2ff';
  }));
  ['dragleave','drop'].forEach(evt => drop.addEventListener(evt, (e)=>{
    e.preventDefault(); e.stopPropagation();
    drop.style.background = '#f8fafc';
  }));
  drop.addEventListener('drop', (e)=>{
    const f = e.dataTransfer.files && e.dataTransfer.files[0];
    if(f) start(f);
  });
})();
</script>
</body>
</html>
"""
