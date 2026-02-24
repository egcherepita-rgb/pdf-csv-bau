# -*- coding: utf-8 -*-
"""
BAU (Бауцентр) PDF -> XLSX converter
Output XLSX columns: Артикул | ШТ | Площадь (Площадь пустая)
If article not found in Art1.xlsx -> "НЕ ЗАВЕДЕН"

Dependencies (requirements.txt):
fastapi, uvicorn, gunicorn, openpyxl, PyMuPDF, python-multipart
"""

from __future__ import annotations

import io
import os
import re
import uuid
import time
import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

import openpyxl


APP_TITLE = "Бауцентр — Конвертация PDF"
ART_XLSX_PATH = os.environ.get("ART_XLSX_PATH", "Art1.xlsx")

# -----------------------------
# In-memory job storage
# -----------------------------

@dataclass
class Job:
    status: str  # "pending" | "done" | "error"
    created_at: float
    filename: str = "bau.xlsx"
    data: Optional[bytes] = None
    error: Optional[str] = None


JOBS: Dict[str, Job] = {}
JOBS_LOCK = threading.Lock()
JOB_TTL_SECONDS = 60 * 30  # 30 minutes


def _gc_jobs() -> None:
    now = time.time()
    with JOBS_LOCK:
        dead = [jid for jid, j in JOBS.items() if now - j.created_at > JOB_TTL_SECONDS]
        for jid in dead:
            JOBS.pop(jid, None)


# -----------------------------
# Normalization / mapping
# -----------------------------

_ws_re = re.compile(r"\s+")
_mm_dim_re = re.compile(r"\b\d{2,4}\s*мм\b", re.IGNORECASE)  # remove only explicit мм sizes
_price_re = re.compile(r"(\d[\d\s]*)(?:[.,]\d+)?\s*₽")  # "1 234 ₽"
_qty_sum_anchor_re = re.compile(r"^\s*(\d[\d\s]*)(?:[.,]\d+)?\s*₽\s+(\d+)\s+(\d[\d\s]*)(?:[.,]\d+)?\s*₽\s*$")
_project_total_re = re.compile(r"^\s*Стоимость\s+проекта\s*:\s*\d", re.IGNORECASE)


def normalize_name(s: str) -> str:
    """Normalize for matching to Art1.xlsx. Keep '60х40' etc; remove only 'мм' dimensions."""
    s = s.replace("\u00A0", " ").replace("×", "х").replace("x", "х")
    s = _mm_dim_re.sub("", s)
    s = s.replace("—", "-").replace("–", "-")
    s = _ws_re.sub(" ", s).strip()
    return s.lower()


def money_to_int(s: str) -> int:
    s = s.replace("\u00A0", " ").replace("₽", "").strip()
    s = s.replace(" ", "")
    # ignore decimals
    if "," in s:
        s = s.split(",")[0]
    if "." in s:
        s = s.split(".")[0]
    return int(s) if s.isdigit() else 0


def is_project_total_only(line: str) -> bool:
    return bool(_project_total_re.match(line))


def load_article_map(xlsx_path: str) -> Dict[str, str]:
    """
    Loads mapping: normalized name -> article
    Expected columns: at least "Наименование" and "Артикул" (case-insensitive).
    If Артикул is empty or 0 -> ignore (treated as not set).
    """
    if not os.path.exists(xlsx_path):
        return {}

    wb = openpyxl.load_workbook(xlsx_path, data_only=True)
    ws = wb.active

    # header row
    headers = {}
    for col in range(1, ws.max_column + 1):
        v = ws.cell(row=1, column=col).value
        if not v:
            continue
        key = str(v).strip().lower()
        headers[key] = col

    name_col = None
    art_col = None
    for k, c in headers.items():
        if k in ("наименование", "номенклатура", "name"):
            name_col = c
        if k in ("артикул", "код", "sku", "id"):
            art_col = c

    if name_col is None or art_col is None:
        # fallback: 1st and 2nd columns
        name_col = name_col or 1
        art_col = art_col or 2

    m: Dict[str, str] = {}
    for r in range(2, ws.max_row + 1):
        name = ws.cell(row=r, column=name_col).value
        art = ws.cell(row=r, column=art_col).value
        if not name:
            continue
        art_s = "" if art is None else str(art).strip()
        if art_s in ("", "0", "0.0"):
            continue
        m[normalize_name(str(name))] = art_s
    return m


# -----------------------------
# PDF parsing (core)
# -----------------------------

def extract_lines_from_pdf(pdf_bytes: bytes) -> List[str]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    lines: List[str] = []
    for page in doc:
        text = page.get_text("text") or ""
        # preserve order
        for ln in text.splitlines():
            ln = ln.strip()
            if not ln:
                continue
            # skip the project total line so it doesn't break anchors
            if is_project_total_only(ln):
                continue
            lines.append(ln)
    return lines


def parse_items_from_lines(lines: List[str]) -> List[Tuple[str, int]]:
    """
    Returns list of (raw_name, qty) in first-seen order.
    Strategy:
      - Look for anchor line: "PRICE ₽  QTY  SUM ₽"
      - Name is accumulated from preceding lines since last anchor.
    """
    items = OrderedDict()  # raw_name -> qty (sum), preserves first insertion order
    name_buf: List[str] = []

    def flush_name() -> str:
        # join buffered lines into a single name
        raw = " ".join(name_buf).strip()
        raw = _ws_re.sub(" ", raw)
        name_buf.clear()
        return raw

    for ln in lines:
        m = _qty_sum_anchor_re.match(ln)
        if m:
            # anchor hit: finalize item
            qty = int(m.group(2))
            raw_name = flush_name()
            if not raw_name:
                # Sometimes name is on same line before the anchor; try best-effort
                # (rare) - skip in that case
                continue
            if raw_name in items:
                items[raw_name] += qty
            else:
                items[raw_name] = qty
            continue

        # Not an anchor. Add to name buffer unless it looks like a stray price-only line.
        # Some PDFs break "price" on its own line; we keep it in buffer only if it has letters.
        if _price_re.search(ln) and not re.search(r"[A-Za-zА-Яа-я]", ln):
            # line mostly numeric price, ignore
            continue

        name_buf.append(ln)

    return [(k, v) for k, v in items.items()]


def build_xlsx(items: List[Tuple[str, int]], article_map: Dict[str, str]) -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "BAU"

    headers = ["Артикул", "ШТ", "Площадь"]
    ws.append(headers)

    for raw_name, qty in items:
        art = article_map.get(normalize_name(raw_name))
        if not art:
            art = "НЕ ЗАВЕДЕН"
        ws.append([art, qty, ""])  # площадь пустая

    # Styling: header bold, widths
    bold = openpyxl.styles.Font(bold=True)
    for col in range(1, 4):
        ws.cell(row=1, column=col).font = bold
        ws.cell(row=1, column=col).alignment = openpyxl.styles.Alignment(horizontal="center", vertical="center")

    ws.column_dimensions["A"].width = 18
    ws.column_dimensions["B"].width = 8
    ws.column_dimensions["C"].width = 12

    # Align data
    for r in range(2, ws.max_row + 1):
        ws.cell(row=r, column=1).alignment = openpyxl.styles.Alignment(horizontal="left", vertical="center")
        ws.cell(row=r, column=2).alignment = openpyxl.styles.Alignment(horizontal="center", vertical="center")
        ws.cell(row=r, column=3).alignment = openpyxl.styles.Alignment(horizontal="center", vertical="center")

    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()


# -----------------------------
# FastAPI app / UI
# -----------------------------

app = FastAPI(title=APP_TITLE)

# serve static assets (logo.png must be here)
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


HTML_PAGE = r"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Бауцентр — Конвертация PDF</title>
  <style>
    :root{
      --bg1:#0b2a5a;
      --bg2:#061a33;
      --card:#ffffff;
      --muted:#6b7280;
      --btn:#87a9ff;
      --btnText:#ffffff;
      --outline:rgba(0,0,0,0.08);
      --shadow: 0 18px 60px rgba(0,0,0,.25);
    }
    *{ box-sizing:border-box; }
    body{
      margin:0;
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, "Noto Sans", "Liberation Sans", sans-serif;
      min-height:100vh;
      display:flex;
      align-items:center;
      justify-content:center;
      background: radial-gradient(1200px 600px at 25% 30%, rgba(15, 136, 120, .25), transparent 60%),
                  radial-gradient(1000px 700px at 55% 35%, rgba(55, 104, 255, .25), transparent 60%),
                  radial-gradient(1200px 700px at 70% 60%, rgba(0, 0, 0, .25), transparent 65%),
                  linear-gradient(135deg, var(--bg1), var(--bg2));
      padding: 28px 16px;
    }

    .wrap{
      width:min(980px, 100%);
      display:flex;
      flex-direction:column;
      align-items:center;
      gap:18px;
    }

    /* TOP LOGO BAR (bigger, like your mock) */
    .topbar{
      display:flex;
      align-items:center;
      justify-content:center;
      gap:14px;
      padding: 14px 26px;
      border-radius: 14px;
      background: rgba(255,255,255,.08);
      backdrop-filter: blur(10px);
      box-shadow: 0 10px 30px rgba(0,0,0,.25);
      border: 1px solid rgba(255,255,255,.12);
    }
    .topbar .logo{
      height: 34px;  /* bigger */
      width:auto;
      display:block;
    }
    .topbar .brand{
      color:#fff;
      font-weight:700;
      font-size: 20px;
      letter-spacing:.2px;
      opacity:.95;
    }

    .card{
      width:min(860px, 100%);
      background: var(--card);
      border-radius: 16px;
      box-shadow: var(--shadow);
      padding: 34px 34px 26px;
    }

    .title{
      text-align:center;
      font-weight: 800;
      font-size: 28px;
      margin: 2px 0 8px;
      color:#0f172a;
    }
    .subtitle{
      text-align:center;
      margin:0 auto 18px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.4;
      max-width: 620px;
    }
    .subtitle b{ color:#0f172a; }

    .drop{
      border: 1px dashed rgba(15,23,42,.22);
      border-radius: 14px;
      padding: 24px;
      background: rgba(15,23,42,.02);
      display:flex;
      flex-direction:column;
      align-items:center;
      gap: 14px;
    }

    .icon{
      width: 40px;
      height: 40px;
      border-radius: 12px;
      border: 1px solid rgba(0,0,0,.12);
      display:flex;
      align-items:center;
      justify-content:center;
      color:#0f172a;
      background:#fff;
    }
    .hint{
      text-align:center;
      color: var(--muted);
      font-size: 13px;
      margin: 0;
    }

    .row{
      display:flex;
      gap: 12px;
      align-items:center;
      justify-content:center;
      flex-wrap:wrap;
    }

    input[type=file]{ display:none; }

    .btn{
      appearance:none;
      border: 1px solid rgba(0,0,0,.08);
      border-radius: 12px;
      padding: 10px 18px;
      font-weight: 700;
      cursor:pointer;
      background:#fff;
      color:#0f172a;
      transition: transform .06s ease, box-shadow .2s ease, opacity .2s ease;
      box-shadow: 0 6px 18px rgba(0,0,0,.08);
      user-select:none;
    }
    .btn:active{ transform: translateY(1px); }

    .btn.primary{
      background: var(--btn);
      color: var(--btnText);
      border-color: rgba(255,255,255,.25);
      box-shadow: 0 10px 25px rgba(135,169,255,.35);
    }
    .btn[disabled]{
      opacity: .55;
      cursor:not-allowed;
      box-shadow:none;
    }

    .filename{
      margin-top: 10px;
      font-size: 13px;
      color:#111827;
      opacity:.8;
      text-align:center;
    }
    .status{
      margin-top: 8px;
      font-size: 12px;
      color: var(--muted);
      text-align:center;
      min-height: 16px;
    }

    /* dragover */
    .drop.dragover{
      background: rgba(135,169,255,.10);
      border-color: rgba(135,169,255,.8);
    }

    @media (max-width: 520px){
      .card{ padding: 26px 18px 20px; }
      .title{ font-size: 22px; }
      .topbar{ padding: 12px 18px; }
      .topbar .logo{ height: 30px; }
      .topbar .brand{ font-size: 18px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="topbar" aria-label="Bauzentrum">
      <img class="logo" src="/static/logo.png" alt="Бауцентр" />
      <div class="brand">Бауцентр</div>
    </div>

    <div class="card">
      <div class="title">Конвертация PDF → Excel</div>
      <p class="subtitle">
        Загрузите PDF (отчёт/корзина из 3D конфигуратора) — получите Excel (.xlsx).<br/>
        Формат: <b>Артикул</b> / <b>ШТ</b> / <b>Площадь</b> (Площадь пустая).
      </p>

      <div id="drop" class="drop">
        <div class="icon" aria-hidden="true">
          <!-- upload icon -->
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none">
            <path d="M12 3v10" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
            <path d="M8 7l4-4 4 4" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            <path d="M4 14v4a3 3 0 003 3h10a3 3 0 003-3v-4" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
          </svg>
        </div>

        <p class="hint">Перетащите PDF сюда или выберите файл на устройстве.</p>

        <div class="row">
          <label class="btn" for="file">Выбрать PDF</label>
          <button id="downloadBtn" class="btn primary" disabled>Скачать Excel</button>
          <input id="file" type="file" accept="application/pdf" />
        </div>

        <div id="filename" class="filename"></div>
        <div id="status" class="status"></div>
      </div>
    </div>
  </div>

<script>
(() => {
  const fileInput = document.getElementById('file');
  const drop = document.getElementById('drop');
  const filenameEl = document.getElementById('filename');
  const statusEl = document.getElementById('status');
  const downloadBtn = document.getElementById('downloadBtn');

  let jobId = null;

  function setStatus(t){ statusEl.textContent = t || ''; }
  function setFilename(t){ filenameEl.textContent = t || ''; }

  async function upload(file){
    jobId = null;
    downloadBtn.disabled = true;
    setFilename(file.name);
    setStatus('Загрузка…');

    const fd = new FormData();
    fd.append('file', file);

    const r = await fetch('/extract_async', { method: 'POST', body: fd });
    if(!r.ok){
      const txt = await r.text();
      throw new Error(txt || 'Upload failed');
    }
    const j = await r.json();
    jobId = j.job_id;

    setStatus('Обработка…');
    await poll();
  }

  async function poll(){
    if(!jobId) return;
    for(;;){
      const r = await fetch(`/job/${jobId}`);
      if(!r.ok){
        setStatus('Ошибка статуса задачи');
        return;
      }
      const j = await r.json();
      if(j.status === 'pending'){
        await new Promise(res => setTimeout(res, 450));
        continue;
      }
      if(j.status === 'error'){
        setStatus('Ошибка: ' + (j.error || 'неизвестно'));
        return;
      }
      if(j.status === 'done'){
        setStatus('Готово');
        downloadBtn.disabled = false;
        return;
      }
    }
  }

  downloadBtn.addEventListener('click', async () => {
    if(!jobId) return;
    downloadBtn.disabled = true;
    setStatus('Скачивание…');
    const a = document.createElement('a');
    a.href = `/download/${jobId}`;
    a.download = 'bau.xlsx';
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => { downloadBtn.disabled = false; setStatus('Готово'); }, 400);
  });

  fileInput.addEventListener('change', (e) => {
    const f = e.target.files && e.target.files[0];
    if(!f) return;
    upload(f).catch(err => {
      console.error(err);
      setStatus('Ошибка: ' + err.message);
    });
  });

  // drag & drop
  ['dragenter','dragover'].forEach(evt => {
    drop.addEventListener(evt, (e) => {
      e.preventDefault();
      e.stopPropagation();
      drop.classList.add('dragover');
    });
  });
  ['dragleave','drop'].forEach(evt => {
    drop.addEventListener(evt, (e) => {
      e.preventDefault();
      e.stopPropagation();
      drop.classList.remove('dragover');
    });
  });
  drop.addEventListener('drop', (e) => {
    const f = e.dataTransfer.files && e.dataTransfer.files[0];
    if(!f) return;
    if(f.type !== 'application/pdf' && !f.name.toLowerCase().endsWith('.pdf')){
      setStatus('Нужен PDF файл');
      return;
    }
    upload(f).catch(err => {
      console.error(err);
      setStatus('Ошибка: ' + err.message);
    });
  });

})();
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index():
    _gc_jobs()
    return HTML_PAGE


@app.post("/extract_async")
async def extract_async(file: UploadFile = File(...)):
    _gc_jobs()
    job_id = str(uuid.uuid4())
    with JOBS_LOCK:
        JOBS[job_id] = Job(status="pending", created_at=time.time(), filename="bau.xlsx")

    pdf_bytes = await file.read()

    def work():
        try:
            article_map = load_article_map(ART_XLSX_PATH)
            lines = extract_lines_from_pdf(pdf_bytes)
            items = parse_items_from_lines(lines)
            xlsx_bytes = build_xlsx(items, article_map)

            with JOBS_LOCK:
                j = JOBS.get(job_id)
                if j:
                    j.status = "done"
                    j.data = xlsx_bytes
        except Exception as e:
            with JOBS_LOCK:
                j = JOBS.get(job_id)
                if j:
                    j.status = "error"
                    j.error = str(e)

    threading.Thread(target=work, daemon=True).start()
    return JSONResponse({"job_id": job_id})


@app.get("/job/{job_id}")
def job_status(job_id: str):
    _gc_jobs()
    with JOBS_LOCK:
        j = JOBS.get(job_id)
        if not j:
            return JSONResponse({"status": "error", "error": "job_not_found"}, status_code=404)
        if j.status == "error":
            return JSONResponse({"status": "error", "error": j.error or "unknown"})
        return JSONResponse({"status": j.status})


@app.get("/download/{job_id}")
def download(job_id: str):
    _gc_jobs()
    with JOBS_LOCK:
        j = JOBS.get(job_id)
        if not j or j.status != "done" or not j.data:
            return JSONResponse({"detail": "not_ready"}, status_code=404)
        data = j.data
        filename = j.filename or "bau.xlsx"

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"'
    }
    return StreamingResponse(io.BytesIO(data),
                             media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers=headers)
