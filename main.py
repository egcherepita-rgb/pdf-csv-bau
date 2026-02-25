import io
import os
import re
import time
import json
import threading
from uuid import uuid4
from collections import OrderedDict
from typing import List, Tuple, Dict, Optional, Any

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response, HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles

try:
    import openpyxl
except Exception:
    openpyxl = None


app = FastAPI(
    title="Бауцентр • PDF → XLSX (АРТИКУЛ / ШТ / ПЛОЩАДЬ)",
    version="1.0.0",
)

# Static files (logo, etc.)
app.mount("/static", StaticFiles(directory="static"), name="static")

# -------------------------
# Regex / helpers (из рабочего main (1).py + расширение под площадь)
# -------------------------
RX_SIZE = re.compile(r"\b\d{2,}[xх×]\d{2,}(?:[xх×]\d{1,})?\b", re.IGNORECASE)
RX_MM = re.compile(r"мм", re.IGNORECASE)
RX_WEIGHT = re.compile(r"\b\d+(?:[.,]\d+)?\s*кг\.?\b", re.IGNORECASE)

RX_MONEY_LINE = re.compile(r"^\d+(?:[ \u00a0]\d{3})*(?:[.,]\d+)?\s*₽$")
RX_INT = re.compile(r"^\d+$")
RX_ANY_RUB = re.compile(r"₽")

RX_PRICE_QTY_SUM = re.compile(
    r"(?P<price>\d+(?:[ \u00a0]\d{3})*(?:[.,]\d+)?)\s*₽\s+"
    r"(?P<qty>\d{1,4})\s+"
    r"(?P<sum>\d+(?:[ \u00a0]\d{3})*(?:[.,]\d+)?)\s*₽",
    re.IGNORECASE,
)

RX_DIMS_ANYWHERE = re.compile(
    r"\s*\d{1,4}[xх×]\d{1,4}(?:[xх×]\d{1,5})?\s*мм\.?\s*",
    re.IGNORECASE,
)

# Площадь: "1.23 м2", "1,23 м²", "1.23 кв. м"
RX_AREA = re.compile(
    r"(?P<val>\d+(?:[.,]\d+)?)\s*(?:м2|м²|кв\.?\s*м|квм)\b",
    re.IGNORECASE,
)

RX_FLOAT_ONLY = re.compile(r"^\d+(?:[.,]\d+)?$")


def normalize_space(s: str) -> str:
    s = (s or "").replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def normalize_key(name: str) -> str:
    s = normalize_space(name).lower()
    s = s.replace("×", "x").replace("х", "x")
    s = RX_DIMS_ANYWHERE.sub(" ", s)
    s = normalize_space(s)
    return s


def strip_dims_anywhere(name: str) -> str:
    name = normalize_space(name)
    name2 = RX_DIMS_ANYWHERE.sub(" ", name)
    return normalize_space(name2)


# -------------------------
# Артикулы (Art1.xlsx) — кастомные для Бауцентра
# -------------------------
def load_article_map() -> Tuple[Dict[str, str], str]:
    if openpyxl is None:
        return {}, "openpyxl_not_installed"

    # По умолчанию Art1.xlsx (как ты написал)
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
        if not товар or not арт:
            continue
        товар_s = normalize_space(str(товар))
        арт_s = normalize_space(str(арт))
        if not товар_s or not арт_s:
            continue
        m[normalize_key(товар_s)] = арт_s

    return m, "ok"


ARTICLE_MAP, ARTICLE_MAP_STATUS = load_article_map()

# -------------------------
# Счетчик конвертаций (простое файловое хранилище)
# -------------------------
from threading import Lock

COUNTER_FILE = os.getenv("COUNTER_FILE", "conversions.count")
_counter_lock = Lock()


def _read_counter() -> int:
    try:
        with open(COUNTER_FILE, "r", encoding="utf-8") as f:
            return int((f.read() or "0").strip())
    except Exception:
        return 0


def _write_counter(v: int) -> None:
    tmp = COUNTER_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(str(v))
    os.replace(tmp, COUNTER_FILE)


def increment_counter() -> int:
    with _counter_lock:
        v = _read_counter() + 1
        _write_counter(v)
        return v


def get_counter() -> int:
    return _read_counter()


# -------------------------
# Async jobs (progress) — FILE storage
# -------------------------
JOB_DIR = os.getenv("JOB_DIR", "/tmp/pdf2xlsx_bau_jobs")
JOB_TTL_SEC = int(os.getenv("JOB_TTL_SEC", str(30 * 60)))  # seconds


def _ensure_job_dir() -> None:
    os.makedirs(JOB_DIR, exist_ok=True)


def _job_json_path(job_id: str) -> str:
    return os.path.join(JOB_DIR, f"{job_id}.json")


def _job_result_path(job_id: str) -> str:
    return os.path.join(JOB_DIR, f"{job_id}.xlsx")


def _write_json_atomic(path: str, data: dict) -> None:
    _ensure_job_dir()
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)


def _read_json(path: str) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _cleanup_jobs() -> None:
    _ensure_job_dir()
    now = time.time()
    try:
        for name in os.listdir(JOB_DIR):
            if not (name.endswith(".json") or name.endswith(".xlsx")):
                continue
            p = os.path.join(JOB_DIR, name)
            try:
                if now - os.path.getmtime(p) > JOB_TTL_SEC:
                    os.remove(p)
            except Exception:
                pass
    except Exception:
        pass


def _set_job(job_id: str, **kwargs) -> None:
    _cleanup_jobs()
    path = _job_json_path(job_id)
    data = _read_json(path) or {}
    data.update(kwargs)
    _write_json_atomic(path, data)


def _get_job(job_id: str) -> Optional[dict]:
    _cleanup_jobs()
    return _read_json(_job_json_path(job_id))


def _set_job_result(job_id: str, xlsx_bytes: bytes) -> None:
    _cleanup_jobs()
    _ensure_job_dir()
    p = _job_result_path(job_id)
    tmp = p + ".tmp"
    with open(tmp, "wb") as f:
        f.write(xlsx_bytes)
    os.replace(tmp, p)


def _get_job_result(job_id: str) -> Optional[bytes]:
    p = _job_result_path(job_id)
    try:
        with open(p, "rb") as f:
            return f.read()
    except Exception:
        return None


# -------------------------
# Instruction media (optional)
# -------------------------
INSTRUCTION_VIDEO_PATH = os.getenv("INSTRUCTION_VIDEO_PATH", "instruction.mp4")

# -------------------------
# PDF parsing helpers (из рабочего main (1).py)
# -------------------------
def is_noise(line: str) -> bool:
    low = (line or "").strip().lower()
    if not low:
        return True
    if low.startswith("страница:"):
        return True
    if low.startswith("ваш проект"):
        return True
    if "проект создан" in low:
        return True
    if "развертка стены" in low:
        return True
    if "стоимость проекта" in low:
        return True
    return False


def is_totals_block(line: str) -> bool:
    low = (line or "").strip().lower()
    return (
        low.startswith("общий вес")
        or low.startswith("максимальный габарит заказа")
        or low.startswith("адрес:")
        or low.startswith("телефон:")
        or low.startswith("email")
    )


def money_to_number(line: str) -> int:
    s = normalize_space(line).replace("₽", "").strip()
    s = s.replace("\u00a0", " ")
    s = s.replace(" ", "")
    s = s.replace(",", ".")
    try:
        return int(float(s))
    except Exception:
        return -1


def is_project_total_only(line: str, prev_line: str = "") -> bool:
    if not RX_MONEY_LINE.fullmatch(normalize_space(line)):
        return False
    prev = normalize_space(prev_line).lower()
    if "стоимость проекта" in prev:
        return True
    v = money_to_number(line)
    return v >= 10000


def is_header_token(line: str) -> bool:
    low = normalize_space(line).lower().replace("–", "-").replace("—", "-")
    return low in {"фото", "товар", "габариты", "вес", "цена за шт", "кол-во", "сумма", "площадь"}


def looks_like_dim_or_weight(line: str) -> bool:
    if RX_WEIGHT.search(line):
        return True
    if RX_SIZE.search(line) and RX_MM.search(line):
        return True
    return False


def looks_like_money_or_qty(line: str) -> bool:
    if RX_MONEY_LINE.fullmatch(line):
        return True
    if RX_INT.fullmatch(line):
        return True
    return False


def clean_name_from_buffer(buf: List[str]) -> str:
    filtered: List[str] = []
    for ln in buf:
        if is_noise(ln) or is_header_token(ln) or is_totals_block(ln):
            continue
        filtered.append(ln)

    while filtered and (looks_like_dim_or_weight(filtered[-1]) or looks_like_money_or_qty(filtered[-1])):
        filtered.pop()

    name = normalize_space(" ".join(filtered))
    name = re.sub(r"^Фото\s*", "", name, flags=re.IGNORECASE).strip()
    name = re.sub(r"^Товар\s*", "", name, flags=re.IGNORECASE).strip()
    name = strip_dims_anywhere(name)
    return name


def _to_float(s: str) -> float:
    s = normalize_space(s).replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def extract_area_from_context(lines: List[str]) -> float:
    """
    Пытаемся вытащить площадь из набора строк вокруг позиции.
    Поддержка вариантов:
      - "1.23 м2", "1,23 м²", "1.23 кв. м"
      - "Площадь 1.23" / "Площадь:" + следующая строка "1.23"
    """
    # 1) явные единицы
    for ln in lines:
        m = RX_AREA.search(ln)
        if m:
            return _to_float(m.group("val"))

    # 2) по слову "площадь"
    for idx, ln in enumerate(lines):
        low = ln.lower()
        if "площад" in low:
            # число в той же строке
            mm = re.search(r"(\d+(?:[.,]\d+)?)", ln)
            if mm:
                return _to_float(mm.group(1))
            # число в следующей строке
            if idx + 1 < len(lines) and RX_FLOAT_ONLY.fullmatch(lines[idx + 1]):
                return _to_float(lines[idx + 1])

    return 0.0


# -------------------------
# Main parser: собираем name -> (qty_sum, area_sum)
# -------------------------
def parse_items(pdf_bytes: bytes) -> Tuple[List[Tuple[str, int, float]], Dict[str, Any]]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    total_pages = doc.page_count

    # name -> {"qty": int, "area": float}
    ordered: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
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
        # сохраняем старую фильтрацию — в большинстве твоих PDF "якорь" через ₽
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
                # площадь пробуем искать в буфере + текущей строке + пары следующих строк
                ctx = (buf + [line] + lines[i + 1 : i + 4]) if buf else ([line] + lines[i + 1 : i + 4])
                area = extract_area_from_context(ctx)
                buf.clear()

                if name:
                    try:
                        qty = int(m.group("qty"))
                    except Exception:
                        qty = 0

                    if 1 <= qty <= 500:
                        if name not in ordered:
                            ordered[name] = {"qty": 0, "area": 0.0}
                        ordered[name]["qty"] += qty
                        ordered[name]["area"] += float(area or 0.0)
                        stats["items_found"] += 1
                        stats["anchors_inline"] += 1

                i += 1
                continue

            # C) EMBEDDED price anchor: line contains ₽, next line qty, next line sum ₽
            if RX_ANY_RUB.search(line):
                if i + 2 < len(lines) and RX_INT.fullmatch(lines[i + 1]) and RX_MONEY_LINE.fullmatch(lines[i + 2]):
                    try:
                        qty = int(lines[i + 1])
                    except Exception:
                        qty = 0

                    if 1 <= qty <= 500:
                        name = clean_name_from_buffer(buf + [line])
                        ctx = (buf + [line] + lines[i + 1 : i + 5])
                        area = extract_area_from_context(ctx)
                        buf.clear()

                        if name:
                            if name not in ordered:
                                ordered[name] = {"qty": 0, "area": 0.0}
                            ordered[name]["qty"] += qty
                            ordered[name]["area"] += float(area or 0.0)
                            stats["items_found"] += 1
                            stats["anchors_multiline"] += 1

                    i += 3
                    continue

            # B) MULTILINE anchor: price ₽ -> qty -> sum ₽
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
                ctx = (buf + lines[i : sum_idx + 3])  # чуть шире окно
                area = extract_area_from_context(ctx)
                buf.clear()

                if name:
                    qty = int(lines[qty_idx])
                    if name not in ordered:
                        ordered[name] = {"qty": 0, "area": 0.0}
                    ordered[name]["qty"] += qty
                    ordered[name]["area"] += float(area or 0.0)
                    stats["items_found"] += 1
                    stats["anchors_multiline"] += 1

                i = sum_idx + 1
                continue

            buf.append(line)
            i += 1

    out_rows: List[Tuple[str, int, float]] = []
    for name, v in ordered.items():
        out_rows.append((name, int(v.get("qty") or 0), float(v.get("area") or 0.0)))

    return out_rows, stats


# -------------------------
# XLSX output (АРТИКУЛ / ШТ / ПЛОЩАДЬ)
# -------------------------
def make_xlsx(rows: List[Tuple[str, int, float]]) -> bytes:
    if openpyxl is None:
        raise RuntimeError("openpyxl is not installed")

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "BAU"

    ws.append(["АРТИКУЛ", "ШТ", "ПЛОЩАДЬ"])

    for name, qty, area in rows:
        art = ARTICLE_MAP.get(normalize_key(name), "")
        # ПЛОЩАДЬ пишем числом (float). Если не нашли — будет 0.
        ws.append([art, int(qty or 0), float(area or 0.0)])

    # небольшая косметика
    ws.column_dimensions["A"].width = 18
    ws.column_dimensions["B"].width = 10
    ws.column_dimensions["C"].width = 14

    for cell in ws["B"][1:]:
        cell.number_format = "0"
    for cell in ws["C"][1:]:
        cell.number_format = "0.00"

    # закрепить шапку
    ws.freeze_panes = "A2"

    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()


# -------------------------
# UI (простая страница, без скролла)
# -------------------------
HOME_HTML = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Бауцентр • PDF → XLSX</title>
  <style>
    :root { --bg:#0b0f17; --card:#121a2a; --text:#e9eefc; --muted:#a8b3d6; --border:rgba(255,255,255,.08); --btn:#ffd33d; }
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
      top: 18px;
      left: 0;
      width: 100%;
      text-align: center;
      z-index: 5;
      pointer-events: none;
    }
    .hero-logo { margin-top: 6px; height: 76px; width: auto; opacity: .98; }
    .wrap {
      height: 100vh;
      display:flex;
      align-items:center;
      justify-content:center;
      padding: 28px;
      padding-top: 150px;
    }
    .card {
      width:min(900px, 100%);
      background: rgba(18,26,42,.92);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 22px;
      box-shadow: 0 18px 60px rgba(0,0,0,.45);
    }
    .top { display:flex; gap:14px; align-items:center; justify-content:space-between; flex-wrap:wrap; }
    h1 { margin:0; font-size: 28px; letter-spacing: .2px; }
    .hint { margin: 8px 0 0; color: var(--muted); font-size: 14px; }
    .badge { font-size: 12px; color: var(--muted); border: 1px solid var(--border); padding: 6px 10px; border-radius: 999px; }
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
      font-weight: 900;
      background: var(--btn);
      color: #1a1a1a;
    }
    button:disabled { opacity: .55; cursor:not-allowed; }
    .status { margin-top: 14px; font-size: 14px; color: var(--muted); white-space: pre-wrap; text-align:center; }
    .status.ok { color: #79ffa8; }
    .status.err { color: #ff7b8a; }
    .corner { position: fixed; right: 12px; bottom: 10px; font-size: 12px; color: var(--muted); opacity: .9; }
  </style>
</head>
<body>
  <div class="hero">
    <img class="hero-logo" src="/static/logo.png" alt="Бауцентр" />
  </div>

  <div class="wrap">
    <div class="card">
      <div class="top">
        <div>
          <h1>PDF → XLSX</h1>
          <div class="hint">Скачивание файла Excel: АРТИКУЛ / ШТ / ПЛОЩАДЬ</div>
        </div>
        <div class="badge">XLSX • 3 колонки</div>
      </div>

      <div class="row">
        <div class="file">
          <input id="pdf" type="file" accept="application/pdf,.pdf" />
        </div>
        <button id="btn" disabled>Скачать XLSX</button>
      </div>

      <div id="status" class="status"></div>
    </div>
  </div>

  <div class="corner" id="counter">…</div>

  <script>
    const input = document.getElementById('pdf');
    const btn = document.getElementById('btn');
    const statusEl = document.getElementById('status');

    function ok(msg){ statusEl.className='status ok'; statusEl.textContent=msg; }
    function err(msg){ statusEl.className='status err'; statusEl.textContent=msg; }
    function neutral(msg){ statusEl.className='status'; statusEl.textContent=msg||''; }

    async function loadCounter(){
      try {
        const r = await fetch('/stats');
        if (!r.ok) return;
        const j = await r.json();
        if (typeof j.conversions === 'number') {
          document.getElementById('counter').textContent = String(j.conversions);
        }
      } catch(e) {}
    }
    loadCounter();

    input.addEventListener('change', () => {
      const f = input.files && input.files[0];
      btn.disabled = !f;
      neutral(f ? ('Выбран файл: ' + f.name) : '');
    });

    btn.addEventListener('click', async () => {
      const f = input.files && input.files[0];
      if (!f) return;

      btn.disabled = true;
      const start = Date.now();

      let timer = setInterval(() => {
        const sec = Math.floor((Date.now() - start) / 1000);
        neutral('Обработка… прошло ' + sec + ' сек');
      }, 500);

      try {
        const fd = new FormData();
        fd.append('file', f);

        neutral('Загружаю PDF…');
        const r = await fetch('/extract_async', { method: 'POST', body: fd });
        if (!r.ok) {
          let text = await r.text();
          try { const j = JSON.parse(text); if (j.detail) text = String(j.detail); } catch(e) {}
          throw new Error(text || ('HTTP ' + r.status));
        }
        const data = await r.json();
        const job_id = data.job_id;

        while (true) {
          const s = await fetch('/job/' + job_id);
          if (!s.ok) {
            let text = await s.text();
            try { const j = JSON.parse(text); if (j.detail) text = String(j.detail); } catch(e) {}
            throw new Error(text || ('HTTP ' + s.status));
          }
          const j = await s.json();

          const sec = Math.floor((Date.now() - start) / 1000);
          let msg = (j.message || 'Обработка…') + ' • ' + sec + ' сек';

          if (j.total_pages && j.processed_pages) {
            msg += ' • страниц: ' + j.processed_pages + '/' + j.total_pages;
          }
          neutral(msg);

          if (j.status === 'done') {
            const dl = await fetch('/job/' + job_id + '/download');
            if (!dl.ok) {
              let text = await dl.text();
              try { const jj = JSON.parse(text); if (jj.detail) text = String(jj.detail); } catch(e) {}
              throw new Error(text || ('HTTP ' + dl.status));
            }
            const blob = await dl.blob();

            const filename = (j.filename || ((f.name || 'items.pdf').replace(/\\.pdf$/i, '') + '.xlsx'));
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            a.remove();
            URL.revokeObjectURL(url);

            ok('Готово! Файл скачан: ' + filename);
            loadCounter();
            break;
          }

          if (j.status === 'error') {
            throw new Error(j.message || 'Ошибка обработки');
          }

          await new Promise(res => setTimeout(res, 600));
        }

      } catch (e) {
        err('Ошибка: ' + String(e.message || e));
      } finally {
        clearInterval(timer);
        btn.disabled = !(input.files && input.files[0]);
      }
    });
  </script>
</body>
</html>
"""


# -------------------------
# Endpoints
# -------------------------
@app.get("/stats")
def stats():
    return {"conversions": get_counter()}


@app.get("/health")
def health():
    try:
        _ensure_job_dir()
        job_files = len([x for x in os.listdir(JOB_DIR) if x.endswith(".json")])
    except Exception:
        job_files = -1

    return {
        "status": "ok",
        "article_map_size": len(ARTICLE_MAP),
        "article_map_status": ARTICLE_MAP_STATUS,
        "conversions": get_counter(),
        "job_dir": JOB_DIR,
        "job_files": job_files,
        "openpyxl": bool(openpyxl is not None),
    }


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def home():
    return HOME_HTML


@app.api_route("/instruction.mp4", methods=["GET", "HEAD"])
def instruction_video():
    if not os.path.exists(INSTRUCTION_VIDEO_PATH):
        raise HTTPException(status_code=404, detail="instruction.mp4 not found")
    return FileResponse(INSTRUCTION_VIDEO_PATH, media_type="video/mp4")


@app.post("/extract")
async def extract(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Загрузите PDF файл (.pdf).")

    if openpyxl is None:
        raise HTTPException(status_code=500, detail="openpyxl не установлен (нужен для XLSX).")

    pdf_bytes = await file.read()

    try:
        rows, stats_ = parse_items(pdf_bytes)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Не удалось распарсить PDF: {e}")

    if not rows:
        raise HTTPException(
            status_code=422,
            detail=f"Не удалось найти позиции. debug={stats_}",
        )

    xlsx_bytes = make_xlsx(rows)
    increment_counter()
    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="bau_items.xlsx"'},
    )


@app.post("/extract_async")
async def extract_async(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Загрузите PDF файл (.pdf).")

    if openpyxl is None:
        raise HTTPException(status_code=500, detail="openpyxl не установлен (нужен для XLSX).")

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

            xlsx_bytes = make_xlsx(rows)
            _set_job_result(job_id, xlsx_bytes)
            increment_counter()

            _set_job(
                job_id,
                status="done",
                message="Готово",
                stats=st,
            )
        except Exception as e:
            _set_job(job_id, status="error", message=f"Ошибка: {e}")

    threading.Thread(target=worker, daemon=True).start()
    return {"job_id": job_id}


@app.get("/job/{job_id}")
def job_status(job_id: str):
    j = _get_job(job_id)
    if not j:
        raise HTTPException(status_code=404, detail="job not found")

    return {
        "status": j.get("status"),
        "message": j.get("message"),
        "processed_pages": int(j.get("processed_pages", 0) or 0),
        "total_pages": int(j.get("total_pages", 0) or 0),
        "stats": j.get("stats"),
        "filename": j.get("filename"),
    }


import urllib.parse


@app.get("/job/{job_id}/download")
def job_download(job_id: str):
    j = _get_job(job_id)
    if not j:
        raise HTTPException(status_code=404, detail="job not found")
    if j.get("status") != "done":
        raise HTTPException(status_code=409, detail="job not done")

    xlsx_bytes = _get_job_result(job_id)
    if not xlsx_bytes:
        raise HTTPException(status_code=404, detail="result not found")

    filename_utf8 = j.get("filename", "items.xlsx")
    filename_ascii = re.sub(r"[^A-Za-z0-9_.-]+", "_", filename_utf8)
    quoted = urllib.parse.quote(filename_utf8)

    headers = {
        "Content-Disposition": (
            f'attachment; filename="{filename_ascii}"; '
            f"filename*=UTF-8''{quoted}"
        )
    }

    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )
