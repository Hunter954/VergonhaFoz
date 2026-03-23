import base64
import hashlib
import json
import os
import re
import time
from collections import deque
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from flask import current_app

from .extensions import db
from .models import PropertyRecord, PropertyScan, SourcePdf
from .pdf_parser import parse_property_pdf

try:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except Exception:
    sync_playwright = None
    PlaywrightTimeoutError = Exception
    PLAYWRIGHT_AVAILABLE = False


PDF_PATTERNS = [
    re.compile(r"\.pdf(?:$|\?)", re.IGNORECASE),
    re.compile(r"(emitir|guia|boleto|carn[eê]|relatorio|documento|pdf|espelho)", re.IGNORECASE),
]

NO_CADASTRO_PATTERNS = [
    re.compile(r"n[aã]o\s+foi\s+poss[ií]vel\s+carregar\s+os\s+dados\s+do\s+cadastro", re.IGNORECASE),
    re.compile(r"tente\s+novamente\s+mais\s+tarde", re.IGNORECASE),
    re.compile(r"cadastro\s+n[aã]o\s+encontrado", re.IGNORECASE),
    re.compile(r"im[oó]vel\s+n[aã]o\s+encontrado", re.IGNORECASE),
]

PRINT_SELECTORS = [
    "i.fa-print",
    "i.fas.fa-print",
    "[title*='imprim' i]",
    "button[title*='imprim' i]",
    "a[title*='imprim' i]",
    "text=Imprimir",
]

DOWNLOAD_SELECTORS = [
    "text=Baixar",
    "a:has-text('Baixar')",
    "button:has-text('Baixar')",
    "[download]",
    "a[href^='blob:']",
    "iframe[src^='blob:']",
    "embed[src^='blob:']",
]


def _headers(referer=None):
    headers = {
        "User-Agent": current_app.config["USER_AGENT"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
    }
    if referer:
        headers["Referer"] = referer
    return headers


def same_host(a: str, b: str) -> bool:
    return urlparse(a).netloc == urlparse(b).netloc


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def sha256_of_bytes(content: bytes) -> str:
    h = hashlib.sha256()
    h.update(content)
    return h.hexdigest()


def is_probably_pdf_link(url: str) -> bool:
    return any(pattern.search(url) for pattern in PDF_PATTERNS)


def extract_page_title(html: str) -> str | None:
    soup = BeautifulSoup(html or "", "html.parser")
    if soup.title and soup.title.text:
        return normalize_text(soup.title.text)[:255]
    return None


def page_indicates_no_cadastro(text: str) -> bool:
    normalized = normalize_text(text)
    return any(pattern.search(normalized) for pattern in NO_CADASTRO_PATTERNS)


def discover_pdf_links_in_html(base_url: str, html: str, max_depth: int = 1):
    seen = set()
    found = []
    queue = deque([(base_url, html, 0)])

    while queue:
        page_url, page_html, depth = queue.popleft()
        if page_url in seen or depth > max_depth:
            continue
        seen.add(page_url)

        soup = BeautifulSoup(page_html or "", "html.parser")
        for tag in soup.find_all(["a", "iframe", "embed", "object", "button"]):
            href = tag.get("href") or tag.get("src") or tag.get("data") or tag.get("data-href")
            onclick = tag.get("onclick") or ""
            text = tag.get_text(" ", strip=True) or ""
            candidates = []
            if href:
                candidates.append(href)
            if onclick:
                candidates.extend(re.findall(r"['\"](.*?)['\"]", onclick))
            if text and is_probably_pdf_link(text):
                candidates.append(text)

            for candidate in candidates:
                absolute = urljoin(page_url, candidate)
                if same_host(base_url, absolute) and is_probably_pdf_link(absolute):
                    found.append((absolute, page_url))

        for match in re.findall(r'https?://[^"\'\s>]+', page_html or ""):
            if same_host(base_url, match) and is_probably_pdf_link(match):
                found.append((match, page_url))

        for match in re.findall(r'(/[A-Za-z0-9_\-\./?=&%]+)', page_html or ""):
            if "pdf" in match.lower() or any(word in match.lower() for word in ["boleto", "guia", "carne", "emitir", "relatorio", "espelho"]):
                absolute = urljoin(page_url, match)
                if same_host(base_url, absolute):
                    found.append((absolute, page_url))

    unique = []
    unique_urls = set()
    for source_url, page_url in found:
        if source_url not in unique_urls:
            unique.append((source_url, page_url))
            unique_urls.add(source_url)
    return unique


def fetch_target_page(url: str):
    timeout = current_app.config["CRAWL_TIMEOUT_SECONDS"]
    response = requests.get(url, headers=_headers(), timeout=timeout)
    content_type = (response.headers.get("content-type") or "").lower()
    body_text = response.text if "text" in content_type or "json" in content_type or not content_type else response.text

    no_cadastro = False
    if "json" in content_type:
        try:
            payload = response.json()
            body_text = json.dumps(payload, ensure_ascii=False)
        except Exception:
            body_text = response.text

    if page_indicates_no_cadastro(body_text):
        no_cadastro = True

    return {
        "status_code": response.status_code,
        "content_type": content_type,
        "text": body_text,
        "html": response.text,
        "page_title": extract_page_title(response.text),
        "no_cadastro": no_cadastro,
        "response": response,
    }


def save_pdf_bytes(content: bytes, source_url: str, referer_url: str | None = None, status: str = "baixado"):
    digest = sha256_of_bytes(content)
    existing = SourcePdf.query.filter_by(sha256=digest).first()
    if existing:
        if referer_url and not existing.page_url:
            existing.page_url = referer_url
            db.session.commit()
        return existing, False

    filename = f"{digest}.pdf"
    file_path = os.path.join(current_app.config["PDF_STORAGE_DIR"], filename)
    with open(file_path, "wb") as f:
        f.write(content)

    source = SourcePdf(
        source_url=source_url,
        page_url=referer_url,
        file_path=file_path,
        sha256=digest,
        file_size=len(content),
        status=status,
    )
    db.session.add(source)
    db.session.commit()
    return source, True


def download_pdf(source_url: str, referer_url: str | None = None):
    timeout = current_app.config["CRAWL_TIMEOUT_SECONDS"]
    response = requests.get(source_url, headers=_headers(referer_url), timeout=timeout)
    response.raise_for_status()

    content_type = (response.headers.get("content-type") or "").lower()
    if "application/pdf" not in content_type and not response.content.startswith(b"%PDF"):
        raise ValueError(f"URL retornou conteúdo não-PDF: {content_type}")

    return save_pdf_bytes(response.content, source_url=source_url, referer_url=referer_url)


def process_pdf(source: SourcePdf):
    data = parse_property_pdf(source.file_path)

    if source.property_record:
        record = source.property_record
        for key, value in data.items():
            setattr(record, key, value)
        record.raw_json = data
    else:
        record = PropertyRecord(source_pdf_id=source.id, raw_json=data, **data)
        db.session.add(record)

    source.last_processed_at = db.func.now()
    source.status = "processado"
    db.session.commit()
    return record


def get_or_create_scan(sequence_id: int, target_url: str) -> PropertyScan:
    scan = PropertyScan.query.filter_by(sequence_id=sequence_id).first()
    if scan:
        return scan
    scan = PropertyScan(sequence_id=sequence_id, target_url=target_url, status="pendente")
    db.session.add(scan)
    db.session.commit()
    return scan


def update_scan(scan: PropertyScan, **kwargs):
    for key, value in kwargs.items():
        setattr(scan, key, value)
    scan.updated_at = datetime.utcnow()
    db.session.commit()
    return scan


def _page_text(page) -> str:
    try:
        return normalize_text(page.locator("body").inner_text(timeout=4000))
    except Exception:
        try:
            return normalize_text(page.content())
        except Exception:
            return ""


def _find_first(page, selectors):
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if locator.count() > 0 and locator.is_visible(timeout=1000):
                return locator, selector
        except Exception:
            continue
    return None, None


def _extract_blob_from_page(page):
    try:
        current_url = page.url or ""
        if current_url.startswith("blob:"):
            b64 = page.evaluate(
                """async () => {
                    const res = await fetch(window.location.href);
                    const buf = await res.arrayBuffer();
                    let binary = '';
                    const bytes = new Uint8Array(buf);
                    const chunk = 0x8000;
                    for (let i = 0; i < bytes.length; i += chunk) {
                      binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
                    }
                    return btoa(binary);
                }"""
            )
            return base64.b64decode(b64), current_url
    except Exception:
        pass

    js = """async () => {
        const candidates = [];
        const pushIfBlob = (v) => { if (v && typeof v === 'string' && v.startsWith('blob:')) candidates.push(v); };
        document.querySelectorAll('a, iframe, embed, object').forEach(el => {
          pushIfBlob(el.href);
          pushIfBlob(el.src);
          pushIfBlob(el.data);
        });
        if (!candidates.length) return null;
        const res = await fetch(candidates[0]);
        const buf = await res.arrayBuffer();
        let binary = '';
        const bytes = new Uint8Array(buf);
        const chunk = 0x8000;
        for (let i = 0; i < bytes.length; i += chunk) {
          binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
        }
        return { url: candidates[0], b64: btoa(binary) };
    }"""
    try:
        result = page.evaluate(js)
        if result and result.get("b64"):
            return base64.b64decode(result["b64"]), result.get("url")
    except Exception:
        pass
    return None, None


def _capture_pdf_via_playwright(target_url: str):
    if not PLAYWRIGHT_AVAILABLE:
        raise RuntimeError("Playwright não está instalado na aplicação.")

    timeout_ms = current_app.config["PLAYWRIGHT_TIMEOUT_MS"]
    wait_after_print_ms = current_app.config["PLAYWRIGHT_WAIT_AFTER_PRINT_MS"]
    use_headless = current_app.config["PLAYWRIGHT_HEADLESS"]

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=use_headless,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        context = browser.new_context(
            accept_downloads=True,
            user_agent=current_app.config["USER_AGENT"],
            locale="pt-BR",
            viewport={"width": 1440, "height": 1100},
        )
        page = context.new_page()
        page.goto(target_url, wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_load_state("networkidle", timeout=timeout_ms)

        body_text = _page_text(page)
        if page_indicates_no_cadastro(body_text):
            browser.close()
            return {"status": "sem_cadastro", "message": "Não foi possível carregar os dados do cadastro."}

        print_locator, print_selector = _find_first(page, PRINT_SELECTORS)
        if not print_locator:
            browser.close()
            raise RuntimeError("Ícone/botão de impressão não encontrado na página.")

        popup = None
        try:
            with context.expect_page(timeout=3000) as popup_info:
                print_locator.click(timeout=timeout_ms)
            popup = popup_info.value
            popup.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
        except Exception:
            print_locator.click(timeout=timeout_ms)
            page.wait_for_timeout(wait_after_print_ms)

        active_page = popup or page
        try:
            active_page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 10000))
        except Exception:
            pass
        active_page.wait_for_timeout(wait_after_print_ms)

        no_cadastro_text = _page_text(active_page)
        if page_indicates_no_cadastro(no_cadastro_text):
            browser.close()
            return {"status": "sem_cadastro", "message": "Não foi possível carregar os dados do cadastro."}

        pdf_bytes, pdf_blob_url = _extract_blob_from_page(active_page)
        if pdf_bytes and pdf_bytes.startswith(b"%PDF"):
            browser.close()
            return {
                "status": "processado",
                "pdf_bytes": pdf_bytes,
                "pdf_url": pdf_blob_url or active_page.url,
                "capture_method": f"playwright_blob:{print_selector}",
            }

        download_locator, download_selector = _find_first(active_page, DOWNLOAD_SELECTORS)
        if download_locator:
            try:
                with active_page.expect_download(timeout=timeout_ms) as download_info:
                    download_locator.click(timeout=timeout_ms)
                download = download_info.value
                tmp_path = download.path()
                if tmp_path and os.path.exists(tmp_path):
                    with open(tmp_path, "rb") as f:
                        pdf_bytes = f.read()
                    browser.close()
                    return {
                        "status": "processado",
                        "pdf_bytes": pdf_bytes,
                        "pdf_url": download.url,
                        "capture_method": f"playwright_download:{download_selector}",
                    }
            except Exception:
                pass

        # Última tentativa: procurar blob novamente após clicar em baixar.
        try:
            if download_locator:
                download_locator.click(timeout=timeout_ms)
                active_page.wait_for_timeout(1500)
                pdf_bytes, pdf_blob_url = _extract_blob_from_page(active_page)
                if pdf_bytes and pdf_bytes.startswith(b"%PDF"):
                    browser.close()
                    return {
                        "status": "processado",
                        "pdf_bytes": pdf_bytes,
                        "pdf_url": pdf_blob_url or active_page.url,
                        "capture_method": f"playwright_blob_after_download:{download_selector}",
                    }
        except Exception:
            pass

        browser.close()
        raise RuntimeError("PDF não encontrado nem capturado após clicar em imprimir/baixar.")


def process_sequence_id(sequence_id: int):
    target_url = current_app.config["SEQUENTIAL_URL_TEMPLATE"].format(id=sequence_id)
    scan = get_or_create_scan(sequence_id, target_url)
    scan.attempts += 1
    scan.last_checked_at = datetime.utcnow()
    db.session.commit()

    try:
        page = fetch_target_page(target_url)
        scan.http_status = page["status_code"]
        scan.page_title = page["page_title"]

        if page["no_cadastro"]:
            update_scan(
                scan,
                status="sem_cadastro",
                no_cadastro_detected=True,
                error_message="Não foi possível carregar os dados do cadastro.",
            )
            return {"sequence_id": sequence_id, "status": "sem_cadastro"}

        links = discover_pdf_links_in_html(target_url, page["html"], max_depth=1)
        if links:
            pdf_url, referer = links[0]
            source, _created = download_pdf(pdf_url, referer)
            scan.pdf_url = pdf_url
            scan.source_pdf_id = source.id
            source.property_scan = scan
            db.session.commit()

            process_pdf(source)
            update_scan(
                scan,
                status="processado",
                no_cadastro_detected=False,
                error_message=None,
            )
            return {"sequence_id": sequence_id, "status": "processado", "pdf_url": pdf_url, "method": "requests"}

        if current_app.config.get("PLAYWRIGHT_ENABLED", True):
            pw = _capture_pdf_via_playwright(target_url)
            if pw["status"] == "sem_cadastro":
                update_scan(
                    scan,
                    status="sem_cadastro",
                    no_cadastro_detected=True,
                    error_message=pw.get("message") or "Não foi possível carregar os dados do cadastro.",
                )
                return {"sequence_id": sequence_id, "status": "sem_cadastro", "method": "playwright"}

            source, _created = save_pdf_bytes(
                pw["pdf_bytes"],
                source_url=pw.get("pdf_url") or f"blob:{sequence_id}",
                referer_url=target_url,
                status="baixado",
            )
            scan.pdf_url = pw.get("pdf_url")
            scan.source_pdf_id = source.id
            source.property_scan = scan
            db.session.commit()
            process_pdf(source)
            update_scan(scan, status="processado", no_cadastro_detected=False, error_message=None)
            return {
                "sequence_id": sequence_id,
                "status": "processado",
                "pdf_url": pw.get("pdf_url"),
                "method": pw.get("capture_method", "playwright"),
            }

        update_scan(
            scan,
            status="pdf_nao_encontrado",
            no_cadastro_detected=False,
            error_message="Página carregou, mas nenhum PDF foi localizado.",
        )
        return {"sequence_id": sequence_id, "status": "pdf_nao_encontrado"}
    except Exception as exc:
        update_scan(scan, status="erro", error_message=str(exc))
        return {"sequence_id": sequence_id, "status": "erro", "error": str(exc)}


def get_next_range(batch_size: int, start_id: int, end_id: int):
    last_scan = PropertyScan.query.order_by(PropertyScan.sequence_id.desc()).first()
    next_id = start_id if not last_scan else max(last_scan.sequence_id + 1, start_id)
    if next_id > end_id:
        return None
    final_id = min(next_id + batch_size - 1, end_id)
    return next_id, final_id


def run_scan(start_url: str | None = None, start_id: int | None = None, end_id: int | None = None, batch_size: int | None = None):
    if not current_app.config.get("SEQUENTIAL_SCAN_ENABLED", True):
        raise RuntimeError("A varredura sequencial está desabilitada.")

    batch_size = batch_size or current_app.config["SCAN_BATCH_SIZE"]

    auto_range = start_id is None and end_id is None
    if auto_range:
        next_range = get_next_range(batch_size, current_app.config["SEQUENCE_START_ID"], current_app.config["SEQUENCE_END_ID"])
        if not next_range:
            return {
                "mode": "sequencial",
                "message": "Faixa final já processada.",
                "processed_ids": 0,
                "statuses": {},
                "items": [],
            }
        start_id, end_id = next_range
    else:
        start_id = start_id if start_id is not None else current_app.config["SEQUENCE_START_ID"]
        end_id = end_id if end_id is not None else min(start_id + batch_size - 1, current_app.config["SEQUENCE_END_ID"])

    results = {
        "mode": "sequencial",
        "url_template": current_app.config["SEQUENTIAL_URL_TEMPLATE"],
        "start_id": start_id,
        "end_id": end_id,
        "processed_ids": 0,
        "statuses": {"processado": 0, "sem_cadastro": 0, "pdf_nao_encontrado": 0, "erro": 0},
        "items": [],
    }

    for sequence_id in range(start_id, end_id + 1):
        item = process_sequence_id(sequence_id)
        results["items"].append(item)
        results["processed_ids"] += 1
        status = item["status"]
        results["statuses"][status] = results["statuses"].get(status, 0) + 1

    return results
