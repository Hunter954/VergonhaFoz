import base64
import hashlib
import json
import os
import re
import subprocess
from collections import deque
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from flask import current_app
from sqlalchemy.exc import IntegrityError

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


def _maybe_install_playwright_chromium(reason: str | None = None):
    if not current_app.config.get("AUTO_INSTALL_PLAYWRIGHT_BROWSERS", True):
        return
    try:
        marker = os.path.join(current_app.config.get("DATA_DIR", "/data"), ".playwright_chromium_installed")
        if os.path.exists(marker):
            return
        subprocess.run(["playwright", "install", "chromium"], check=True)
        with open(marker, "w") as f:
            f.write(str(reason or "installed"))
    except Exception:
        return


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
    "i.fas.ng-star-inserted.fa-print",
    "button i.fa-print",
    "a i.fa-print",
    "[title*='imprim' i]",
    "button[title*='imprim' i]",
    "a[title*='imprim' i]",
    "button:has(i.fa-print)",
    "a:has(i.fa-print)",
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
    filename = f"{digest}.pdf"
    file_path = os.path.join(current_app.config["PDF_STORAGE_DIR"], filename)

    os.makedirs(current_app.config["PDF_STORAGE_DIR"], exist_ok=True)
    if not os.path.exists(file_path):
        with open(file_path, "wb") as f:
            f.write(content)

    # 1) prioridade: mesma source_url já existe -> atualiza
    existing_by_url = SourcePdf.query.filter_by(source_url=source_url).first()
    if existing_by_url:
        existing_by_url.page_url = referer_url or existing_by_url.page_url
        existing_by_url.file_path = file_path
        existing_by_url.sha256 = digest
        existing_by_url.file_size = len(content)
        existing_by_url.status = status
        try:
            db.session.commit()
        except Exception:
            db.session.rollback()
            raise
        return existing_by_url, False

    # 2) fallback: mesmo arquivo já existe por hash -> reaproveita
    existing_by_sha = SourcePdf.query.filter_by(sha256=digest).first()
    if existing_by_sha:
        if referer_url and not existing_by_sha.page_url:
            existing_by_sha.page_url = referer_url
        if source_url and not existing_by_sha.source_url:
            existing_by_sha.source_url = source_url
        existing_by_sha.file_path = file_path
        existing_by_sha.file_size = len(content)
        existing_by_sha.status = status
        try:
            db.session.commit()
        except Exception:
            db.session.rollback()
            raise
        return existing_by_sha, False

    source = SourcePdf(
        source_url=source_url,
        page_url=referer_url,
        file_path=file_path,
        sha256=digest,
        file_size=len(content),
        status=status,
    )
    db.session.add(source)

    try:
        db.session.commit()
        return source, True
    except IntegrityError:
        db.session.rollback()

        # condição de corrida ou registro já inserido antes
        existing_by_url = SourcePdf.query.filter_by(source_url=source_url).first()
        if existing_by_url:
            existing_by_url.page_url = referer_url or existing_by_url.page_url
            existing_by_url.file_path = file_path
            existing_by_url.sha256 = digest
            existing_by_url.file_size = len(content)
            existing_by_url.status = status
            db.session.commit()
            return existing_by_url, False

        existing_by_sha = SourcePdf.query.filter_by(sha256=digest).first()
        if existing_by_sha:
            if referer_url and not existing_by_sha.page_url:
                existing_by_sha.page_url = referer_url
            if source_url and not existing_by_sha.source_url:
                existing_by_sha.source_url = source_url
            existing_by_sha.file_path = file_path
            existing_by_sha.file_size = len(content)
            existing_by_sha.status = status
            db.session.commit()
            return existing_by_sha, False

        raise


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

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise

    return record


def get_or_create_scan(sequence_id: int, target_url: str) -> PropertyScan:
    scan = PropertyScan.query.filter_by(sequence_id=sequence_id).first()
    if scan:
        return scan
    scan = PropertyScan(sequence_id=sequence_id, target_url=target_url, status="pendente")
    db.session.add(scan)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise
    return scan


def update_scan(scan: PropertyScan, **kwargs):
    for key, value in kwargs.items():
        setattr(scan, key, value)
    scan.updated_at = datetime.utcnow()
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise
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


def _iter_pages(context, page, popup=None):
    seen = []
    for candidate in [popup, page] + list(context.pages):
        if candidate and candidate not in seen:
            seen.append(candidate)
    return seen


def _iter_frames(context, page, popup=None):
    for pg in _iter_pages(context, page, popup):
        for frame in pg.frames:
            yield pg, frame


def _first_visible_in_frames(context, page, popup, selectors):
    for pg, frame in _iter_frames(context, page, popup):
        for selector in selectors:
            try:
                locator = frame.locator(selector)
                count = locator.count()
                for idx in range(min(count, 5)):
                    item = locator.nth(idx)
                    try:
                        if item.is_visible(timeout=800):
                            return pg, frame, item, selector
                    except Exception:
                        continue
            except Exception:
                continue
    return None, None, None, None


def _find_print_candidates(page):
    candidates = []
    for selector in PRINT_SELECTORS:
        try:
            locator = page.locator(selector)
            count = locator.count()
            for idx in range(min(count, 8)):
                item = locator.nth(idx)
                try:
                    if item.is_visible(timeout=800):
                        box = item.bounding_box() or {}
                        candidates.append((item, selector, box.get("x", 0), box.get("y", 0)))
                except Exception:
                    continue
        except Exception:
            continue
    candidates.sort(key=lambda x: (-x[2], x[3]))
    return [(a, b) for a, b, _, _ in candidates]


def _extract_blob_from_frame(frame):
    js = """async () => {
        const candidates = [];
        const pushIfBlob = (v) => { if (v && typeof v === 'string' && v.startsWith('blob:')) candidates.push(v); };
        document.querySelectorAll('a, iframe, embed, object').forEach(el => {
          pushIfBlob(el.href);
          pushIfBlob(el.src);
          pushIfBlob(el.data);
        });
        if (window.location.href && window.location.href.startsWith('blob:')) candidates.unshift(window.location.href);
        if (!candidates.length) return null;
        for (const url of candidates) {
          try {
            const res = await fetch(url);
            const buf = await res.arrayBuffer();
            let binary = '';
            const bytes = new Uint8Array(buf);
            const chunk = 0x8000;
            for (let i = 0; i < bytes.length; i += chunk) {
              binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
            }
            return { url, b64: btoa(binary) };
          } catch (e) {}
        }
        return null;
    }"""
    try:
        result = frame.evaluate(js)
        if result and result.get("b64"):
            return base64.b64decode(result["b64"]), result.get("url")
    except Exception:
        pass
    return None, None


def _extract_any_blob(context, page, popup=None):
    for _pg, frame in _iter_frames(context, page, popup):
        pdf_bytes, blob_url = _extract_blob_from_frame(frame)
        if pdf_bytes and pdf_bytes.startswith(b"%PDF"):
            return pdf_bytes, blob_url
    return None, None


def _click_best_effort(locator, timeout_ms):
    try:
        locator.click(timeout=timeout_ms)
        return True
    except Exception:
        try:
            locator.click(timeout=timeout_ms, force=True)
            return True
        except Exception:
            try:
                locator.dispatch_event("click")
                return True
            except Exception:
                return False


def _build_network_pdf_collector(context):
    bucket = []

    def on_response(response):
        try:
            headers = {k.lower(): v for k, v in response.headers.items()}
            ctype = (headers.get("content-type") or "").lower()
            url = response.url or ""
            if "application/pdf" in ctype or ".pdf" in url.lower() or any(x in url.lower() for x in ["relatorio", "espelho", "imprimir", "download"]):
                body = response.body()
                if body and body.startswith(b"%PDF"):
                    bucket.append((url, body))
        except Exception:
            pass

    context.on("response", on_response)
    return bucket


def _capture_pdf_via_playwright(target_url: str):
    if not PLAYWRIGHT_AVAILABLE:
        raise RuntimeError("Playwright não está instalado na aplicação.")

    timeout_ms = current_app.config["PLAYWRIGHT_TIMEOUT_MS"]
    wait_after_print_ms = current_app.config["PLAYWRIGHT_WAIT_AFTER_PRINT_MS"]
    use_headless = current_app.config["PLAYWRIGHT_HEADLESS"]

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(
                headless=use_headless,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            )
        except Exception as exc:
            msg = str(exc)
            if "Executable doesn" in msg or "playwright install" in msg or "Executable doesn't exist" in msg:
                _maybe_install_playwright_chromium(reason=msg[:200])
                browser = p.chromium.launch(
                    headless=use_headless,
                    args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
                )
            else:
                raise

        context = browser.new_context(
            accept_downloads=True,
            user_agent=current_app.config["USER_AGENT"],
            locale="pt-BR",
            viewport={"width": 1440, "height": 1100},
        )
        network_pdfs = _build_network_pdf_collector(context)
        page = context.new_page()
        page.goto(target_url, wait_until="domcontentloaded", timeout=timeout_ms)
        try:
            page.wait_for_load_state("networkidle", timeout=timeout_ms)
        except Exception:
            pass

        body_text = _page_text(page)
        if page_indicates_no_cadastro(body_text):
            browser.close()
            return {"status": "sem_cadastro", "message": "Não foi possível carregar os dados do cadastro."}

        popup = None
        last_error = None
        print_candidates = _find_print_candidates(page)
        if not print_candidates:
            browser.close()
            raise RuntimeError("Ícone/botão de impressão não encontrado na página.")

        for print_locator, print_selector in print_candidates[:5]:
            try:
                try:
                    with context.expect_page(timeout=2500) as popup_info:
                        ok = _click_best_effort(print_locator, timeout_ms)
                        if not ok:
                            raise RuntimeError("Falha ao clicar no botão de imprimir")
                    popup = popup_info.value
                    try:
                        popup.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
                    except Exception:
                        pass
                except Exception:
                    ok = _click_best_effort(print_locator, timeout_ms)
                    if not ok:
                        raise RuntimeError("Falha ao clicar no botão de imprimir")
                    page.wait_for_timeout(wait_after_print_ms)

                active_pages = _iter_pages(context, page, popup)
                for ap in active_pages:
                    try:
                        ap.wait_for_timeout(wait_after_print_ms)
                        ap.wait_for_load_state("networkidle", timeout=min(timeout_ms, 8000))
                    except Exception:
                        pass

                all_text = " ".join(_page_text(ap) for ap in active_pages)
                if page_indicates_no_cadastro(all_text):
                    browser.close()
                    return {"status": "sem_cadastro", "message": "Não foi possível carregar os dados do cadastro."}

                if network_pdfs:
                    url, body = network_pdfs[-1]
                    browser.close()
                    return {
                        "status": "processado",
                        "pdf_bytes": body,
                        "pdf_url": url,
                        "capture_method": f"playwright_network:{print_selector}",
                    }

                pdf_bytes, pdf_blob_url = _extract_any_blob(context, page, popup)
                if pdf_bytes and pdf_bytes.startswith(b"%PDF"):
                    browser.close()
                    return {
                        "status": "processado",
                        "pdf_bytes": pdf_bytes,
                        "pdf_url": pdf_blob_url or (popup.url if popup else page.url),
                        "capture_method": f"playwright_blob:{print_selector}",
                    }

                found_pg, found_frame, download_locator, download_selector = _first_visible_in_frames(context, page, popup, DOWNLOAD_SELECTORS)
                if download_locator:
                    try:
                        with found_pg.expect_download(timeout=timeout_ms) as download_info:
                            ok = _click_best_effort(download_locator, timeout_ms)
                            if not ok:
                                raise RuntimeError("Falha ao clicar no botão Baixar")
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
                        try:
                            found_pg.wait_for_timeout(1800)
                        except Exception:
                            pass

                    if network_pdfs:
                        url, body = network_pdfs[-1]
                        browser.close()
                        return {
                            "status": "processado",
                            "pdf_bytes": body,
                            "pdf_url": url,
                            "capture_method": f"playwright_network_after_download:{download_selector}",
                        }

                    pdf_bytes, pdf_blob_url = _extract_any_blob(context, page, popup)
                    if pdf_bytes and pdf_bytes.startswith(b"%PDF"):
                        browser.close()
                        return {
                            "status": "processado",
                            "pdf_bytes": pdf_bytes,
                            "pdf_url": pdf_blob_url or (popup.url if popup else page.url),
                            "capture_method": f"playwright_blob_after_download:{download_selector}",
                        }

                last_error = f"Cliquei em {print_selector}, mas não achei PDF nem botão Baixar utilizável."
            except Exception as exc:
                last_error = str(exc)
                continue

        browser.close()
        raise RuntimeError(last_error or "PDF não encontrado após clicar em imprimir/baixar.")


def process_sequence_id(sequence_id: int):
    target_url = current_app.config["SEQUENTIAL_URL_TEMPLATE"].format(id=sequence_id)
    scan = get_or_create_scan(sequence_id, target_url)
    scan.attempts += 1
    scan.last_checked_at = datetime.utcnow()

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise

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
            try:
                db.session.commit()
            except Exception:
                db.session.rollback()
                raise

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

            try:
                db.session.commit()
            except Exception:
                db.session.rollback()
                raise

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
        db.session.rollback()
        try:
            update_scan(scan, status="erro", error_message=str(exc))
        except Exception:
            db.session.rollback()
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
