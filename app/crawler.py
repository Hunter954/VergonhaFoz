import hashlib
import json
import os
import re
from collections import deque
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from flask import current_app

from .extensions import db
from .models import PropertyRecord, PropertyScan, SourcePdf
from .pdf_parser import parse_property_pdf


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


def _headers(referer=None):
    headers = {"User-Agent": current_app.config["USER_AGENT"]}
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


def download_pdf(source_url: str, referer_url: str | None = None):
    timeout = current_app.config["CRAWL_TIMEOUT_SECONDS"]
    response = requests.get(source_url, headers=_headers(referer_url), timeout=timeout)
    response.raise_for_status()

    content_type = (response.headers.get("content-type") or "").lower()
    if "application/pdf" not in content_type and not response.content.startswith(b"%PDF"):
        raise ValueError(f"URL retornou conteúdo não-PDF: {content_type}")

    digest = sha256_of_bytes(response.content)
    existing = SourcePdf.query.filter_by(sha256=digest).first()
    if existing:
        return existing, False

    filename = f"{digest}.pdf"
    file_path = os.path.join(current_app.config["PDF_STORAGE_DIR"], filename)
    with open(file_path, "wb") as f:
        f.write(response.content)

    source = SourcePdf(
        source_url=source_url,
        page_url=referer_url,
        file_path=file_path,
        sha256=digest,
        file_size=len(response.content),
        status="baixado",
    )
    db.session.add(source)
    db.session.commit()
    return source, True


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
        if not links:
            update_scan(
                scan,
                status="pdf_nao_encontrado",
                no_cadastro_detected=False,
                error_message="Página carregou, mas nenhum PDF foi localizado.",
            )
            return {"sequence_id": sequence_id, "status": "pdf_nao_encontrado"}

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
        return {"sequence_id": sequence_id, "status": "processado", "pdf_url": pdf_url}
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
