import io
import os

import pandas as pd
from flask import Blueprint, abort, current_app, jsonify, render_template, request, send_file, url_for
from sqlalchemy import or_

from .models import PropertyRecord, PropertyScan, SourcePdf
from .crawler import process_sequence_id, run_scan

bp = Blueprint("main", __name__)

FILTER_FIELDS = [
    "nome", "cpf", "rg", "inscricao_imobiliaria", "endereco", "bairro", "cidade", "cep",
    "telefone_residencial", "telefone_celular", "valor_venal_territorial", "area_lote",
    "tipo_lote", "matricula", "setor", "quadra", "lote", "exercicio"
]


def apply_record_filters(query):
    q = request.args.get("q", "").strip()
    if q:
        like = f"%{q}%"
        query = query.filter(or_(
            PropertyRecord.nome.ilike(like),
            PropertyRecord.cpf.ilike(like),
            PropertyRecord.rg.ilike(like),
            PropertyRecord.inscricao_imobiliaria.ilike(like),
            PropertyRecord.endereco.ilike(like),
            PropertyRecord.bairro.ilike(like),
            PropertyRecord.cidade.ilike(like),
            PropertyRecord.matricula.ilike(like),
        ))

    for field in FILTER_FIELDS:
        value = request.args.get(field, "").strip()
        if value:
            query = query.filter(getattr(PropertyRecord, field).ilike(f"%{value}%"))

    scan_status = request.args.get("scan_status", "").strip()
    if scan_status:
        query = query.join(SourcePdf.property_scan).filter(PropertyScan.status == scan_status)

    return query


def apply_scan_filters(query):
    sequence_from = request.args.get("sequence_from", "").strip()
    sequence_to = request.args.get("sequence_to", "").strip()
    scan_status = request.args.get("scan_status", "").strip()

    if sequence_from.isdigit():
        query = query.filter(PropertyScan.sequence_id >= int(sequence_from))
    if sequence_to.isdigit():
        query = query.filter(PropertyScan.sequence_id <= int(sequence_to))
    if scan_status:
        query = query.filter(PropertyScan.status == scan_status)

    return query


@bp.route("/")
def index():
    record_page = max(int(request.args.get("record_page", 1)), 1)
    scan_page = max(int(request.args.get("scan_page", 1)), 1)
    per_page = min(max(int(request.args.get("per_page", 25)), 10), 200)

    records_query = PropertyRecord.query.join(SourcePdf).order_by(PropertyRecord.updated_at.desc())
    records_query = apply_record_filters(records_query)
    records_pagination = records_query.paginate(page=record_page, per_page=per_page, error_out=False)

    scans_query = PropertyScan.query.order_by(PropertyScan.sequence_id.desc())
    scans_query = apply_scan_filters(scans_query)
    scans_pagination = scans_query.paginate(page=scan_page, per_page=per_page, error_out=False)

    last_scan = PropertyScan.query.order_by(PropertyScan.sequence_id.desc()).first()
    stats = {
        "records": PropertyRecord.query.count(),
        "pdfs": SourcePdf.query.count(),
        "processed": SourcePdf.query.filter_by(status="processado").count(),
        "scanned": PropertyScan.query.count(),
        "sem_cadastro": PropertyScan.query.filter_by(status="sem_cadastro").count(),
        "pdf_nao_encontrado": PropertyScan.query.filter_by(status="pdf_nao_encontrado").count(),
        "erros": PropertyScan.query.filter_by(status="erro").count(),
        "last_sequence": last_scan.sequence_id if last_scan else None,
    }
        # URLs de paginação (Jinja não aceita **kwargs dentro de url_for)
    base_args = request.args.to_dict(flat=True)

    def _url_with(**overrides):
        args = dict(base_args)
        for k, v in overrides.items():
            if v is None:
                args.pop(k, None)
            else:
                args[k] = str(v)
        return url_for('main.index', **args)

    record_prev_url = _url_with(record_page=records_pagination.prev_num) if records_pagination.has_prev else None
    record_next_url = _url_with(record_page=records_pagination.next_num) if records_pagination.has_next else None
    scan_prev_url = _url_with(scan_page=scans_pagination.prev_num) if scans_pagination.has_prev else None
    scan_next_url = _url_with(scan_page=scans_pagination.next_num) if scans_pagination.has_next else None

    return render_template(
        "index.html",
        records_pagination=records_pagination,
        scans_pagination=scans_pagination,
        stats=stats,
        record_prev_url=record_prev_url,
        record_next_url=record_next_url,
        scan_prev_url=scan_prev_url,
        scan_next_url=scan_next_url,
    )


@bp.route("/scan", methods=["POST"])
def scan_now():
    start_id_raw = request.form.get("start_id", "").strip()
    end_id_raw = request.form.get("end_id", "").strip()
    batch_size_raw = request.form.get("batch_size", "").strip()

    start_id = int(start_id_raw) if start_id_raw.isdigit() else current_app.config["SEQUENCE_START_ID"]
    end_id = int(end_id_raw) if end_id_raw.isdigit() else min(start_id + current_app.config["SCAN_BATCH_SIZE"] - 1, current_app.config["SEQUENCE_END_ID"])
    batch_size = int(batch_size_raw) if batch_size_raw.isdigit() else None

    if batch_size and not end_id_raw:
        end_id = min(start_id + batch_size - 1, current_app.config["SEQUENCE_END_ID"])

    result = run_scan(start_id=start_id, end_id=end_id, batch_size=batch_size)
    return render_template("scan_result.html", result=result)


@bp.route("/api/scan", methods=["POST"])
def scan_now_api():
    payload = request.get_json(silent=True) or {}
    start_id = payload.get("start_id")
    end_id = payload.get("end_id")
    batch_size = payload.get("batch_size")
    result = run_scan(start_id=start_id, end_id=end_id, batch_size=batch_size)
    return jsonify(result)


@bp.route("/scan/<int:sequence_id>/retry", methods=["POST"])
def retry_sequence(sequence_id):
    result = process_sequence_id(sequence_id)
    return render_template("scan_result.html", result=result)


@bp.route("/record/<int:record_id>")
def record_detail(record_id):
    record = PropertyRecord.query.get_or_404(record_id)
    return render_template("detail.html", record=record)


@bp.route("/pdf/<int:source_id>")
def get_pdf(source_id):
    source = SourcePdf.query.get_or_404(source_id)
    if not os.path.exists(source.file_path):
        abort(404)
    return send_file(source.file_path, mimetype="application/pdf", as_attachment=False)


@bp.route("/export.xlsx")
def export_xlsx():
    query = PropertyRecord.query.join(SourcePdf).outerjoin(SourcePdf.property_scan).order_by(PropertyRecord.updated_at.desc())
    query = apply_record_filters(query)
    rows = []
    for record in query.all():
        item = {c.name: getattr(record, c.name) for c in record.__table__.columns}
        item["pdf_url"] = request.host_url.rstrip("/") + f"/pdf/{record.source_pdf_id}"
        if record.source_pdf and record.source_pdf.property_scan:
            item["sequence_id"] = record.source_pdf.property_scan.sequence_id
            item["scan_status"] = record.source_pdf.property_scan.status
            item["target_url"] = record.source_pdf.property_scan.target_url
        rows.append(item)

    df = pd.DataFrame(rows)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="imoveis")
    output.seek(0)

    return send_file(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name="imoveis_extraidos.xlsx",
    )


@bp.route("/health")
def health():
    return jsonify({"status": "ok"})
