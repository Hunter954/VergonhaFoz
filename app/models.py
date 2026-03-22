from datetime import datetime
from .extensions import db


class SourcePdf(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    source_url = db.Column(db.Text, nullable=False, unique=True)
    page_url = db.Column(db.Text, nullable=True)
    file_path = db.Column(db.Text, nullable=False)
    sha256 = db.Column(db.String(64), nullable=False, index=True)
    file_size = db.Column(db.Integer, nullable=True)
    fetched_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    last_processed_at = db.Column(db.DateTime, nullable=True)
    status = db.Column(db.String(40), default="baixado", nullable=False)
    error_message = db.Column(db.Text, nullable=True)

    property_record = db.relationship("PropertyRecord", back_populates="source_pdf", uselist=False)
    property_scan = db.relationship("PropertyScan", back_populates="source_pdf", uselist=False)


class PropertyScan(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sequence_id = db.Column(db.Integer, nullable=False, unique=True, index=True)
    target_url = db.Column(db.Text, nullable=False, unique=True)
    status = db.Column(db.String(40), nullable=False, default="pendente", index=True)
    attempts = db.Column(db.Integer, nullable=False, default=0)
    no_cadastro_detected = db.Column(db.Boolean, nullable=False, default=False)
    http_status = db.Column(db.Integer, nullable=True)
    page_title = db.Column(db.String(255), nullable=True)
    pdf_url = db.Column(db.Text, nullable=True)
    error_message = db.Column(db.Text, nullable=True)
    last_checked_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    source_pdf_id = db.Column(db.Integer, db.ForeignKey("source_pdf.id"), nullable=True, unique=True)

    source_pdf = db.relationship("SourcePdf", back_populates="property_scan")


class PropertyRecord(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    source_pdf_id = db.Column(db.Integer, db.ForeignKey("source_pdf.id"), nullable=False, unique=True)

    nome = db.Column(db.String(255), index=True)
    cpf = db.Column(db.String(32), index=True)
    rg = db.Column(db.String(64), index=True)
    inscricao_imobiliaria = db.Column(db.String(64), index=True)
    endereco = db.Column(db.String(255), index=True)
    numero = db.Column(db.String(64), index=True)
    complemento = db.Column(db.String(255), index=True)
    bairro = db.Column(db.String(128), index=True)
    cidade = db.Column(db.String(128), index=True)
    cep = db.Column(db.String(32), index=True)
    telefone_residencial = db.Column(db.String(32), index=True)
    telefone_celular = db.Column(db.String(32), index=True)
    matricula = db.Column(db.String(64), index=True)
    valor_venal_territorial = db.Column(db.String(64), index=True)
    area_lote = db.Column(db.String(64), index=True)
    tipo_lote = db.Column(db.String(128), index=True)
    area_construida = db.Column(db.String(64), index=True)
    setor = db.Column(db.String(64), index=True)
    quadra = db.Column(db.String(64), index=True)
    lote = db.Column(db.String(64), index=True)
    exercicio = db.Column(db.String(32), index=True)
    texto_extraido = db.Column(db.Text)
    raw_json = db.Column(db.JSON)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    source_pdf = db.relationship("SourcePdf", back_populates="property_record")
