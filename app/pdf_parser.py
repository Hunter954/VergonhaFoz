import re
import fitz


def normalize_spaces(value: str | None) -> str | None:
    if value is None:
        return None
    value = re.sub(r"\s+", " ", value).strip(" :-\n\t")
    return value or None


def find_first(patterns, text):
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
        if match:
            return normalize_spaces(match.group(1))
    return None


def extract_text_from_pdf(file_path: str) -> str:
    doc = fitz.open(file_path)
    chunks = []
    for page in doc:
        chunks.append(page.get_text("text"))
    return "\n".join(chunks)


def parse_property_pdf(file_path: str) -> dict:
    text = extract_text_from_pdf(file_path)

    phone_matches = re.findall(r"(?:\+?55\s*)?(?:\(?\d{2}\)?\s*)?(?:9?\d{4})-?\d{4}", text)
    phone_matches = [normalize_spaces(x) for x in phone_matches if normalize_spaces(x)]

    data = {
        "nome": find_first([
            r"(?:Nome|Contribuinte|Propriet[áa]rio(?:\(a\))?)\s*[:\-]\s*(.+)",
        ], text),
        "cpf": find_first([
            r"\bCPF\s*[:\-]\s*([0-9\.\-\*/]+)",
            r"CPF/CNPJ\s*[:\-]\s*([0-9\.\-/]+)",
        ], text),
        "rg": find_first([
            r"\bRG\s*[:\-]\s*([A-Z0-9\.\-/]+)",
        ], text),
        "inscricao_imobiliaria": find_first([
            r"Inscri(?:ç|c)[aã]o Imobili[áa]ria\s*[:\-]\s*([A-Z0-9\.\-/]+)",
            r"Inscri(?:ç|c)[aã]o\s*[:\-]\s*([A-Z0-9\.\-/]+)",
        ], text),
        "endereco": find_first([
            r"Endere(?:ç|c)o\s*[:\-]\s*(.+)",
            r"Logradouro\s*[:\-]\s*(.+)",
        ], text),
        "numero": find_first([
            r"(?:N[uú]mero|N[º°])\s*[:\-]\s*([^\n]+)",
        ], text),
        "complemento": find_first([
            r"Complemento\s*[:\-]\s*(.+)",
        ], text),
        "bairro": find_first([
            r"Bairro\s*[:\-]\s*(.+)",
        ], text),
        "cidade": find_first([
            r"Cidade\s*[:\-]\s*(.+)",
            r"Munic[ií]pio\s*[:\-]\s*(.+)",
        ], text),
        "cep": find_first([
            r"\bCEP\s*[:\-]\s*([0-9\-\.]+)",
        ], text),
        "telefone_residencial": phone_matches[0] if phone_matches else None,
        "telefone_celular": phone_matches[1] if len(phone_matches) > 1 else None,
        "valor_venal_territorial": find_first([
            r"Valor Venal Territorial\s*R\$?\s*[:\-]?\s*([0-9\.,]+)",
            r"Valor Venal Territorial\s*[:\-]\s*R\$?\s*([0-9\.,]+)",
        ], text),
        "area_lote": find_first([
            r"[ÁA]rea do Lote\s*[:\-]\s*([0-9\.,m² ]+)",
        ], text),
        "tipo_lote": find_first([
            r"Tipo de Lote\s*[:\-]\s*(.+)",
        ], text),
        "matricula": find_first([
            r"Matr[ií]cula\s*[:\-]\s*([A-Z0-9\.\-/]+)",
        ], text),
        "area_construida": find_first([
            r"[ÁA]rea Constru[ií]da\s*[:\-]\s*([0-9\.,m² ]+)",
        ], text),
        "setor": find_first([
            r"Setor\s*[:\-]\s*([A-Z0-9\.\-/]+)",
        ], text),
        "quadra": find_first([
            r"Quadra\s*[:\-]\s*([A-Z0-9\.\-/]+)",
        ], text),
        "lote": find_first([
            r"Lote\s*[:\-]\s*([A-Z0-9\.\-/]+)",
        ], text),
        "exercicio": find_first([
            r"Exerc[ií]cio\s*[:\-]\s*(\d{4})",
        ], text),
        "texto_extraido": text,
    }

    return data
