import re
import fitz


def normalize_spaces(value: str | None) -> str | None:
    if value is None:
        return None
    value = re.sub(r"[ \t\r\f\v]+", " ", value)
    value = re.sub(r"\n+", " ", value)
    value = value.strip(" :-\n\t")
    return value or None


def normalize_inline(value: str | None) -> str | None:
    value = normalize_spaces(value)
    if value is None:
        return None
    value = re.sub(r"\s+/\s*$", "", value)
    value = re.sub(r"\s{2,}", " ", value)
    return value or None


def extract_text_from_pdf(file_path: str) -> str:
    doc = fitz.open(file_path)
    chunks = []
    for page in doc:
        text = page.get_text("text", sort=True)
        chunks.append(text)
    return "\n".join(chunks)


def clean_label_value(value: str | None) -> str | None:
    value = normalize_inline(value)
    if value is None:
        return None
    bad_values = {
        "-",
        "--",
        "NÃO INFORMADO",
        "NAO INFORMADO",
        "NÃO INFORMADA",
        "NAO INFORMADA",
        "N/I",
        "NI",
    }
    if value.upper() in bad_values:
        return None
    return value


def compile_stop_pattern(stop_labels: list[str]) -> str:
    escaped = [re.escape(label) for label in stop_labels if label]
    return "|".join(escaped) if escaped else r"$a"


def find_first(patterns, text):
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
        if match:
            return clean_label_value(match.group(1))
    return None


def extract_section(text: str, start_label: str, end_labels: list[str]) -> str:
    start = re.search(re.escape(start_label), text, flags=re.IGNORECASE)
    if not start:
        return ""
    start_idx = start.end()
    end_idx = len(text)
    for label in end_labels:
        end = re.search(re.escape(label), text[start_idx:], flags=re.IGNORECASE)
        if end:
            end_idx = min(end_idx, start_idx + end.start())
    return text[start_idx:end_idx]


def find_in_section(section: str, label: str, stop_labels: list[str]) -> str | None:
    if not section:
        return None
    stop_pattern = compile_stop_pattern(stop_labels)
    pattern = rf"{label}\s*:\s*(.*?)\s*(?=(?:{stop_pattern})\s*:|$)"
    match = re.search(pattern, section, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    return clean_label_value(match.group(1))


def find_name_in_contrib_section(section: str) -> str | None:
    patterns = [
        r"Nome\s*:\s*(?:\[[^\]]+\]\s*)?([A-ZÀ-Ý][A-ZÀ-Ý\s'\.-]+?)(?=\s+(?:CPF/CNPJ|RG|Tipo de Pessoa|Endere(?:ç|c)o|Complemento|Bairro|Cidade|CEP|Contato|Telefone|E-mail)\s*:|$)",
        r"Propriet[áa]rio\s+PRINCIPAL\s*\[[^\]]+\]\s*([A-ZÀ-Ý][A-ZÀ-Ý\s'\.-]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, section, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return clean_label_value(match.group(1))
    return None


def normalize_document(value: str | None) -> str | None:
    value = clean_label_value(value)
    if not value:
        return None
    match = re.search(r"[0-9][0-9\.\-/]+[0-9]", value)
    return match.group(0) if match else value


def normalize_money(value: str | None) -> str | None:
    value = clean_label_value(value)
    if not value:
        return None
    match = re.search(r"\d[\d\.]*,\d{2}", value)
    return match.group(0) if match else value


def normalize_area(value: str | None) -> str | None:
    value = clean_label_value(value)
    if not value:
        return None
    match = re.search(r"\d[\d\.]*,\d{2}\s*m[²2]?", value, flags=re.IGNORECASE)
    return match.group(0).replace("m2", "m²") if match else value


def extract_number_from_address(address: str | None) -> tuple[str | None, str | None]:
    address = clean_label_value(address)
    if not address:
        return None, None

    number = None
    patterns = [
        r",\s*N[º°o]?\s*(\d+[A-Za-z]?)",
        r",\s*n[º°o]?\s*(\d+[A-Za-z]?)",
        r",\s*(\d+[A-Za-z]?)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, address, flags=re.IGNORECASE)
        if match:
            number = match.group(1)
            break

    cleaned_address = re.sub(r",\s*N[º°o]?\s*\d+[A-Za-z]?", "", address, flags=re.IGNORECASE)
    cleaned_address = re.sub(r",\s*\d+[A-Za-z]?\b$", "", cleaned_address, flags=re.IGNORECASE)
    cleaned_address = cleaned_address.strip(" ,")
    return clean_label_value(cleaned_address), clean_label_value(number)


def parse_property_pdf(file_path: str) -> dict:
    text = extract_text_from_pdf(file_path)

    contrib_section = extract_section(
        text,
        "CONTRIBUINTE",
        ["TESTADAS / LOGRADOUROS", "TESTADAS/LOGRADOUROS", "INFORMAÇÕES DO TERRENO", "ISENÇÕES", "OBSERVAÇÕES"],
    )
    buro_section = extract_section(text, "INFORMAÇÕES BUROCRÁTICAS", ["INFORMAÇÕES GERAIS", "DIMENSÕES", "CONTRIBUINTE"])
    gerais_section = extract_section(text, "INFORMAÇÕES GERAIS", ["DIMENSÕES", "CONTRIBUINTE"])
    dimensoes_section = extract_section(text, "DIMENSÕES", ["Valor Venal Territorial", "CONTRIBUINTE"])

    nome = find_name_in_contrib_section(contrib_section)
    cpf = normalize_document(find_in_section(contrib_section, r"CPF/CNPJ", ["RG", "Tipo de Pessoa", "Endere(?:ç|c)o", "Complemento", "Bairro", "Cidade", "CEP", "Contato"]))
    rg = normalize_document(find_in_section(contrib_section, r"RG", ["Tipo de Pessoa", "Endere(?:ç|c)o", "Complemento", "Bairro", "Cidade", "CEP", "Contato", "Telefone"]))
    endereco_raw = find_in_section(contrib_section, r"Endere(?:ç|c)o", ["Complemento", "Bairro", "Cidade", "CEP", "Contato", "Telefone Residencial", "Telefone Celular", "E-mail", "Telefone Comercial", "Fone Fax"])
    endereco, numero = extract_number_from_address(endereco_raw)
    complemento = find_in_section(contrib_section, r"Complemento", ["Bairro", "Cidade", "CEP", "Contato", "Telefone Residencial", "Telefone Celular", "E-mail", "Telefone Comercial", "Fone Fax"])
    bairro = find_in_section(contrib_section, r"Bairro", ["Cidade", "CEP", "Contato", "Telefone Residencial", "Telefone Celular", "E-mail", "Telefone Comercial", "Fone Fax"])
    cidade = find_in_section(contrib_section, r"Cidade", ["CEP", "Contato", "Telefone Residencial", "Telefone Celular", "E-mail", "Telefone Comercial", "Fone Fax"])
    cep = find_in_section(contrib_section, r"CEP", ["Contato", "Telefone Residencial", "Telefone Celular", "E-mail", "Telefone Comercial", "Fone Fax"])
    telefone_residencial = normalize_document(find_in_section(contrib_section, r"Telefone Residencial", ["Telefone Celular", "E-mail", "Telefone Comercial", "Fone Fax"]))
    telefone_celular = normalize_document(find_in_section(contrib_section, r"Telefone Celular", ["E-mail", "Telefone Comercial", "Fone Fax"]))

    inscricao = normalize_document(find_first([
        r"Inscri(?:ç|c)[aã]o Imobili[áa]ria\s*:\s*([0-9\./-]+)",
    ], text))

    matricula = normalize_document(find_in_section(buro_section, r"Matr[ií]cula", ["Cart[óo]rio", "Livro", "Folha", "INCRA", "Caucionado", "Ano Al[ií]quota Progressiva"]))
    valor_venal_territorial = normalize_money(find_first([
        r"Valor Venal Territorial\s*R\$\s*([0-9\.,]+)",
        r"Valor Venal Territorial\s*:\s*R\$\s*([0-9\.,]+)",
    ], text))
    area_lote = normalize_area(find_in_section(dimensoes_section, r"[ÁA]rea do Lote", ["[ÁA]rea [ÚU]til do Lote", "[ÁA]rea Privativa", "[ÁA]rea Comum", "Profundidade"]))
    tipo_lote = find_in_section(gerais_section, r"Tipo de Lote", ["[ÁA]rea do Lote", "[ÁA]rea Privativa", "Valor Venal Territorial", "CONTRIBUINTE"])
    area_construida = normalize_area(find_in_section(dimensoes_section, r"[ÁA]rea Privativa", ["[ÁA]rea Comum", "Profundidade"]))

    setor = find_first([r"Setor\s*:\s*([0-9A-Z]+)"], text)
    quadra = find_first([r"Quadra\s*:\s*([0-9A-Z]+)"], text)
    lote = find_first([r"Lote\s*:\s*([0-9A-Z]+)"], text)
    exercicio = find_first([r"Exerc[ií]cio\s*:\s*(\d{4})"], text)

    # Fallbacks para PDFs com ordenação diferente no texto extraído.
    if not nome:
        nome = find_first([
            r"Nome\s*:\s*(?:\[[^\]]+\]\s*)?([A-ZÀ-Ý][A-ZÀ-Ý\s'\.-]+)",
            r"PROPRIET[ÁA]RIO\s+PRINCIPAL\s*\[[^\]]+\]\s*([A-ZÀ-Ý][A-ZÀ-Ý\s'\.-]+)",
        ], text)
    if not cpf:
        cpf = normalize_document(find_first([r"CPF/CNPJ\s*:\s*([0-9\./-]+)"], text))
    if not rg:
        rg = normalize_document(find_first([r"\bRG\s*:\s*([0-9\./-]+)"], text))
    if not cidade:
        cidade = find_first([r"Cidade\s*:\s*([^\n]+)"], text)
    if not bairro:
        bairro = find_first([r"Bairro\s*:\s*([^\n]+)"], text)
    if not cep:
        cep = normalize_document(find_first([r"CEP\s*:\s*([0-9\./-]+)"], text))
    if not endereco:
        endereco = find_first([r"Endere(?:ç|c)o\s*:\s*([^\n]+)"], text)
        endereco, numero_fallback = extract_number_from_address(endereco)
        numero = numero or numero_fallback
    if not area_lote:
        area_lote = normalize_area(find_first([r"[ÁA]rea do Lote\s*:\s*([0-9\.,]+\s*m[²2]?)"], text))
    if not tipo_lote:
        tipo_lote = find_first([r"Tipo de Lote\s*:\s*([^\n]+)"], text)
    if not matricula:
        matricula = normalize_document(find_first([r"Matr[ií]cula\s*:\s*([0-9\./-]+)"], text))
    if not telefone_residencial:
        telefone_residencial = normalize_document(find_first([r"Telefone Residencial\s*:\s*([0-9\(\)\s\-]+)"], text))
    if not telefone_celular:
        telefone_celular = normalize_document(find_first([r"Telefone Celular\s*:\s*([0-9\(\)\s\-]+)"], text))

    data = {
        "nome": clean_label_value(nome),
        "cpf": normalize_document(cpf),
        "rg": normalize_document(rg),
        "inscricao_imobiliaria": normalize_document(inscricao),
        "endereco": clean_label_value(endereco),
        "numero": clean_label_value(numero),
        "complemento": clean_label_value(complemento),
        "bairro": clean_label_value(bairro),
        "cidade": clean_label_value(cidade),
        "cep": normalize_document(cep),
        "telefone_residencial": normalize_document(telefone_residencial),
        "telefone_celular": normalize_document(telefone_celular),
        "valor_venal_territorial": normalize_money(valor_venal_territorial),
        "area_lote": normalize_area(area_lote),
        "tipo_lote": clean_label_value(tipo_lote),
        "matricula": normalize_document(matricula),
        "area_construida": normalize_area(area_construida),
        "setor": clean_label_value(setor),
        "quadra": clean_label_value(quadra),
        "lote": clean_label_value(lote),
        "exercicio": clean_label_value(exercicio),
        "texto_extraido": text,
    }

    return data
