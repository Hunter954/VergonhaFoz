# Coletor Sequencial de PDFs - Imóveis Foz

Aplicação Flask para:

- varrer uma sequência de URLs no formato `/imoveis/1/{id}?aba=debitos`
- detectar quando o portal retorna erro de cadastro inexistente
- pular automaticamente para o próximo ID
- localizar e baixar o PDF do imóvel quando existir
- salvar os PDFs em volume persistente
- extrair dados relevantes do PDF
- listar tudo em tabela com filtros
- exportar para XLSX
- manter log por ID com status da tentativa

## Arquitetura

- **Flask**: painel web e API
- **Flask-SQLAlchemy**: banco SQLite no volume
- **APScheduler**: varredura periódica
- **Requests + BeautifulSoup**: carregamento e descoberta de PDFs
- **PyMuPDF**: leitura do PDF
- **Pandas/OpenPyXL**: exportação Excel

## Status por ID

- `processado`
- `sem_cadastro`
- `pdf_nao_encontrado`
- `erro`

## Campos extraídos

- Nome
- CPF
- RG
- Inscrição Imobiliária
- Endereço
- Número
- Complemento
- Bairro
- Cidade
- CEP
- Telefone residencial
- Telefone celular
- Valor Venal Territorial
- Área do Lote
- Tipo de Lote
- Matrícula
- Área construída
- Setor
- Quadra
- Lote
- Exercício
- Texto extraído integral

## Variáveis de ambiente

- `SECRET_KEY`
- `DATA_DIR=/data`
- `DATABASE_URL=sqlite:////data/app.db` opcional
- `SEQUENTIAL_SCAN_ENABLED=true`
- `SEQUENTIAL_URL_TEMPLATE=https://governodigital.foz.pr.gov.br/governo-digital/contribuinte/imoveis/1/{id}?aba=debitos`
- `SEQUENCE_START_ID=1`
- `SEQUENCE_END_ID=5000`
- `SCAN_BATCH_SIZE=50`
- `SCAN_INTERVAL_MINUTES=30`
- `CRAWL_TIMEOUT_SECONDS=25`
- `USER_AGENT=...`

## Railway

1. Suba este projeto.
2. Crie um volume e monte em `/data`.
3. Configure as variáveis de ambiente.
4. Faça o deploy.
5. Rode lotes pequenos primeiro, como `1` até `100`.

## Observações

- Esta versão já trata o caso de IDs inexistentes e pula para o próximo.
- O detector procura a mensagem de erro do cadastro também no HTML/texto retornado.
- Se o portal só renderizar a mensagem ou o botão do PDF via JavaScript pesado, a próxima evolução é acoplar Playwright.
- Os regex de extração podem ser refinados quando você tiver alguns PDFs reais de amostra.

## Rodando localmente

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python run.py
```

Abra `http://localhost:5000`.
