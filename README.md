# Coletor sequencial de imóveis - Foz

Versão com captura de PDF por navegador automatizado.

## Como funciona

1. Varre URLs no formato `/imoveis/1/{id}?aba=debitos`
2. Se a página mostrar a mensagem de cadastro inexistente, marca `sem_cadastro`
3. Se não houver link direto de PDF no HTML, abre um navegador headless com Playwright
4. Clica no ícone de impressora
5. Espera o popup / visualizador do PDF
6. Tenta capturar o PDF por download ou lendo o `blob:` gerado no navegador
7. Salva o PDF em `/data/pdfs`
8. Extrai os campos e salva no banco

## Variáveis importantes

- `DATA_DIR=/data`
- `SEQUENTIAL_URL_TEMPLATE=https://governodigital.foz.pr.gov.br/governo-digital/contribuinte/imoveis/1/{id}?aba=debitos`
- `SEQUENCE_START_ID=1`
- `SEQUENCE_END_ID=5000`
- `SCAN_BATCH_SIZE=25`
- `SCAN_INTERVAL_MINUTES=30`
- `PLAYWRIGHT_ENABLED=true`
- `PLAYWRIGHT_HEADLESS=true`
- `PLAYWRIGHT_TIMEOUT_MS=30000`
- `PLAYWRIGHT_WAIT_AFTER_PRINT_MS=2500`
- `USER_AGENT=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36`

## Railway

Monte um volume em `/data`.

Essa versão instala o Chromium no build via Playwright.
