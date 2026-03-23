# Coletor sequencial de imóveis - Foz (Playwright + Blob)

Esta versão captura PDF mesmo quando o portal gera um `blob:` após clicar na **impressora**.

## Por que dava `Executable doesn't exist`?

No Railway às vezes o cache de browsers do Playwright não fica disponível na imagem final.
Por isso esta versão faz **auto-instalação do Chromium** quando necessário (1ª execução) e grava em um caminho persistente.

## Variáveis recomendadas (Railway)

- `DATA_DIR=/data`
- **Crie um volume** montado em `/data`
- `PLAYWRIGHT_BROWSERS_PATH=/data/ms-playwright`
- `AUTO_INSTALL_PLAYWRIGHT_BROWSERS=true`

Varredura:
- `SEQUENTIAL_URL_TEMPLATE=https://governodigital.foz.pr.gov.br/governo-digital/contribuinte/imoveis/1/{id}?aba=debitos`
- `SEQUENCE_START_ID=1`
- `SEQUENCE_END_ID=5000`
- `SCAN_BATCH_SIZE=10`

Playwright:
- `PLAYWRIGHT_ENABLED=true`
- `PLAYWRIGHT_HEADLESS=true`
- `PLAYWRIGHT_TIMEOUT_MS=45000`
- `PLAYWRIGHT_WAIT_AFTER_PRINT_MS=4000`

User-Agent:
- `USER_AGENT=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36`

## Fluxo

1) abre a página do imóvel
2) detecta `sem cadastro`
3) tenta achar link direto de PDF
4) se não achar: Playwright → clica impressora → captura PDF por rede/blob/download
5) salva o PDF em `/data/pdfs`
6) extrai campos e salva no banco


## Persistência recomendada

- PDFs: use um volume montado em `/data`
- Banco: use `DATABASE_URL` do Postgres no Railway

### Variáveis importantes

```
DATA_DIR=/data
DATABASE_URL=postgresql://...
PLAYWRIGHT_ENABLED=true
PLAYWRIGHT_HEADLESS=true
SEQUENTIAL_URL_TEMPLATE=https://governodigital.foz.pr.gov.br/governo-digital/contribuinte/imoveis/1/{id}?aba=debitos
```

Observação: se `DATABASE_URL` não existir, o app faz fallback para SQLite em `/data/app.db`.
