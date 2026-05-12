# 🪙 CryptoPipeline — Arquitetura Medallion no Databricks

Pipeline de dados de criptoativos construído no **Databricks** com a **Arquitetura Medallion (Bronze → Silver → Gold)**, utilizando Delta Lake, Auto Loader e Unity Catalog. Os dados são extraídos em tempo real da API pública do CoinGecko e transformados em insights analíticos prontos para dashboards.

---

## 📐 Arquitetura

```
CoinGecko API
     │
     ▼
┌─────────────┐
│   BRONZE    │  Ingestão bruta via Auto Loader → Delta Table
│  (Raw JSON) │  projetos.crypto.bronze_market_data
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   SILVER    │  Limpeza, tipagem, deduplicação → Delta Table
│  (Process)  │  projetos.crypto.silver_market_data
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    GOLD     │  Regras de negócio + métricas → Delta Table (MERGE/Upsert)
│ (Analítica) │  projetos.crypto.gold_market_data
└─────────────┘
```

---

## 📁 Estrutura do Projeto

```
CryptoPipeline/
├── bronze/
│   └── bronze_layer.ipynb      # Extração da API e ingestão via Auto Loader
├── silver/
│   └── silver_layer.ipynb      # Tratamento e padronização dos dados
└── gold/
    └── gold_layer.ipynb        # Regras de negócio e geração de insights
```

---

## 🔶 Camada Bronze

**Objetivo:** Extrair os dados brutos da API do CoinGecko e armazená-los sem transformações.

**O que faz:**
- Consome os **top 50 criptoativos por market cap** via API REST do CoinGecko
- Inclui variações de preço em **1h, 24h e 7 dias**
- Persiste o payload JSON em `dbfs:/Volumes/projetos/crypto/crypto_volume/raw/json/{data}/`
- Utiliza **Auto Loader** (`cloudFiles`) para ingestão incremental e automática dos arquivos JSON em streaming
- Grava na Delta Table com `trigger(availableNow=True)` — equivalente a um batch incremental
- Adiciona metadados de auditoria: `_file_path` e `_ingested_at`

**Tecnologias:** `requests`, `PySpark Structured Streaming`, `Auto Loader`, `Delta Lake`, `Unity Catalog`

---

## 🥈 Camada Silver

**Objetivo:** Transformar os dados brutos em uma tabela limpa, tipada e deduplicada.

**O que faz:**
- Lê da tabela Bronze e **explode o array `data`** (um registro por moeda)
- Seleciona e renomeia colunas com nomenclatura semântica em português
- Aplica **casting explícito** de tipos (`DoubleType`, `LongType`, `TimestampType`)
- Extrai a data do arquivo pelo nome do path via `regexp_extract`
- Remove **duplicatas** pela chave `(coin_id, extraido_em)`
- Grava com **particionamento por `extraido_em`** para leitura eficiente

**Colunas principais geradas:**

| Coluna | Descrição |
|---|---|
| `coin_id` | Identificador único da moeda |
| `preco_usd` | Preço atual em dólares |
| `market_cap_usd` | Capitalização de mercado |
| `volume_24h_usd` | Volume negociado em 24h |
| `variacao_pct_1h/24h/7d` | Variações percentuais |
| `ath_usd` / `atl_usd` | All-Time High e All-Time Low |
| `supply_circulante` | Supply em circulação |
| `processado_em` | Timestamp de processamento |

---

## 🥇 Camada Gold

**Objetivo:** Aplicar regras de negócio e gerar métricas analíticas prontas para consumo.

**O que faz:**
- Lê da Silver e enriquece cada registro com **17 novas colunas analíticas**
- Executa **MERGE (upsert)** para garantir idempotência — sem duplicatas ao re-executar
- Utiliza **Window Functions** para ranking dentro de categorias

**Regras de negócio implementadas:**

| # | Coluna | Lógica |
|---|---|---|
| 1 | `categoria_market_cap` | Large / Mid / Small / Micro Cap por faixa de market cap |
| 2 | `score_liquidez` | Volume 24h ÷ Market Cap (turnover diário) |
| 3 | `categoria_liquidez` | Alta / Média / Baixa Liquidez pelo score |
| 4 | `tendencia_curto_prazo` | Bullish / Bearish / Lateral pela variação 24h |
| 5 | `tendencia_medio_prazo` | Bullish / Bearish / Lateral pela variação 7d |
| 6 | `sinal_mercado` | Sinal composto (ex: Forte Alta, Queda de Curto Prazo) |
| 7 | `categoria_volatilidade` | Extremamente Volátil → Baixa Volatilidade |
| 8 | `distancia_ath_categoria` | Próximo do ATH → Bear Market Deep |
| 9 | `flag_supply_escasso` | Supply circulante próximo do máximo |
| 10 | `flag_stablecoin` | Preço entre US$ 0,98 e US$ 1,02 |
| 11 | `score_risco` | Pontuação aditiva de risco (0–10) |
| 12 | `categoria_risco` | Mínimo / Baixo / Moderado / Alto / Muito Alto |
| 13 | `oportunidade_compra` | Combinação de desconto, liquidez, tendência e market cap |
| 14 | `preco_brl` | Preço convertido para BRL |
| 15 | `market_cap_brl` | Market cap em BRL |
| 16 | `ranking_volume_na_categoria` | Rank de volume dentro de cada faixa de cap |
| 17 | `processado_gold_em` | Timestamp de auditoria da camada Gold |

---

## 🛠️ Tecnologias Utilizadas

| Tecnologia | Uso |
|---|---|
| **Databricks** | Plataforma de execução e orquestração |
| **Apache Spark (PySpark)** | Processamento distribuído |
| **Delta Lake** | Formato de tabela ACID com suporte a MERGE |
| **Auto Loader** | Ingestão incremental de arquivos JSON |
| **Unity Catalog** | Governança e organização das tabelas |
| **CoinGecko API** | Fonte de dados de mercado de criptoativos |
| **Python** | Linguagem principal dos notebooks |

---

## ⚙️ Configuração

### Pré-requisitos

- Workspace Databricks com Unity Catalog habilitado
- Catalog `projetos` e schema `crypto` criados
- Volume `crypto_volume` configurado em `projetos.crypto`
- Cluster com acesso à internet (para chamada à API do CoinGecko)

### Ordem de execução

```
1. bronze/bronze_layer.ipynb   → Extrai e ingere os dados brutos
2. silver/silver_layer.ipynb   → Trata e padroniza os dados
3. gold/gold_layer.ipynb       → Aplica regras de negócio
```

---

## 📊 Modelo de Dados (Gold)

A tabela Gold é a camada de consumo final. Cada linha representa **uma snapshot de um criptoativo** em um momento específico, enriquecida com classificações e scores prontos para uso em ferramentas como Power BI, Tableau ou dashboards nativos do Databricks.

---

## 📌 Observações

- O pipeline foi desenvolvido como projeto de aprendizado durante treinamento na plataforma Databricks
- A API do CoinGecko utilizada é a versão pública (sem autenticação), com limite de requisições
- O MERGE na Gold garante **idempotência**: re-executar o pipeline não duplica registros
