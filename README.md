# mcmv_create_dataset

**Etapa 5 do pipeline MCMV** — Extração estruturada de beneficiários a partir dos PDFs filtrados pela Etapa 4.

## O que este repositório faz

Usa a Claude API para ler os PDFs em `mcmv_extract_flags/editais_match/{cidade}/` e extrair,
de forma estruturada, todos os beneficiários listados nos editais de sorteio do programa
Minha Casa Minha Vida (sorteados, reservas e inscritos não sorteados).

O agente opera cidade a cidade, re-verificando cada uma a cada **4 horas** para capturar
eventuais novos PDFs.

## Contexto do pipeline

```
Etapa 1  mcmv_edital_download   — baixa os PDFs dos Diários Oficiais
Etapa 4  mcmv_extract_flags     — filtra PDFs com ≥ 10 matches de CPF/NIS
Etapa 5  mcmv_create_dataset    — ← você está aqui
```

## Estrutura de pastas

```
mcmv_create_dataset/
├── src/
│   ├── setup_estrutura.py   # cria as pastas necessárias
│   └── etapa5_agente.py     # agente principal de extração
├── output/
│   └── por_cidade/          # {cidade}.csv — um registro por beneficiário (não versionar)
├── cache/
│   └── state.json           # timestamps, arquivos processados, padrões por cidade (não versionar)
├── logs/
│   └── pipeline.log         # log de execução (não versionar)
├── requirements.txt
├── .gitignore
├── README.md
└── CLAUDE.md
```

## Entradas

| Caminho | Descrição |
|---|---|
| `mcmv_extract_flags/editais_match/{cidade}/*.pdf` | PDFs filtrados (páginas relevantes + margem) |
| `mcmv_extract_flags/logs/matches.csv` | Metadados de matches por arquivo (n_matches) |
| `mcmv_edital_download/input/empreendimentos.csv` | CSV âncora com empreendimentos e beneficiários |

## Saída

`output/por_cidade/{cidade}.csv` — uma linha por beneficiário, com as colunas:

| Coluna | Descrição |
|---|---|
| `cpf` | 11 dígitos sem pontuação |
| `nis` | 11 dígitos |
| `nome` | nome completo (se disponível) |
| `identificador_original` | exatamente como aparece no PDF |
| `status` | `sorteado` / `inscrito_nao_sorteado` / `reserva` / `indefinido` |
| `posicao_lista` | número de ordem na lista |
| `grupo_criterio` | texto exato do grupo/critério (ex: "Idosos", "Grupo 1") |
| `subgrupo` | subgrupo dentro do critério |
| `empreendimento` | nome do empreendimento âncora |
| `ano_empreendimento` | ano do empreendimento |
| `cidade` | nome do município |
| `estado` | UF |
| `arquivo_origem` | nome do PDF de origem |
| `pagina_origem` | página do PDF onde foi encontrado |

## Como rodar

```bash
pip install -r requirements.txt

# 1. Cria estrutura de pastas (apenas na primeira vez)
python src/setup_estrutura.py

# 2. Processa todas as cidades (execução única)
python src/etapa5_agente.py

# 3. Modo loop contínuo (verifica novas cidades/PDFs a cada 30min,
#    re-processa cada cidade após 4h)
python src/etapa5_agente.py --loop

# 4. Processa apenas uma cidade específica
python src/etapa5_agente.py --city AC_RIO_BRANCO
```

## Variáveis de ambiente

| Variável | Descrição |
|---|---|
| `ANTHROPIC_API_KEY` | chave da Claude API (obrigatória) |

## Lógica de re-verificação

- O arquivo `cache/state.json` guarda, por cidade: último horário de execução, arquivos já processados (por mtime) e os padrões de formato descobertos.
- Uma cidade é re-processada se: há PDFs novos/modificados **ou** já passaram ≥ 4 horas desde o último run.
- No modo `--loop`, o script varre todas as cidades a cada 30 min; cada cidade individualmente só é re-analisada quando devida.

## .gitignore

`output/`, `cache/` e `logs/` estão no `.gitignore` — nunca versionar dados.
