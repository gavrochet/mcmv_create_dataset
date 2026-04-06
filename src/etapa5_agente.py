#!/usr/bin/env python3
"""
Etapa 5 — Extração Estruturada de Beneficiários MCMV
Usa Claude API para extrair dados de beneficiários dos PDFs filtrados em editais_match/.

Fluxo por cidade:
  1. Discovery: analisa PDFs com >50 matches para descobrir o formato do edital
  2. Extraction: extrai dados de cada PDF usando os padrões descobertos
  3. Recheck: re-processa cidades a cada CHECK_INTERVAL_HOURS (ou se houver PDFs novos)

Saída: output/por_cidade/{cidade}.csv (um registro por beneficiário)
Estado: cache/state.json (timestamps, arquivos processados, padrões descobertos)
"""

import argparse
import json
import logging
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import anthropic
import fitz
import pandas as pd

# ─── Caminhos ────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parents[1]
EDITAIS_MATCH_DIR = Path("G:/My Drive/Claude/mcmv_extract_flags/editais_match")
MATCHES_CSV       = Path("G:/My Drive/Claude/mcmv_extract_flags/logs/matches.csv")
EMPREENDIMENTOS_CSV = Path("G:/My Drive/Claude/mcmv_edital_download/input/empreendimentos.csv")
OUTPUT_DIR  = BASE_DIR / "output" / "por_cidade"
CACHE_DIR   = BASE_DIR / "cache"
LOGS_DIR    = BASE_DIR / "logs"
STATE_FILE  = CACHE_DIR / "state.json"

# ─── Parâmetros ──────────────────────────────────────────────────────────────

CHECK_INTERVAL_HOURS  = 4     # re-processar cidade se >= 4h desde o último run
HIGH_MATCH_THRESHOLD  = 50    # PDFs com mais matches são usados na descoberta
DISCOVERY_SAMPLE_SIZE = 3     # quantos PDFs de alta frequência usar na descoberta
MAX_CHARS_PER_PAGE    = 15000 # máximo de caracteres enviados por página ao Claude
MODEL = "claude-sonnet-4-6"

OUTPUT_COLUMNS = [
    "cpf", "nis", "nome", "identificador_original",
    "status", "posicao_lista", "grupo_criterio", "subgrupo",
    "empreendimento", "ano_empreendimento",
    "cidade", "estado", "arquivo_origem", "pagina_origem",
]

# ─── Logging ─────────────────────────────────────────────────────────────────

LOGS_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "pipeline.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ─── Prompts ─────────────────────────────────────────────────────────────────

DISCOVERY_PROMPT = """\
Você está analisando documentos do programa habitacional MCMV (Minha Casa Minha Vida)
para a cidade: {city}.

A seguir estão trechos de {n} documentos que tiveram o maior número de beneficiários identificados.
Seu objetivo é descobrir como esses documentos organizam as informações para que possamos
extrair dados estruturados de beneficiários.

Empreendimentos âncora desta cidade:
{anchor_json}

--- DOCUMENTOS ---
{doc_texts}
--- FIM ---

Analise os trechos e retorne APENAS um objeto JSON (sem texto antes ou depois) com:
{{
  "formato_geral": "descrição do formato predominante",
  "estrutura": "tabular|lista_numerada|lista_simples|mista",
  "grupos_criterios": {{
    "como_aparecem": "cabeçalho|texto_precedente|coluna_tabela|misto|ausente",
    "exemplos_encontrados": ["lista dos grupos/critérios encontrados literalmente"],
    "observacoes": "como identificar o grupo de cada beneficiário"
  }},
  "status": {{
    "sorteado": ["termos/marcadores que indicam sorteado"],
    "inscrito_nao_sorteado": ["termos/marcadores"],
    "reserva": ["termos/marcadores"],
    "observacoes": "como distinguir status entre beneficiários"
  }},
  "posicao_lista": {{
    "presente": true,
    "formato": "descrição de como o número de ordem aparece"
  }},
  "identificadores": {{
    "cpf_formatado": true,
    "cpf_raw": false,
    "nis": false,
    "nome_presente": true
  }},
  "observacoes_extras": "quaisquer outros padrões relevantes para extração"
}}
"""

EXTRACTION_PROMPT = """\
Você está extraindo dados de beneficiários de um documento MCMV.

Cidade: {city}
Arquivo: {filename}
Página: {page_num}

Padrões de formato descobertos para esta cidade:
{patterns_json}

Empreendimentos âncora desta cidade:
{anchor_json}

--- TEXTO DA PÁGINA ---
{page_text}
--- FIM ---

Extraia TODOS os beneficiários mencionados nesta página.
Regras críticas:
- NUNCA agregue grupos distintos (Idosos, PCD, Lista Geral etc.) em um único campo.
- Capture grupo_criterio EXATAMENTE como aparece no texto — não normalize, não traduza.
- Sorteados, inscritos não sorteados e reserva são categorias distintas — nunca misture.
- Se houver subgrupos dentro de um grupo, capture em "subgrupo".
- Se grupo for ambíguo, use grupo_criterio = "AMBIGUO" e coloque o trecho relevante em subgrupo.

Para cada beneficiário, retorne um objeto com:
{{
  "cpf": "11 dígitos sem pontuação, ou null",
  "nis": "11 dígitos, ou null",
  "nome": "nome completo ou null",
  "identificador_original": "exatamente como aparece no texto",
  "status": "sorteado|inscrito_nao_sorteado|reserva|indefinido",
  "posicao_lista": número inteiro ou null,
  "grupo_criterio": "texto exato do grupo/critério ou 'AMBIGUO'",
  "subgrupo": "texto exato do subgrupo ou null",
  "empreendimento": "nome do empreendimento âncora mais provável ou 'INDEFINIDO'",
  "ano_empreendimento": "ano (4 dígitos) ou null"
}}

Retorne APENAS um array JSON. Se não houver beneficiários nesta página, retorne [].
"""

# ─── Estado ──────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {"cities": {}}


def save_state(state: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ─── Dados de entrada ────────────────────────────────────────────────────────

def load_matches_info() -> pd.DataFrame:
    if not MATCHES_CSV.exists():
        return pd.DataFrame(columns=["cidade", "arquivo_pdf", "n_matches_no_arquivo"])
    df = pd.read_csv(MATCHES_CSV, dtype=str)
    df["n_matches_no_arquivo"] = pd.to_numeric(df["n_matches_no_arquivo"], errors="coerce").fillna(0)
    return df


def load_anchor_map() -> dict[str, list[dict]]:
    """Retorna {city_folder: [{empreendimento, ano}]}."""
    if not EMPREENDIMENTOS_CSV.exists():
        return {}
    df = pd.read_csv(EMPREENDIMENTOS_CSV, dtype=str)
    result: dict[str, list[dict]] = {}
    for _, row in df.iterrows():
        uf  = str(row.get("UF", "")).strip()
        mun = str(row.get("Nome_Municipio", "")).strip().replace(" ", "_")
        folder = f"{uf}_{mun}"
        ano = str(row.get("mode_sign_date", "") or "")[:4]
        entry = {
            "empreendimento": str(row.get("Empreendimento", "NA")),
            "ano": ano if ano.isdigit() else "NA",
        }
        result.setdefault(folder, []).append(entry)
    return result


# ─── PDF ─────────────────────────────────────────────────────────────────────

def extract_pages_text(pdf_path: Path) -> list[str]:
    """Retorna lista de textos por página (0-based)."""
    try:
        doc = fitz.open(pdf_path)
        try:
            return [page.get_text() for page in doc]
        finally:
            doc.close()
    except Exception as e:
        log.warning(f"  Erro ao abrir {pdf_path.name}: {e}")
        return []


# ─── Helpers Claude ──────────────────────────────────────────────────────────

def _extract_json_object(text: str) -> dict:
    start = text.find("{")
    end   = text.rfind("}") + 1
    if start == -1 or end == 0:
        raise ValueError("Nenhum objeto JSON encontrado na resposta.")
    return json.loads(text[start:end])


def _extract_json_array(text: str) -> list:
    start = text.find("[")
    end   = text.rfind("]") + 1
    if start == -1 or end == 0:
        return []
    return json.loads(text[start:end])


def _normalize_id(raw: str | None) -> str | None:
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    return digits.zfill(11) if digits else None


# ─── Discovery ───────────────────────────────────────────────────────────────

def discover_patterns(
    city: str,
    sample: list[tuple[Path, int]],
    anchor_info: list[dict],
    client: anthropic.Anthropic,
) -> dict:
    """
    Analisa PDFs de amostra e retorna dicionário de padrões de formato.
    sample: lista de (pdf_path, n_matches), ordenada por n_matches desc.
    """
    doc_texts_parts = []
    for pdf_path, count in sample:
        pages = extract_pages_text(pdf_path)
        combined = "\n\n--- NOVA PÁGINA ---\n\n".join(pages[:12])
        combined = combined[:MAX_CHARS_PER_PAGE * 2]
        doc_texts_parts.append(f"=== {pdf_path.name} ({count} matches) ===\n{combined}")

    prompt = DISCOVERY_PROMPT.format(
        city=city,
        n=len(sample),
        anchor_json=json.dumps(anchor_info, ensure_ascii=False, indent=2),
        doc_texts="\n\n".join(doc_texts_parts),
    )

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        return _extract_json_object(response.content[0].text)
    except Exception as e:
        log.warning(f"[{city}] Falha na descoberta de padrões: {e}")
        return {"formato_geral": "desconhecido", "observacoes_extras": str(e)}


# ─── Extraction ──────────────────────────────────────────────────────────────

def extract_from_pdf(
    pdf_path: Path,
    city: str,
    patterns: dict,
    anchor_info: list[dict],
    client: anthropic.Anthropic,
) -> list[dict]:
    """Extrai registros de beneficiários de um PDF, página a página."""
    pages_text = extract_pages_text(pdf_path)
    all_rows: list[dict] = []
    uf, municipio = city.split("_", 1) if "_" in city else (city, city)

    for page_idx, text in enumerate(pages_text):
        if not text.strip():
            continue

        prompt = EXTRACTION_PROMPT.format(
            city=city,
            filename=pdf_path.name,
            page_num=page_idx + 1,
            patterns_json=json.dumps(patterns, ensure_ascii=False),
            anchor_json=json.dumps(anchor_info, ensure_ascii=False),
            page_text=text[:MAX_CHARS_PER_PAGE],
        )

        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )
            records = _extract_json_array(response.content[0].text)

            for rec in records:
                # Normaliza identificadores
                rec["cpf"] = _normalize_id(rec.get("cpf"))
                rec["nis"] = _normalize_id(rec.get("nis"))
                # Adiciona metadados
                rec["cidade"]         = municipio.replace("_", " ")
                rec["estado"]         = uf
                rec["arquivo_origem"] = pdf_path.name
                rec["pagina_origem"]  = page_idx + 1

            all_rows.extend(records)

        except Exception as e:
            log.warning(f"  [{city}] Erro pág {page_idx + 1} de {pdf_path.name}: {e}")

    return all_rows


# ─── Output ──────────────────────────────────────────────────────────────────

def save_rows(city: str, rows: list[dict]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"{city}.csv"
    df = pd.DataFrame(rows)
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[OUTPUT_COLUMNS]
    if out_path.exists():
        df.to_csv(out_path, mode="a", header=False, index=False, encoding="utf-8")
    else:
        df.to_csv(out_path, mode="w", header=True, index=False, encoding="utf-8")


# ─── Lógica de cidade ────────────────────────────────────────────────────────

def needs_processing(city: str, city_state: dict, pdfs: list[Path]) -> bool:
    """True se há PDFs novos ou se já passaram CHECK_INTERVAL_HOURS."""
    processed = city_state.get("processed_files", {})
    for p in pdfs:
        if processed.get(p.name) != str(p.stat().st_mtime):
            return True
    last_run = city_state.get("last_run")
    if not last_run:
        return True
    elapsed = datetime.now() - datetime.fromisoformat(last_run)
    return elapsed >= timedelta(hours=CHECK_INTERVAL_HOURS)


def process_city(
    city: str,
    state: dict,
    matches_df: pd.DataFrame,
    anchor_map: dict,
    client: anthropic.Anthropic,
) -> int:
    city_state = state["cities"].setdefault(city, {
        "last_run": None,
        "processed_files": {},
        "patterns": None,
    })

    pdf_dir = EDITAIS_MATCH_DIR / city
    if not pdf_dir.exists():
        return 0
    pdfs = sorted(pdf_dir.glob("*.pdf"))
    if not pdfs:
        return 0

    if not needs_processing(city, city_state, pdfs):
        log.info(f"[{city}] sem novidades — próximo check em {CHECK_INTERVAL_HOURS}h")
        return 0

    anchor_info = anchor_map.get(city, [])

    # ── Discovery ──────────────────────────────────────────────────────────
    if not city_state["patterns"]:
        log.info(f"[{city}] Fase de descoberta de padrões...")
        city_matches = (
            matches_df[matches_df["cidade"] == city]
            if not matches_df.empty else pd.DataFrame()
        )
        pdfs_with_counts: list[tuple[Path, int]] = []
        for p in pdfs:
            row = city_matches[city_matches["arquivo_pdf"] == p.name]
            count = int(row["n_matches_no_arquivo"].iloc[0]) if not row.empty else 0
            pdfs_with_counts.append((p, count))
        pdfs_with_counts.sort(key=lambda x: x[1], reverse=True)

        high = [(p, c) for p, c in pdfs_with_counts if c > HIGH_MATCH_THRESHOLD]
        sample = (high if high else pdfs_with_counts)[:DISCOVERY_SAMPLE_SIZE]

        city_state["patterns"] = discover_patterns(city, sample, anchor_info, client)
        log.info(f"[{city}] Padrão: {city_state['patterns'].get('formato_geral', '?')}")

    # ── Extraction ─────────────────────────────────────────────────────────
    processed = city_state["processed_files"]
    new_pdfs = [
        p for p in pdfs
        if processed.get(p.name) != str(p.stat().st_mtime)
    ]

    total_rows = 0
    for pdf_path in new_pdfs:
        log.info(f"  [{city}] Extraindo {pdf_path.name}...")
        rows = extract_from_pdf(pdf_path, city, city_state["patterns"], anchor_info, client)

        # Fallback sem padrões se retornou vazio
        if not rows and city_state["patterns"]:
            log.info(f"  [{city}] Sem resultados — tentando extração sem padrões...")
            rows = extract_from_pdf(pdf_path, city, {}, anchor_info, client)

        if rows:
            save_rows(city, rows)
            total_rows += len(rows)
            log.info(f"  [{city}] {pdf_path.name}: {len(rows)} registros")
        else:
            log.info(f"  [{city}] {pdf_path.name}: nenhum registro")

        processed[pdf_path.name] = str(pdf_path.stat().st_mtime)

    city_state["last_run"] = datetime.now().isoformat()
    return total_rows


# ─── Loop principal ───────────────────────────────────────────────────────────

def run_once(state: dict, matches_df: pd.DataFrame, anchor_map: dict, client: anthropic.Anthropic) -> None:
    cities = [d.name for d in sorted(EDITAIS_MATCH_DIR.iterdir()) if d.is_dir()]
    if not cities:
        log.warning(f"Nenhuma cidade encontrada em {EDITAIS_MATCH_DIR}")
        return

    log.info(f"=== Ciclo de extração — {len(cities)} cidade(s) ===")
    total = 0
    for city in cities:
        try:
            n = process_city(city, state, matches_df, anchor_map, client)
            total += n
        except Exception as e:
            log.error(f"[{city}] Erro inesperado: {e}", exc_info=True)
        finally:
            save_state(state)

    log.info(f"=== Ciclo concluído — {total} registros extraídos ===")


def main() -> None:
    parser = argparse.ArgumentParser(description="Etapa 5 — Extração de beneficiários MCMV")
    parser.add_argument(
        "--loop", action="store_true",
        help=f"Executa em loop contínuo, verificando cidades a cada {CHECK_INTERVAL_HOURS}h",
    )
    parser.add_argument(
        "--city", type=str, default=None,
        help="Processa apenas a cidade informada (ex: AC_RIO_BRANCO)",
    )
    args = parser.parse_args()

    client = anthropic.Anthropic()
    state  = load_state()

    matches_df = load_matches_info()
    anchor_map = load_anchor_map()
    log.info(f"Matches: {len(matches_df)} linhas | Âncoras: {len(anchor_map)} cidades")

    if args.city:
        # Modo cidade única
        log.info(f"=== Modo cidade única: {args.city} ===")
        # Força re-processamento zerando last_run da cidade
        state["cities"].setdefault(args.city, {})["last_run"] = None
        try:
            process_city(args.city, state, matches_df, anchor_map, client)
        finally:
            save_state(state)
        return

    if args.loop:
        log.info(f"Modo loop — recheck a cada {CHECK_INTERVAL_HOURS}h por cidade")
        while True:
            run_once(state, matches_df, anchor_map, client)
            # Recarrega dados externos a cada ciclo
            matches_df = load_matches_info()
            anchor_map = load_anchor_map()
            log.info(f"Aguardando 30min para próximo ciclo de varredura...")
            time.sleep(30 * 60)  # verifica novas cidades/PDFs a cada 30min
    else:
        run_once(state, matches_df, anchor_map, client)


if __name__ == "__main__":
    main()
