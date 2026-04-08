#!/usr/bin/env python3
"""
Extrator específico para AC_RIO_BRANCO.

O Diário Oficial do Acre publica listas MCMV no seguinte formato tabular:
  ORDEM
  CONG
  NOME (caps)
  CPF formatado (xxx.xxx.xxx-xx)
  QD
  LT

O cabeçalho da seção (portaria SEHAB) informa: empreendimento, status e entrega.
Este extrator usa regex diretamente, sem chamar o Claude CLI.
"""

import csv
import re
import sys
from pathlib import Path

import fitz
import pandas as pd

# ─── Caminhos ────────────────────────────────────────────────────────────────

BASE_DIR    = Path(__file__).resolve().parents[1]
PDF_DIR     = Path("G:/My Drive/Claude/mcmv_extract_flags/editais_match/AC_RIO_BRANCO")
EMPREND_CSV = Path("G:/My Drive/Claude/mcmv_edital_download/input/empreendimentos.csv")
OUTPUT_DIR  = BASE_DIR / "output" / "por_cidade"
OUTPUT_CSV  = OUTPUT_DIR / "AC_RIO_BRANCO.csv"

CITY    = "RIO BRANCO"
STATE   = "AC"
FOLDER  = "AC_RIO_BRANCO"

OUTPUT_COLUMNS = [
    "cpf", "nis", "nome", "identificador_original",
    "status", "posicao_lista", "grupo_criterio", "subgrupo",
    "empreendimento", "ano_empreendimento",
    "cidade", "estado", "arquivo_origem", "pagina_origem",
]

# ─── Padrões regex ────────────────────────────────────────────────────────────

# Linha de CPF formatado
CPF_LINE = re.compile(r"^(\d{3}\.\d{3}\.\d{3}-\d{2})\s*$", re.MULTILINE)
# Cabeçalho de portaria SEHAB com empreendimento
SEHAB_HEADER = re.compile(
    r"PORTARIA[^\n]*SEHAB[^\n]*\n.*?empreendimento\s+([A-ZÁÉÍÓÚÂÊÔÇÃÕ][^\n\-–—]+)",
    re.IGNORECASE | re.DOTALL
)
# Título da lista (grupo/critério)
LIST_TITLE = re.compile(
    r"LISTA\s+(?:DOS?\s+)?(?:BENEFICI[ÁA]RIOS?|SORTEADOS?|INSCRITOS?|RESERVAS?)[^\n]*",
    re.IGNORECASE
)
# Status a partir do título
STATUS_MAP = [
    (re.compile(r"SORTEADO|CONTEMPLADO|BENEFICI[ÁA]RIO", re.I), "sorteado"),
    (re.compile(r"RESERVA", re.I), "reserva"),
    (re.compile(r"INSCRITO", re.I), "inscrito_nao_sorteado"),
]
# Nomes em caps (pelo menos 2 palavras, sem dígitos)
NAME_PAT = re.compile(r"^([A-ZÁÉÍÓÚÂÊÔÇÃÕ][A-ZÁÉÍÓÚÂÊÔÇÃÕa-záéíóúâêôçãõ\s]{5,})$")
# Número de ordem
ORDER_PAT = re.compile(r"^\d{1,4}$")
# Código de inscrição MCMV (ex: CN-355, AA-13, BS-43A, SEDS, TQ-200)
CODE_PAT  = re.compile(r"^[A-Z]{2,4}[-][\w]+$|^SEDS$|^INQ\d*$")
# Padrões que indicam contexto NÃO-MCMV (processo seletivo, aposentadoria etc.)
NOT_MCMV  = re.compile(
    r"PROCESSO SELETIVO|CLASSIFICA[ÇC][ÃA]O|PONTOS|CARGO\s+\d|INSCRI[ÇC][ÃA]O\s+N[oº°]\s*\d"
    r"|APOSENT|MATR[IÍ]CULA\s+\d+[-]",
    re.IGNORECASE
)


def normalize_cpf(cpf_str: str) -> str:
    return re.sub(r"\D", "", cpf_str).zfill(11)


def detect_status(title: str) -> str:
    for pat, status in STATUS_MAP:
        if pat.search(title):
            return status
    return "indefinido"


def load_anchor_map() -> dict:
    """Retorna {empreendimento_upper: (empreendimento_orig, ano)}."""
    if not EMPREND_CSV.exists():
        return {}
    df = pd.read_csv(EMPREND_CSV, dtype=str)
    result = {}
    for _, row in df.iterrows():
        uf  = str(row.get("UF", "")).strip()
        mun = str(row.get("Nome_Municipio", "")).strip()
        if uf != "AC" or "RIO BRANCO" not in mun.upper():
            continue
        emp = str(row.get("Empreendimento", "")).strip()
        ano = str(row.get("mode_sign_date", "") or "")[:4]
        if emp:
            result[emp.upper()] = (emp, ano if ano.isdigit() else "NA")
    return result


def best_empreendimento(text: str, anchor_map: dict) -> tuple[str, str]:
    """Tenta identificar o empreendimento pelo texto de contexto."""
    text_upper = text.upper()
    for key, (emp, ano) in anchor_map.items():
        if key in text_upper:
            return emp, ano
    return "INDEFINIDO", "NA"


def parse_page(lines: list[str], page_num: int, context: dict, anchor_map: dict) -> list[dict]:
    """
    Varre as linhas de uma página e extrai registros de beneficiários.
    context é atualizado com portaria/grupo encontrado na página.
    """
    records = []

    full_text = "\n".join(lines)

    # Descarta páginas com indicadores de conteúdo não-MCMV
    if NOT_MCMV.search(full_text):
        return []

    # Exige ao menos um código CONG como indicador positivo de lista MCMV
    # (padrão como CN-355, AA-13, BH-50A, SEDS, TQ-200 etc.)
    CONG_POSITIVE = re.compile(r"^(?:[A-Z]{2,4}-[\w]+|SEDS)\s*$", re.MULTILINE)
    if not CONG_POSITIVE.search(full_text) and "SEHAB" not in full_text.upper():
        return []

    # Atualiza contexto se houver novo cabeçalho de portaria
    sehab_m = SEHAB_HEADER.search(full_text)
    if sehab_m:
        context["empreendimento_text"] = sehab_m.group(1).strip()
        emp_name, emp_ano = best_empreendimento(sehab_m.group(1), anchor_map)
        context["empreendimento"] = emp_name
        context["ano"] = emp_ano

    title_m = LIST_TITLE.search(full_text)
    if title_m:
        context["grupo_criterio"] = title_m.group(0).strip()
        context["status"] = detect_status(title_m.group(0))

    # Parse linha a linha buscando o padrão: ORDEM / CÓDIGO / NOME / CPF / QD / LT
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        # Detecta CPF formatado
        cpf_m = CPF_LINE.match(line)
        if cpf_m:
            cpf_raw = cpf_m.group(1)
            cpf_norm = normalize_cpf(cpf_raw)

            # Tenta recuperar as linhas anteriores: nome (i-1), código (i-2), ordem (i-3)
            nome       = lines[i-1].strip() if i >= 1 else ""
            code       = lines[i-2].strip() if i >= 2 else ""
            order_str  = lines[i-3].strip() if i >= 3 else ""

            # Valida: nome deve ter pelo menos 5 chars e não ser número
            if not nome or not re.search(r"[A-Za-záéíóúâêôçãõÁÉÍÓÚÂÊÔÇÃÕ]{3}", nome):
                i += 1
                continue

            # Descarta CPFs em contexto de portaria (CPF de servidor público)
            # Indicador: linha anterior contém "matrícula" ou "CPF" em texto corrido
            context_lines = "\n".join(lines[max(0,i-5):i])
            if re.search(r"matr[íi]cula|CPF\s+\d|inscri[çc][ãa]o\s+n[oº]", context_lines, re.I):
                i += 1
                continue

            try:
                posicao = int(order_str) if ORDER_PAT.match(order_str) else None
            except ValueError:
                posicao = None

            record = {
                "cpf": cpf_norm,
                "nis": None,
                "nome": nome,
                "identificador_original": cpf_raw,
                "status": context.get("status", "indefinido"),
                "posicao_lista": posicao,
                "grupo_criterio": context.get("grupo_criterio", "AMBIGUO"),
                "subgrupo": None,
                "empreendimento": context.get("empreendimento", "INDEFINIDO"),
                "ano_empreendimento": context.get("ano", "NA"),
                "cidade": CITY,
                "estado": STATE,
                "arquivo_origem": context.get("arquivo", ""),
                "pagina_origem": page_num,
            }
            records.append(record)
        i += 1

    return records


def extract_pdf(pdf_path: Path, anchor_map: dict) -> list[dict]:
    """Extrai todos os registros MCMV de um PDF."""
    doc = fitz.open(pdf_path)
    all_records = []
    context: dict = {"arquivo": pdf_path.name}

    SEHAB_PAT  = re.compile(r"SEHAB|Minha Casa|habitação|SORTEADO|BENEFICI|CONTEMPLA", re.IGNORECASE)
    CPF_PAT    = re.compile(r"\d{3}\.\d{3}\.\d{3}-\d{2}")

    # Identifica páginas com SEHAB e suas vizinhas com CPF
    pages_text = [p.get_text() for p in doc]
    doc.close()

    sehab_pages = {i for i, t in enumerate(pages_text) if SEHAB_PAT.search(t)}
    candidate: set[int] = set()
    for sp in sehab_pages:
        for j in range(max(0, sp - 1), min(len(pages_text), sp + 8)):
            if CPF_PAT.search(pages_text[j]) or j == sp:
                candidate.add(j)

    for page_idx in sorted(candidate):
        text  = pages_text[page_idx]
        lines = text.split("\n")
        recs  = parse_page(lines, page_idx + 1, context, anchor_map)
        all_records.extend(recs)

    return all_records


def dedup(records: list[dict]) -> list[dict]:
    """
    Remove duplicatas por CPF — portarias republicadas geram registros duplicados
    de beneficiários. Mantém o primeiro registro encontrado.
    """
    seen: set[str] = set()
    out: list[dict] = []
    for r in records:
        if r["cpf"] not in seen:
            seen.add(r["cpf"])
            out.append(r)
    return out


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    anchor_map = load_anchor_map()
    print(f"Âncoras carregadas: {len(anchor_map)}")

    all_records = []
    for pdf_path in sorted(PDF_DIR.glob("*.pdf")):
        recs = extract_pdf(pdf_path, anchor_map)
        print(f"  {pdf_path.name}: {len(recs)} registros")
        all_records.extend(recs)

    all_records = dedup(all_records)
    print(f"\nTotal antes dedup: {len(all_records)}")

    if not all_records:
        print("Nenhum registro encontrado.")
        return

    df = pd.DataFrame(all_records)
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[OUTPUT_COLUMNS]
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Salvo em {OUTPUT_CSV} ({len(df)} linhas)")


if __name__ == "__main__":
    main()
