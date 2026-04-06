#!/usr/bin/env python3
"""
Cria a estrutura de pastas do repositório mcmv_create_dataset.
Execute uma vez antes de rodar o agente principal.
"""

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

DIRS = [
    BASE_DIR / "src",
    BASE_DIR / "output" / "por_cidade",
    BASE_DIR / "cache" / "patterns",
    BASE_DIR / "logs",
]


def main():
    for d in DIRS:
        d.mkdir(parents=True, exist_ok=True)
        print(f"  ok  {d.relative_to(BASE_DIR)}")
    print("Estrutura criada.")


if __name__ == "__main__":
    main()
