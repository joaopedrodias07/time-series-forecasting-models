"""
main.py
-------
Ponto de entrada do pipeline ETL de temperatura da CETESB.
 
Uso:
    python main.py
    python main.py --pdf data/outro_arquivo.pdf --limiar 8
"""
 
import argparse
from pathlib import Path
 
import pandas as pd
 
from src.loader import extrair_cetesb_timeseries
from src.preprocessor import preparar_serie
 
 
# ---------------------------------------------------------------------------
# Configurações padrão
# ---------------------------------------------------------------------------
 
PDF_PATH    = Path("data/temperatura_ar_santos.pdf")
OUTPUT_PATH = Path("data/serie_tratada.csv")
LIMIAR      = 6  # horas — blocos <= LIMIAR recebem interpolação
 
 
# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
 
def run(pdf_path: Path, output_path: Path, limiar: int) -> pd.DataFrame:
    print(f"[1/3] Extraindo dados de: {pdf_path}")
    df = extrair_cetesb_timeseries(str(pdf_path))
    print(f"      {len(df):,} registros extraídos | "
          f"{df['temperatura'].isna().sum():,} nulos")
 
    print(f"[2/3] Pré-processando (limiar={limiar}h) ...")
    df = preparar_serie(df, limiar=limiar)
    print(f"      Nulos restantes: {df['temperatura_tratada'].isna().sum()}")
 
    print(f"[3/3] Salvando em: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df[["temperatura", "temperatura_tratada", "is_missing"]].to_csv(output_path)
    print("      Concluído.")
 
    return df
 
 
# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
 
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline ETL — CETESB Temperatura")
    parser.add_argument("--pdf",    type=Path, default=PDF_PATH,    help="Caminho do PDF")
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH, help="CSV de saída")
    parser.add_argument("--limiar", type=int,  default=LIMIAR,      help="Limiar de blocos NaN (horas)")
    args = parser.parse_args()
 
    run(args.pdf, args.output, args.limiar)