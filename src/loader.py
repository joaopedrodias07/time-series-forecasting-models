"""
loader.py
---------
Extração de dados horários de temperatura a partir de PDFs da CETESB.
"""
 
import re
import calendar
import pandas as pd
import pdfplumber
 
 
# Mapeamento de meses em português
MESES_MAP = {
    "Janeiro": 1, "Fevereiro": 2, "Março": 3, "Abril": 4,
    "Maio": 5, "Junho": 6, "Julho": 7, "Agosto": 8,
    "Setembro": 9, "Outubro": 10, "Novembro": 11, "Dezembro": 12,
}
 
 
def extrair_cetesb_timeseries(pdf_path: str) -> pd.DataFrame:
    """
    Extrai dados horários de temperatura da CETESB a partir de um PDF.
 
    Parâmetros
    ----------
    pdf_path : str
        Caminho para o arquivo PDF da CETESB.
 
    Retorna
    -------
    pd.DataFrame
        DataFrame com colunas:
        - datetime  : timestamp horário (pd.Timestamp)
        - temperatura: float ou NaN para valores ausentes ("-")
    """
    registros = []
 
    with pdfplumber.open(pdf_path) as pdf:
        for pagina in pdf.pages:
            texto = pagina.extract_text()
            if not texto:
                continue
 
            linhas = texto.split("\n")
            mes, ano = _detectar_mes_ano(linhas)
 
            if mes is None or ano is None:
                continue
 
            dias_no_mes = calendar.monthrange(ano, mes)[1]
 
            for linha in linhas:
                match = re.match(r"^(\d{1,2})\s+(.*)", linha)
                if not match:
                    continue
 
                dia = int(match.group(1))
                if dia > dias_no_mes:
                    continue
 
                valores = re.findall(r"-|\d+,\d+", linha)
 
                # Garantir exatamente 24 valores
                if len(valores) < 24:
                    valores += [None] * (24 - len(valores))
                else:
                    valores = valores[:24]
 
                for hora_idx, valor in enumerate(valores, start=1):
                    temp = None if (valor == "-" or valor is None) \
                        else float(valor.replace(",", "."))
 
                    data = pd.Timestamp(
                        year=ano,
                        month=mes,
                        day=dia,
                        hour=hora_idx - 1,
                    )
                    registros.append((data, temp))
 
    df = pd.DataFrame(registros, columns=["datetime", "temperatura"])
    df = df.groupby("datetime", as_index=False).mean()
    df = df.sort_values("datetime").reset_index(drop=True)
 
    return df
 
 
# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------
 
def _detectar_mes_ano(linhas: list[str]) -> tuple[int | None, int | None]:
    """Detecta mês e ano a partir das linhas de texto de uma página."""
    for linha in linhas:
        for nome_mes, num_mes in MESES_MAP.items():
            if nome_mes in linha:
                match_ano = re.search(r"(20\d{2})", linha)
                if match_ano:
                    return num_mes, int(match_ano.group(1))
    return None, None