"""
preprocessor.py
---------------
Preparação e tratamento de valores faltantes da série temporal de temperatura.
"""
 
import pandas as pd
 
 
# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------
 
def preparar_serie(df: pd.DataFrame, limiar: int = 6) -> pd.DataFrame:
    """
    Executa o pipeline completo de pré-processamento:
      1. Define índice temporal e frequência horária
      2. Identifica blocos de NaN
      3. Trata valores faltantes (interpolação + lag sazonal)
 
    Parâmetros
    ----------
    df : pd.DataFrame
        DataFrame com colunas 'datetime' e 'temperatura' (saída do loader).
    limiar : int
        Tamanho máximo (horas) de um bloco considerado "pequeno".
        Blocos <= limiar → interpolação. Blocos > limiar → lag sazonal.
 
    Retorna
    -------
    pd.DataFrame
        DataFrame indexado por datetime com colunas:
        - temperatura        : valores originais
        - temperatura_tratada: valores após imputação (sem NaN)
        - is_missing         : bool, True onde havia NaN original
        - tamanho_bloco_nan  : tamanho do bloco consecutivo de NaN
    """
    df = _definir_indice(df)
    df = _identificar_blocos_nan(df)
    df = _tratar_missing(df, limiar)
    df = _validar(df)
    return df
 
 
# ---------------------------------------------------------------------------
# Etapas internas
# ---------------------------------------------------------------------------
 
def _definir_indice(df: pd.DataFrame) -> pd.DataFrame:
    """Define índice temporal com frequência horária."""
    df = df.set_index("datetime")
    df = df.asfreq("h")
    df["is_missing"] = df["temperatura"].isna()
    return df
 
 
def _identificar_blocos_nan(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula o tamanho de cada bloco consecutivo de NaN."""
    grupo = (df["is_missing"] != df["is_missing"].shift()).cumsum()
    df["tamanho_bloco_nan"] = df.groupby(grupo)["is_missing"].transform("sum")
    return df
 
 
def _tratar_missing(df: pd.DataFrame, limiar: int) -> pd.DataFrame:
    """
    Imputa valores faltantes em dois estágios:
      - Blocos pequenos (<=limiar): interpolação temporal
      - Blocos grandes (>limiar) : combinação ponderada de lags (24h e 168h)
                                   com fallback para média sazonal (mês + hora)
    """
    is_nan = df["temperatura"].isna()
    mask_pequeno = is_nan & (df["tamanho_bloco_nan"] <= limiar)
    mask_grande  = is_nan & (df["tamanho_bloco_nan"] >  limiar)
 
    df["temperatura_tratada"] = df["temperatura"].copy()
 
    # ── Blocos pequenos: interpolação temporal ──────────────────────────────
    temp_interp = df["temperatura"].interpolate(method="time")
    df.loc[mask_pequeno, "temperatura_tratada"] = temp_interp[mask_pequeno]
 
    # ── Blocos grandes: lags sobre a série ORIGINAL + fallback sazonal ──────
    lag_24  = df["temperatura"].shift(24).rolling(window=5, center=True, min_periods=1).mean()
    lag_168 = df["temperatura"].shift(168).rolling(window=5, center=True, min_periods=1).mean()
 
    df["_mes"]  = df.index.month
    df["_hora"] = df.index.hour
    media_sazonal = df.groupby(["_mes", "_hora"])["temperatura"].transform("mean")
 
    for idx in df[mask_grande].index:
        v24  = lag_24.at[idx]
        v168 = lag_168.at[idx]
        vSaz = media_sazonal.at[idx]
 
        if pd.notna(v24) and pd.notna(v168):
            df.at[idx, "temperatura_tratada"] = 0.7 * v24 + 0.3 * v168
        elif pd.notna(v24):
            df.at[idx, "temperatura_tratada"] = v24
        elif pd.notna(v168):
            df.at[idx, "temperatura_tratada"] = v168
        else:
            df.at[idx, "temperatura_tratada"] = vSaz
 
    df.drop(columns=["_mes", "_hora"], inplace=True)
    return df
 
 
def _validar(df: pd.DataFrame) -> pd.DataFrame:
    """Garante que não restam NaNs após o tratamento."""
    nulos = df["temperatura_tratada"].isnull().sum()
    if nulos > 0:
        raise ValueError(
            f"Ainda há {nulos} NaN(s) em 'temperatura_tratada' após o tratamento. "
            "Verifique os dados de origem."
        )
    return df