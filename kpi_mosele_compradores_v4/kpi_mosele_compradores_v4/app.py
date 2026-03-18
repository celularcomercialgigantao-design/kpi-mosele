from __future__ import annotations

import io
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from flask import Flask, render_template, request, send_file, url_for

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "dados"
SUPPORTED_EXTENSIONS = {".xlsx", ".xlsm", ".xltx", ".xltm"}

MONTH_ORDER = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "março": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}

BRAND = {
    "orange": "#F59C00",
    "orange_dark": "#D97D00",
    "green": "#00A651",
    "green_dark": "#00793B",
    "bg": "#FFF8EF",
    "text": "#213547",
    "muted": "#617282",
    "danger": "#D64545",
    "warning": "#C98900",
}

app = Flask(__name__)


def br_currency(value: float | int | None) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        value = 0
    return f"R$ {float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def br_percent(value: float | int | None) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        value = 0
    return f"{float(value):.2f}%".replace(".", ",")


def br_number(value: float | int | None, decimals: int = 0) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        value = 0
    return f"{float(value):,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def normalize_name(value: Any) -> str:
    text = str(value or "").strip()
    replacements = {
        "á": "a", "à": "a", "ã": "a", "â": "a",
        "é": "e", "ê": "e", "í": "i", "ó": "o",
        "ô": "o", "õ": "o", "ú": "u", "ç": "c",
        "Á": "A", "À": "A", "Ã": "A", "Â": "A",
        "É": "E", "Ê": "E", "Í": "I", "Ó": "O",
        "Ô": "O", "Õ": "O", "Ú": "U", "Ç": "C",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return " ".join(text.split()).lower()


def month_sort_key(value: str) -> tuple[int, str]:
    norm = normalize_name(value)
    return (MONTH_ORDER.get(norm, 99), norm)


def latest_excel_file() -> Path | None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    candidates = [p for p in DATA_DIR.iterdir() if p.suffix.lower() in SUPPORTED_EXTENSIONS]
    return max(candidates, key=lambda p: p.stat().st_mtime) if candidates else None


def find_sheet(sheet_names: list[str], expected: str) -> str | None:
    target = normalize_name(expected)
    for name in sheet_names:
        if normalize_name(name) == target:
            return name
    for name in sheet_names:
        if target in normalize_name(name):
            return name
    return None


def read_base_sheet(excel_path: Path, sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(excel_path, sheet_name=sheet_name, header=None)
    if df.empty:
        return pd.DataFrame()

    row0 = [str(x).strip() if pd.notna(x) else "" for x in df.iloc[0].tolist()]
    row1 = [str(x).strip() if pd.notna(x) else "" for x in df.iloc[1].tolist()] if len(df) > 1 else [""] * len(row0)

    headers = []
    for a, b in zip(row0, row1):
        full = f"{a} {b}".strip()
        headers.append(full if full else a)

    data = df.iloc[2:].copy()
    data.columns = headers
    data = data.dropna(how="all")

    rename_map = {}
    for col in data.columns:
        ncol = normalize_name(col)
        if "nome dpto" in ncol or ncol == "categoria":
            rename_map[col] = "categoria"
        elif "comprador" in ncol:
            rename_map[col] = "comprador"
        elif ncol.startswith("ano"):
            rename_map[col] = "ano"
        elif "mes" in ncol:
            rename_map[col] = "mes"
        elif "vendas" in ncol:
            rename_map[col] = "vendas"
        elif "% lucro" in ncol or "margem" in ncol:
            rename_map[col] = "margem"
        elif "lucros" in ncol or ("lucro" in ncol and "%" not in ncol):
            rename_map[col] = "lucro"
        elif "compras" in ncol:
            rename_map[col] = "compras"
        elif "bonific" in ncol:
            rename_map[col] = "bonificacao"

    data = data.rename(columns=rename_map)
    required = ["categoria", "comprador", "ano", "mes", "vendas", "lucro", "margem", "compras", "bonificacao"]
    for col in required:
        if col not in data.columns:
            data[col] = 0

    data = data[required].copy()
    data = data.dropna(how="all")
    data["categoria"] = data["categoria"].astype(str).str.strip()
    data["comprador"] = data["comprador"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    data["mes"] = data["mes"].astype(str).str.strip()
    data = data[data["categoria"].ne("")]

    for col in ["vendas", "lucro", "margem", "compras", "bonificacao"]:
        data[col] = pd.to_numeric(data[col], errors="coerce").fillna(0.0)

    data["ano"] = pd.to_numeric(data["ano"], errors="coerce").fillna(0).astype(int)
    data["categoria_key"] = data["categoria"].map(normalize_name)
    data["comprador_key"] = data["comprador"].map(normalize_name)
    data["mes_key"] = data["mes"].map(normalize_name)
    return data


def read_targets_sheet(excel_path: Path, sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    if df.empty:
        return pd.DataFrame()

    rename_map = {}
    for col in df.columns:
        ncol = normalize_name(col)
        if "comprador" in ncol:
            rename_map[col] = "comprador"
        elif "categoria" in ncol:
            rename_map[col] = "categoria"
        elif ncol == "mes" or " mes" in ncol:
            rename_map[col] = "mes"
        elif "crescimento" in ncol:
            rename_map[col] = "crescimento_meta"
        elif "meta_vendas" in ncol or ("meta" in ncol and "venda" in ncol):
            rename_map[col] = "meta_vendas"
        elif "compra_maxima" in ncol or ("compra" in ncol and "max" in ncol):
            rename_map[col] = "compra_maxima"
        elif "meta_bonificacao" in ncol or ("meta" in ncol and "bonific" in ncol):
            rename_map[col] = "meta_bonificacao"
        elif "margem_ano_anterior" in ncol:
            rename_map[col] = "margem_ano_anterior"
        elif "meta_margem" in ncol or ("meta" in ncol and "margem" in ncol):
            rename_map[col] = "meta_margem"
        elif "venda_ano_anterior" in ncol:
            rename_map[col] = "venda_ano_anterior"

    df = df.rename(columns=rename_map)
    required = [
        "comprador", "categoria", "mes", "crescimento_meta", "meta_vendas",
        "compra_maxima", "meta_bonificacao", "margem_ano_anterior", "meta_margem", "venda_ano_anterior"
    ]
    for col in required:
        if col not in df.columns:
            df[col] = 0

    df = df[required].copy()
    df["comprador"] = df["comprador"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    df["categoria"] = df["categoria"].astype(str).str.strip()
    df["mes"] = df["mes"].astype(str).str.strip()

    for col in ["crescimento_meta", "meta_vendas", "compra_maxima", "meta_bonificacao", "margem_ano_anterior", "meta_margem", "venda_ano_anterior"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["categoria_key"] = df["categoria"].map(normalize_name)
    df["comprador_key"] = df["comprador"].map(normalize_name)
    df["mes_key"] = df["mes"].map(normalize_name)
    return df


def classify_status(realized: float, target: float, lower_is_better: bool = False) -> str:
    if target in (0, None) or pd.isna(target):
        return "Sem meta"
    ratio = realized / target if target else 0
    if lower_is_better:
        if ratio <= 1.00:
            return "OK"
        if ratio <= 1.05:
            return "Atenção"
        return "Crítico"
    if ratio >= 1.00:
        return "OK"
    if ratio >= 0.95:
        return "Atenção"
    return "Crítico"


def weighted_score_components(row: pd.Series) -> tuple[float, float, float, float, float]:
    venda_ratio = min((row["vendas"] / row["meta_vendas"]) if row["meta_vendas"] else 0, 1.0)
    margem_ratio = min((row["margem"] / row["meta_margem"]) if row["meta_margem"] else 0, 1.0)
    bonus_ratio = min((row["bonificacao"] / row["meta_bonificacao"]) if row["meta_bonificacao"] else 0, 1.0)

    compra_ratio = (row["compras"] / row["compra_maxima"]) if row["compra_maxima"] else 1.0
    compra_ratio = max(0.0, min(compra_ratio, 2.0))
    compra_score_ratio = 1.0 if compra_ratio <= 1 else max(0.0, 1 - (compra_ratio - 1))

    venda_pts = venda_ratio * 50
    compra_pts = compra_score_ratio * 20
    margem_pts = margem_ratio * 20
    bonus_pts = bonus_ratio * 20
    total = venda_pts + compra_pts + margem_pts + bonus_pts
    return total, venda_pts, compra_pts, margem_pts, bonus_pts


def score_status(row: pd.Series) -> str:
    statuses = [row["status_vendas"], row["status_margem"], row["status_compra"], row["status_bonificacao"]]
    if "Crítico" in statuses:
        return "Crítico"
    if "Atenção" in statuses:
        return "Atenção"
    if all(s == "Sem meta" for s in statuses):
        return "Sem meta"
    return "OK"


def medal_for_position(position: int) -> str:
    if position == 1:
        return "🥇"
    if position == 2:
        return "🥈"
    if position == 3:
        return "🥉"
    return ""


def build_dashboard(excel_path: Path, buyer_filter: str = "", month_filter: str = "") -> dict[str, Any]:
    xls = pd.ExcelFile(excel_path)
    prev_sheet = find_sheet(xls.sheet_names, "Base ano anterior")
    curr_sheet = find_sheet(xls.sheet_names, "Base ano atual")
    targets_sheet = find_sheet(xls.sheet_names, "metas comprador")

    if not prev_sheet or not curr_sheet:
        raise ValueError("Não encontrei as abas 'Base ano anterior' e 'Base ano atual'.")
    if not targets_sheet:
        raise ValueError("Não encontrei a aba 'metas comprador'.")

    base_prev = read_base_sheet(excel_path, prev_sheet)
    base_curr = read_base_sheet(excel_path, curr_sheet)
    metas = read_targets_sheet(excel_path, targets_sheet)

    if base_curr.empty:
        raise ValueError("A aba 'Base ano atual' está vazia ou fora do padrão esperado.")

    merged = base_curr.merge(
        metas,
        on=["categoria_key", "mes_key"],
        how="left",
        suffixes=("", "_meta"),
    )

    missing_meta = merged["meta_margem"].fillna(0).eq(0)
    if missing_meta.any() and not metas.empty:
        metas_by_cat = (
            metas.sort_values(["categoria", "mes"], key=lambda s: s.map(normalize_name))
            .drop_duplicates(subset=["categoria_key"], keep="last")
        )
        fallback = merged.loc[missing_meta, ["categoria_key"]].merge(
            metas_by_cat[[
                "categoria_key", "crescimento_meta", "meta_vendas", "compra_maxima",
                "meta_bonificacao", "margem_ano_anterior", "meta_margem", "venda_ano_anterior"
            ]],
            on="categoria_key",
            how="left"
        )
        for col in ["crescimento_meta", "meta_vendas", "compra_maxima", "meta_bonificacao", "margem_ano_anterior", "meta_margem", "venda_ano_anterior"]:
            merged.loc[missing_meta, col] = merged.loc[missing_meta, col].fillna(fallback[col].values)

    base_prev_min = base_prev[["categoria_key", "mes_key", "vendas", "margem"]].rename(columns={"vendas": "venda_anterior_base", "margem": "margem_anterior_base"})
    merged = merged.merge(base_prev_min, on=["categoria_key", "mes_key"], how="left")

    merged["venda_ano_anterior"] = pd.to_numeric(merged["venda_ano_anterior"], errors="coerce").fillna(merged["venda_anterior_base"]).fillna(0)
    merged["margem_ano_anterior"] = pd.to_numeric(merged["margem_ano_anterior"], errors="coerce").fillna(merged["margem_anterior_base"]).fillna(0)
    merged["crescimento_meta"] = pd.to_numeric(merged["crescimento_meta"], errors="coerce").fillna(0)
    merged["meta_vendas"] = pd.to_numeric(merged["meta_vendas"], errors="coerce").fillna(0)
    merged["compra_maxima"] = pd.to_numeric(merged["compra_maxima"], errors="coerce").fillna(0)
    merged["meta_bonificacao"] = pd.to_numeric(merged["meta_bonificacao"], errors="coerce").fillna(0)
    merged["meta_margem"] = pd.to_numeric(merged["meta_margem"], errors="coerce").fillna(0)

    recalc_vendas = merged["meta_vendas"].eq(0)
    merged.loc[recalc_vendas, "meta_vendas"] = merged.loc[recalc_vendas, "venda_ano_anterior"] * (1 + merged.loc[recalc_vendas, "crescimento_meta"])

    recalc_compra = merged["compra_maxima"].eq(0)
    merged.loc[recalc_compra, "compra_maxima"] = merged.loc[recalc_compra, "meta_vendas"] * 0.72

    recalc_bonus = merged["meta_bonificacao"].eq(0)
    merged.loc[recalc_bonus, "meta_bonificacao"] = merged.loc[recalc_bonus, "compra_maxima"] * 0.02

    merged["pct_compra_sobre_venda"] = (merged["compras"] / merged["vendas"].replace(0, pd.NA) * 100).fillna(0)
    merged["pct_meta_compra_sobre_venda"] = (merged["compra_maxima"] / merged["meta_vendas"].replace(0, pd.NA) * 100).fillna(0)

    compradores = sorted([x for x in merged["comprador"].dropna().astype(str).unique() if x.strip()], key=normalize_name)
    meses = sorted([x for x in merged["mes"].dropna().astype(str).unique() if x.strip()], key=month_sort_key)

    selected_buyer = buyer_filter.strip()
    selected_month = month_filter.strip()

    filtered = merged.copy()
    if selected_buyer:
        filtered = filtered[filtered["comprador"].map(normalize_name) == normalize_name(selected_buyer)]
    if selected_month:
        filtered = filtered[filtered["mes"].map(normalize_name) == normalize_name(selected_month)]

    if filtered.empty:
        raise ValueError("Não encontrei dados para o filtro aplicado.")

    filtered["ating_vendas"] = (filtered["vendas"] / filtered["meta_vendas"].replace(0, pd.NA) * 100).fillna(0)
    filtered["ating_margem"] = (filtered["margem"] / filtered["meta_margem"].replace(0, pd.NA) * 100).fillna(0)
    filtered["uso_compra"] = (filtered["compras"] / filtered["compra_maxima"].replace(0, pd.NA) * 100).fillna(0)
    filtered["ating_bonificacao"] = (filtered["bonificacao"] / filtered["meta_bonificacao"].replace(0, pd.NA) * 100).fillna(0)

    filtered["status_vendas"] = filtered.apply(lambda r: classify_status(r["vendas"], r["meta_vendas"]), axis=1)
    filtered["status_margem"] = filtered.apply(lambda r: classify_status(r["margem"], r["meta_margem"]), axis=1)
    filtered["status_compra"] = filtered.apply(lambda r: classify_status(r["compras"], r["compra_maxima"], lower_is_better=True), axis=1)
    filtered["status_bonificacao"] = filtered.apply(lambda r: classify_status(r["bonificacao"], r["meta_bonificacao"]), axis=1)
    filtered["status_geral"] = filtered.apply(score_status, axis=1)

    score_parts = filtered.apply(weighted_score_components, axis=1, result_type="expand")
    filtered[["score_total", "pts_vendas", "pts_compra", "pts_margem", "pts_bonificacao"]] = score_parts

    total_vendas = filtered["vendas"].sum()
    total_meta_vendas = filtered["meta_vendas"].sum()
    total_compras = filtered["compras"].sum()
    total_compra_max = filtered["compra_maxima"].sum()
    total_bonificacao = filtered["bonificacao"].sum()
    total_meta_bonificacao = filtered["meta_bonificacao"].sum()
    margem_media = filtered["margem"].mean()
    meta_margem_media = filtered["meta_margem"].mean()
    compra_sobre_venda = (total_compras / total_vendas * 100) if total_vendas else 0
    meta_compra_sobre_venda = (total_compra_max / total_meta_vendas * 100) if total_meta_vendas else 0
    score_total_medio = filtered["score_total"].mean()

    ranking_df = filtered.groupby("comprador", as_index=False).agg(
        categorias=("categoria", "nunique"),
        vendas=("vendas", "sum"),
        meta_vendas=("meta_vendas", "sum"),
        margem=("margem", "mean"),
        meta_margem=("meta_margem", "mean"),
        compras=("compras", "sum"),
        compra_maxima=("compra_maxima", "sum"),
        bonificacao=("bonificacao", "sum"),
        meta_bonificacao=("meta_bonificacao", "sum"),
        score_total=("score_total", "mean"),
        pts_vendas=("pts_vendas", "mean"),
        pts_compra=("pts_compra", "mean"),
        pts_margem=("pts_margem", "mean"),
        pts_bonificacao=("pts_bonificacao", "mean"),
    )
    ranking_df["pct_compra_sobre_venda"] = (ranking_df["compras"] / ranking_df["vendas"].replace(0, pd.NA) * 100).fillna(0)
    ranking_df = ranking_df.sort_values(["score_total", "vendas"], ascending=[False, False]).reset_index(drop=True)

    ranking = []
    for idx, row in ranking_df.iterrows():
        ranking.append({
            "posicao": idx + 1,
            "medalha": medal_for_position(idx + 1),
            "comprador": row["comprador"],
            "categorias": int(row["categorias"]),
            "score": br_number(row["score_total"], 1),
            "pontos_vendas": br_number(row["pts_vendas"], 1),
            "pontos_compra": br_number(row["pts_compra"], 1),
            "pontos_margem": br_number(row["pts_margem"], 1),
            "pontos_bonus": br_number(row["pts_bonificacao"], 1),
            "vendas": br_currency(row["vendas"]),
            "meta_vendas": br_currency(row["meta_vendas"]),
            "margem": br_percent(row["margem"]),
            "compra_sobre_venda": br_percent(row["pct_compra_sobre_venda"]),
        })

    diagnostics_df = filtered.copy()
    diagnostics_df["desvio_vendas"] = diagnostics_df["meta_vendas"] - diagnostics_df["vendas"]
    diagnostics_df["desvio_margem"] = diagnostics_df["meta_margem"] - diagnostics_df["margem"]
    diagnostics_df["excesso_compra"] = diagnostics_df["compras"] - diagnostics_df["compra_maxima"]
    diagnostics_df["desvio_bonificacao"] = diagnostics_df["meta_bonificacao"] - diagnostics_df["bonificacao"]

    order_map = {"Crítico": 0, "Atenção": 1, "OK": 2, "Sem meta": 3}
    diagnostics_df["status_ord"] = diagnostics_df["status_geral"].map(order_map).fillna(9)
    diagnostics_df = diagnostics_df.sort_values(["status_ord", "desvio_vendas", "desvio_margem"], ascending=[True, False, False]).head(12)

    diagnostics = []
    for _, row in diagnostics_df.iterrows():
        motivos = []
        if row["status_vendas"] != "OK":
            motivos.append(f"vendas em {br_percent((row['vendas'] / row['meta_vendas'] * 100) if row['meta_vendas'] else 0)} da meta")
        if row["status_margem"] != "OK":
            motivos.append(f"margem em {br_percent((row['margem'] / row['meta_margem'] * 100) if row['meta_margem'] else 0)} da meta")
        if row["status_compra"] != "OK":
            motivos.append(f"compra/venda em {br_percent(row['pct_compra_sobre_venda'])} com teto meta de {br_percent(row['pct_meta_compra_sobre_venda'])}")
        if row["status_bonificacao"] != "OK":
            motivos.append(f"bonificação em {br_percent((row['bonificacao'] / row['meta_bonificacao'] * 100) if row['meta_bonificacao'] else 0)} da meta")
        if not motivos:
            motivos.append("indicadores dentro do esperado")

        diagnostics.append({
            "categoria": row["categoria"],
            "comprador": row["comprador"],
            "mes": row["mes"],
            "status": row["status_geral"],
            "motivos": "; ".join(motivos).capitalize() + ".",
            "vendas": br_currency(row["vendas"]),
            "meta_vendas": br_currency(row["meta_vendas"]),
            "compras": br_currency(row["compras"]),
            "compra_max": br_currency(row["compra_maxima"]),
            "margem": br_percent(row["margem"]),
            "meta_margem": br_percent(row["meta_margem"]),
            "bonificacao": br_currency(row["bonificacao"]),
            "meta_bonificacao": br_currency(row["meta_bonificacao"]),
            "score": br_number(row["score_total"], 1),
        })

    compare_buyers = ranking_df.copy()

    status_counts = filtered["status_geral"].value_counts().reindex(["OK", "Atenção", "Crítico", "Sem meta"], fill_value=0)

    monthly = filtered.groupby(["mes"], as_index=False).agg(
        vendas=("vendas", "sum"),
        meta_vendas=("meta_vendas", "sum"),
        compras=("compras", "sum"),
        compra_maxima=("compra_maxima", "sum"),
        bonificacao=("bonificacao", "sum"),
        meta_bonificacao=("meta_bonificacao", "sum"),
        margem=("margem", "mean"),
        meta_margem=("meta_margem", "mean"),
        score_total=("score_total", "mean"),
    )
    monthly["pct_compra_sobre_venda"] = (monthly["compras"] / monthly["vendas"].replace(0, pd.NA) * 100).fillna(0)
    monthly["pct_meta_compra_sobre_venda"] = (monthly["compra_maxima"] / monthly["meta_vendas"].replace(0, pd.NA) * 100).fillna(0)
    monthly = monthly.sort_values("mes", key=lambda s: s.map(month_sort_key))

    detailed_export = filtered[[
        "comprador", "categoria", "mes", "vendas", "meta_vendas", "compras", "compra_maxima",
        "pct_compra_sobre_venda", "pct_meta_compra_sobre_venda", "margem", "meta_margem", "bonificacao",
        "meta_bonificacao", "status_vendas", "status_compra", "status_margem", "status_bonificacao",
        "status_geral", "pts_vendas", "pts_compra", "pts_margem", "pts_bonificacao", "score_total"
    ]].copy()

    detailed_rows = []
    for _, row in detailed_export.sort_values(["comprador", "mes", "categoria"], key=lambda col: col.map(month_sort_key) if col.name == "mes" else col).iterrows():
        detailed_rows.append({
            "comprador": row["comprador"],
            "categoria": row["categoria"],
            "mes": row["mes"],
            "vendas": br_currency(row["vendas"]),
            "meta_vendas": br_currency(row["meta_vendas"]),
            "compras": br_currency(row["compras"]),
            "compra_maxima": br_currency(row["compra_maxima"]),
            "compra_sobre_venda": br_percent(row["pct_compra_sobre_venda"]),
            "meta_compra_sobre_venda": br_percent(row["pct_meta_compra_sobre_venda"]),
            "margem": br_percent(row["margem"]),
            "meta_margem": br_percent(row["meta_margem"]),
            "bonificacao": br_currency(row["bonificacao"]),
            "meta_bonificacao": br_currency(row["meta_bonificacao"]),
            "pts_vendas": br_number(row["pts_vendas"], 1),
            "pts_compra": br_number(row["pts_compra"], 1),
            "pts_margem": br_number(row["pts_margem"], 1),
            "pts_bonificacao": br_number(row["pts_bonificacao"], 1),
            "score_total": br_number(row["score_total"], 1),
            "status": row["status_geral"],
        })

    chart_payload = {
        "buyers_compare": {
            "labels": compare_buyers["comprador"].tolist(),
            "realizado": [round(x, 2) for x in compare_buyers["vendas"].tolist()],
            "meta": [round(x, 2) for x in compare_buyers["meta_vendas"].tolist()],
        },
        "status_counts": {
            "labels": status_counts.index.tolist(),
            "values": [int(x) for x in status_counts.tolist()],
        },
        "monthly_sales": {
            "labels": monthly["mes"].tolist(),
            "realizado": [round(x, 2) for x in monthly["vendas"].tolist()],
            "meta": [round(x, 2) for x in monthly["meta_vendas"].tolist()],
        },
        "monthly_buy_ratio": {
            "labels": monthly["mes"].tolist(),
            "realizado": [round(x, 2) for x in monthly["pct_compra_sobre_venda"].tolist()],
            "meta": [round(x, 2) for x in monthly["pct_meta_compra_sobre_venda"].tolist()],
        },
        "monthly_margin": {
            "labels": monthly["mes"].tolist(),
            "realizado": [round(x, 2) for x in monthly["margem"].tolist()],
            "meta": [round(x, 2) for x in monthly["meta_margem"].tolist()],
        },
        "monthly_bonus": {
            "labels": monthly["mes"].tolist(),
            "realizado": [round(x, 2) for x in monthly["bonificacao"].tolist()],
            "meta": [round(x, 2) for x in monthly["meta_bonificacao"].tolist()],
        },
        "score_breakdown": {
            "labels": compare_buyers["comprador"].tolist(),
            "vendas": [round(x, 2) for x in compare_buyers["pts_vendas"].tolist()],
            "compra": [round(x, 2) for x in compare_buyers["pts_compra"].tolist()],
            "margem": [round(x, 2) for x in compare_buyers["pts_margem"].tolist()],
            "bonus": [round(x, 2) for x in compare_buyers["pts_bonificacao"].tolist()],
        },
        "monthly_score": {
            "labels": monthly["mes"].tolist(),
            "score": [round(x, 2) for x in monthly["score_total"].tolist()],
        },
    }

    anos = {
        "anterior": int(base_prev["ano"].mode().iloc[0]) if not base_prev.empty else "-",
        "atual": int(base_curr["ano"].mode().iloc[0]) if not base_curr.empty else "-",
    }

    top_buyer_name = ranking_df.iloc[0]["comprador"] if not ranking_df.empty else "-"
    top_buyer_score = br_number(ranking_df.iloc[0]["score_total"], 1) if not ranking_df.empty else "0,0"
    critical_count = int((filtered["status_geral"] == "Crítico").sum())
    attention_count = int((filtered["status_geral"] == "Atenção").sum())
    best_month_name = monthly.sort_values(["score_total", "vendas"], ascending=[False, False]).iloc[0]["mes"] if not monthly.empty else "-"
    best_month_score = br_number(monthly.sort_values(["score_total", "vendas"], ascending=[False, False]).iloc[0]["score_total"], 1) if not monthly.empty else "0,0"

    gap_map = {
        "Vendas": float((filtered["meta_vendas"] - filtered["vendas"]).clip(lower=0).sum()),
        "Compra/Venda": float((filtered["compras"] - filtered["compra_maxima"]).clip(lower=0).sum()),
        "Margem": float((filtered["meta_margem"] - filtered["margem"]).clip(lower=0).sum()),
        "Bonificação": float((filtered["meta_bonificacao"] - filtered["bonificacao"]).clip(lower=0).sum()),
    }
    main_alert = max(gap_map, key=gap_map.get) if gap_map else "Vendas"

    insights = [
        {
            "titulo": "Líder do ranking",
            "valor": f"{top_buyer_name} {medal_for_position(1)}".strip(),
            "texto": f"Maior score médio do painel: {top_buyer_score} pts.",
        },
        {
            "titulo": "Maior pressão do momento",
            "valor": main_alert,
            "texto": f"Indicador que mais pressiona o resultado no filtro atual.",
        },
        {
            "titulo": "Categorias em risco",
            "valor": str(critical_count),
            "texto": f"Com {attention_count} categorias adicionais em atenção.",
        },
        {
            "titulo": "Melhor mês do painel",
            "valor": best_month_name,
            "texto": f"Maior score médio mensal: {best_month_score} pts.",
        },
    ]

    return {
        "arquivo": excel_path.name,
        "atualizado_em": datetime.fromtimestamp(excel_path.stat().st_mtime).strftime("%d/%m/%Y %H:%M"),
        "anos": anos,
        "filters": {"compradores": compradores, "meses": meses},
        "selected": {"comprador": selected_buyer, "mes": selected_month},
        "resumo": {
            "registros": int(len(filtered)),
            "categorias": int(filtered["categoria"].nunique()),
            "compradores": int(filtered["comprador"].nunique()),
            "meses": int(filtered["mes"].nunique()),
        },
        "insights": insights,
        "brand": BRAND,
        "weights": {"vendas": 50, "compra": 20, "margem": 20, "bonificacao": 20, "total": 110},
        "kpis": {
            "score": {
                "label": "Score médio do painel",
                "value": f"{br_number(score_total_medio, 1)} pts",
                "sub": "Peso: vendas 50 | compra/venda 20 | margem 20 | bonificação 20",
            },
            "vendas": {
                "label": "Vendas realizadas",
                "value": br_currency(total_vendas),
                "sub": f"Meta {br_currency(total_meta_vendas)} • Atingimento {br_percent((total_vendas / total_meta_vendas * 100) if total_meta_vendas else 0)}",
            },
            "compra_sobre_venda": {
                "label": "Compra sobre venda",
                "value": br_percent(compra_sobre_venda),
                "sub": f"Meta/teto {br_percent(meta_compra_sobre_venda)} • Compras {br_currency(total_compras)}",
            },
            "margem": {
                "label": "Margem média",
                "value": br_percent(margem_media),
                "sub": f"Meta {br_percent(meta_margem_media)} • Por categoria na planilha",
            },
            "bonificacao": {
                "label": "Bonificação realizada",
                "value": br_currency(total_bonificacao),
                "sub": f"Meta {br_currency(total_meta_bonificacao)} • Atingimento {br_percent((total_bonificacao / total_meta_bonificacao * 100) if total_meta_bonificacao else 0)}",
            },
        },
        "ranking": ranking,
        "diagnostics": diagnostics,
        "details": detailed_rows,
        "charts": json.dumps(chart_payload, ensure_ascii=False),
        "export_link": url_for("export_excel", buyer=selected_buyer, month=selected_month),
        "export_data": {
            "resumo_df": pd.DataFrame([
                {"Indicador": "Arquivo base", "Valor": excel_path.name},
                {"Indicador": "Atualizado em", "Valor": datetime.fromtimestamp(excel_path.stat().st_mtime).strftime('%d/%m/%Y %H:%M')},
                {"Indicador": "Filtro comprador", "Valor": selected_buyer or "Todos"},
                {"Indicador": "Filtro mês", "Valor": selected_month or "Todos"},
                {"Indicador": "Score médio", "Valor": round(score_total_medio, 1)},
                {"Indicador": "Vendas", "Valor": round(total_vendas, 2)},
                {"Indicador": "Meta vendas", "Valor": round(total_meta_vendas, 2)},
                {"Indicador": "Compra sobre venda %", "Valor": round(compra_sobre_venda, 2)},
                {"Indicador": "Meta compra sobre venda %", "Valor": round(meta_compra_sobre_venda, 2)},
                {"Indicador": "Margem média %", "Valor": round(margem_media, 2)},
                {"Indicador": "Meta margem %", "Valor": round(meta_margem_media, 2)},
                {"Indicador": "Bonificação", "Valor": round(total_bonificacao, 2)},
                {"Indicador": "Meta bonificação", "Valor": round(total_meta_bonificacao, 2)},
            ]),
            "ranking_df": ranking_df,
            "diagnostics_df": diagnostics_df,
            "details_df": detailed_export.sort_values(["comprador", "mes", "categoria"]),
            "monthly_df": monthly,
        },
    }


def autosize_columns(worksheet, dataframe: pd.DataFrame) -> None:
    for idx, column in enumerate(dataframe.columns, 1):
        max_length = max(len(str(column)), *(len(str(v)) for v in dataframe[column].head(300).fillna(""))) if not dataframe.empty else len(str(column))
        worksheet.column_dimensions[chr(64 + idx) if idx <= 26 else 'A'].width = min(max(max_length + 2, 12), 28)


def export_dashboard_excel(data: dict[str, Any]) -> io.BytesIO:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        data["export_data"]["resumo_df"].to_excel(writer, sheet_name="Resumo", index=False)
        data["export_data"]["ranking_df"].to_excel(writer, sheet_name="Ranking", index=False)
        data["export_data"]["monthly_df"].to_excel(writer, sheet_name="Mensal", index=False)
        data["export_data"]["diagnostics_df"].to_excel(writer, sheet_name="Diagnostico", index=False)
        data["export_data"]["details_df"].to_excel(writer, sheet_name="Detalhamento", index=False)

        wb = writer.book
        header_fill = __import__("openpyxl.styles", fromlist=["PatternFill"]).PatternFill(fill_type="solid", fgColor="F59C00")
        header_font = __import__("openpyxl.styles", fromlist=["Font"]).Font(color="FFFFFF", bold=True)
        body_fill = __import__("openpyxl.styles", fromlist=["PatternFill"]).PatternFill(fill_type="solid", fgColor="FFF8EF")
        thin = __import__("openpyxl.styles", fromlist=["Side", "Border"]).Side(style="thin", color="E2D5C2")
        border = __import__("openpyxl.styles", fromlist=["Border"]).Border(left=thin, right=thin, top=thin, bottom=thin)

        for ws in wb.worksheets:
            ws.freeze_panes = "A2"
            for row in ws.iter_rows():
                for cell in row:
                    cell.border = border
                    if cell.row == 1:
                        cell.fill = header_fill
                        cell.font = header_font
                    else:
                        cell.fill = body_fill
            for column_cells in ws.columns:
                max_length = max(len(str(cell.value or "")) for cell in column_cells[:300])
                ws.column_dimensions[column_cells[0].column_letter].width = min(max(max_length + 2, 12), 28)

    output.seek(0)
    return output


@app.route("/")
def index():
    excel_file = latest_excel_file()
    if not excel_file:
        return render_template("index.html", no_file=True, data=None, error=None)

    try:
        data = build_dashboard(
            excel_file,
            buyer_filter=request.args.get("buyer", ""),
            month_filter=request.args.get("month", ""),
        )
        return render_template("index.html", no_file=False, data=data, error=None)
    except Exception as exc:
        return render_template("index.html", no_file=False, data=None, error=str(exc))


@app.route("/export/excel")
def export_excel():
    excel_file = latest_excel_file()
    if not excel_file:
        return "Nenhum arquivo Excel encontrado.", 404

    data = build_dashboard(
        excel_file,
        buyer_filter=request.args.get("buyer", ""),
        month_filter=request.args.get("month", ""),
    )
    buffer = export_dashboard_excel(data)
    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"dashboard_compradores_{stamp}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    app.run(debug=True)
