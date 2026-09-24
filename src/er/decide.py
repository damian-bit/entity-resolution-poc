"""p_match = regresión logística (features [+ Laya]) y ruteo MATCH / NON_MATCH / REVIEW."""
from enum import Enum

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
from sklearn.linear_model import LogisticRegression

TARGET_PRECISION = 0.98
AGREE_FEATURES = ["email_agree", "phone_agree", "birth_agree", "city_agree"]
SIM_FEATURES = ["name_sim", "address_sim"]


class Decision(str, Enum):
    MATCH = "MATCH"
    NON_MATCH = "NON_MATCH"
    REVIEW = "REVIEW"


class PairResult(BaseModel):
    """Columna semántica: lo único que sale de la etapa de decisión."""
    model_config = ConfigDict(extra="forbid")
    pair_id: str
    p_same: float | None = Field(default=None, ge=0, le=1)  # Laya
    p_match: float = Field(ge=0, le=1)                      # regresión
    decision: Decision


def design_matrix(df: pd.DataFrame, laya_col: str | None) -> np.ndarray:
    """Agreements en one-hot (igual / distinto; 'falta' es la base) + similitudes [+ p_same]."""
    cols = []
    for f in AGREE_FEATURES:
        cols += [(df[f] == 1).astype(float), (df[f] == -1).astype(float)]
    cols += [df[f].astype(float) for f in SIM_FEATURES]
    if laya_col:
        cols.append(df[laya_col].fillna(0.5).astype(float))
    return np.column_stack(cols)


def fit(df_dev: pd.DataFrame, laya_col: str | None) -> LogisticRegression:
    model = LogisticRegression(max_iter=2000)
    model.fit(design_matrix(df_dev, laya_col), df_dev["is_match"].astype(int))
    return model


def tune_thresholds(p: np.ndarray, y: np.ndarray, target: float = TARGET_PRECISION) -> tuple[float, float]:
    """τ_high: el menor umbral con precisión ≥ target. τ_low: el mayor umbral con NPV ≥ target."""
    grid = np.round(np.arange(0.5, 1.0, 0.005), 3)
    tau_high = next((t for t in grid if (p >= t).any() and y[p >= t].mean() >= target), 1.01)
    tau_low = next((t for t in grid[::-1] - 0.495 if (p <= t).any() and 1 - y[p <= t].mean() >= target), -0.01)
    return float(tau_high), float(round(tau_low, 3))


def route(p_match: np.ndarray, conflict: np.ndarray, invalid: np.ndarray,
          tau_high: float, tau_low: float) -> np.ndarray:
    decision = np.where(p_match >= tau_high, "MATCH", np.where(p_match <= tau_low, "NON_MATCH", "REVIEW"))
    # Un conflicto duro nunca se auto-une; un juicio inválido de Laya nunca decide solo.
    decision = np.where(conflict & (decision == "MATCH"), "REVIEW", decision)
    return np.where(invalid, "REVIEW", decision)


def run_config(df: pd.DataFrame, laya_col: str | None) -> tuple[pd.DataFrame, dict]:
    """Entrena en dev, fija umbrales en dev y decide para todos los pares."""
    dev = df[df["split"] == "dev"]
    model = fit(dev, laya_col)
    p = model.predict_proba(design_matrix(df, laya_col))[:, 1]
    dev_mask = (df["split"] == "dev").to_numpy()
    tau_high, tau_low = tune_thresholds(p[dev_mask], df["is_match"].to_numpy()[dev_mask])
    invalid = df[laya_col].isna().to_numpy() if laya_col else np.zeros(len(df), bool)
    decision = route(p, df["birth_date_conflict"].to_numpy(), invalid, tau_high, tau_low)

    rows = [PairResult(pair_id=pid, p_same=None if (not laya_col or pd.isna(ps)) else float(ps),
                       p_match=float(pm), decision=d).model_dump(mode="json")
            for pid, ps, pm, d in zip(df["pair_id"], df[laya_col] if laya_col else [None] * len(df), p, decision)]
    return pd.DataFrame(rows), {"tau_high": tau_high, "tau_low": tau_low}
