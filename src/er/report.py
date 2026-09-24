"""Métricas contra ground truth (split test) y reports/benchmark.md."""
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score


def true_pair_counts(truth: pd.DataFrame) -> dict[str, int]:
    sizes = truth.groupby(["split", "true_entity_id"]).size()
    return {s: int((n * (n - 1) // 2).sum()) for s, n in sizes.groupby(level=0)}


def config_metrics(dec: pd.DataFrame, total_true_test: int) -> dict:
    t = dec[dec["split"] == "test"]
    match = t["decision"] == "MATCH"
    non = t["decision"] == "NON_MATCH"
    tp = int((match & t["is_match"]).sum())
    return {
        "pairs_test": len(t),
        "pct_match": match.mean(), "pct_non_match": non.mean(),
        "pct_review": (t["decision"] == "REVIEW").mean(),
        "precision_match": tp / match.sum() if match.any() else float("nan"),
        "npv_non_match": (~t.loc[non, "is_match"]).mean() if non.any() else float("nan"),
        "recall_auto": tp / total_true_test if total_true_test else float("nan"),
        "automation": (match | non).mean(),
    }


def cascade_metrics(labeled: pd.DataFrame, ckpt: str, timing: pd.DataFrame) -> dict:
    """Sobre test: la cola REVIEW de las reglas y qué hizo Laya con ella."""
    rules = labeled[(labeled["config"] == "rules") & (labeled["split"] == "test")].set_index("pair_id")
    casc = labeled[labeled["config"] == f"rules+laya_{ckpt}"].set_index("pair_id")["decision"]
    queue = rules[rules["decision"] == "REVIEW"].assign(after=lambda d: d.index.map(casc))
    resolved = queue[queue["after"] != "REVIEW"]
    correct = ((resolved["after"] == "MATCH") & resolved["is_match"]) | \
              ((resolved["after"] == "NON_MATCH") & ~resolved["is_match"])
    laya_s = float((timing["load_s"] + timing["infer_s"]).sum())
    judged = int(timing["pairs"].sum())
    return {
        "queue": len(queue), "queue_true_matches": int(queue["is_match"].sum()),
        "resolved": len(resolved), "resolved_pct": len(resolved) / len(queue) if len(queue) else float("nan"),
        "resolved_match": int((resolved["after"] == "MATCH").sum()),
        "resolved_non_match": int((resolved["after"] == "NON_MATCH").sum()),
        "errors": int((~correct).sum()),
        "precision_resolved": correct.mean() if len(resolved) else float("nan"),
        "pairs_judged_all_splits": judged, "laya_seconds": laya_s,
        "laya_s_per_pair": laya_s / judged if judged else float("nan"),
    }


def laya_metrics(scores: pd.DataFrame, timing: pd.DataFrame, cost_per_hour: float) -> dict:
    out = {}
    for ckpt, s in scores.groupby("checkpoint"):
        t = s[(s["split"] == "test") & s["p_same"].notna()]
        tm = timing[timing["checkpoint"] == ckpt]
        infer_s, pairs = float(tm["infer_s"].sum()), int(tm["pairs"].sum())
        out[ckpt] = {
            "auc_test": roc_auc_score(t["is_match"], t["p_same"]) if t["is_match"].nunique() == 2 else float("nan"),
            "invalid": int(s["p_same"].isna().sum()),
            "batches": len(tm),
            "p50_batch_s": float(tm["infer_s"].median()) if len(tm) else float("nan"),
            "load_s_avg": float(tm["load_s"].mean()) if len(tm) else float("nan"),
            "infer_s_total": infer_s,
            "pairs_per_s": pairs / infer_s if infer_s else float("nan"),
            "sec_per_1k": 1000 * infer_s / pairs if pairs else float("nan"),
            "usd_per_1k": (1000 * infer_s / pairs) / 3600 * cost_per_hour if pairs else float("nan"),
        }
    return out


def _f(x, pct=False, nd=3):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    return f"{100 * x:.1f} %" if pct else f"{x:.{nd}f}"


def render(ctx: dict) -> str:
    L, C = ctx["laya"], ctx["cascade"]
    lines = [
        "# Benchmark — Entity Resolution PoC",
        "",
        f"Run `{ctx['run_id']}` · {ctx['generated_at']} · config que alimenta Gold: **{ctx['primary']}** · "
        f"CPU (Docker, {ctx['hardware']}) · duración del run: **{ctx['run_seconds'] / 60:.1f} min**",
        "",
        "Cascada: las reglas deciden todos los pares; Laya juzga solo los que las reglas dejan en REVIEW.",
        "Métricas sobre el split *test*; entrenamiento y umbrales sobre *dev*.",
        "",
        "## Cascada: qué hizo Laya con la cola de REVIEW (test)",
        "",
        "| Checkpoint | Pares en cola | Matches reales en cola | Resueltos por Laya | MATCH / NON_MATCH | Errores | Precisión de lo resuelto | Tiempo Laya (todos los splits) | s / par |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for ckpt, m in C.items():
        lines.append(
            f"| {ckpt} | {m['queue']} | {m['queue_true_matches']} | {m['resolved']} ({_f(m['resolved_pct'], True)}) | "
            f"{m['resolved_match']} / {m['resolved_non_match']} | {m['errors']} | {_f(m['precision_resolved'])} | "
            f"{m['laya_seconds']:.0f} s ({m['pairs_judged_all_splits']} pares) | {_f(m['laya_s_per_pair'], nd=2)} |")
    lines += [
        "",
        "## Resultado por configuración (test)",
        "",
        "| Config | Precisión MATCH | NPV NON_MATCH | Recall auto | Automatización | MATCH / NON_MATCH / REVIEW | τ_high / τ_low |",
        "|---|---|---|---|---|---|---|",
    ]
    for name, m in ctx["configs"].items():
        th = ctx["thresholds"].get(name) or {}
        ths = f"{th['tau_high']:.3f} / {th['tau_low']:.3f}" if th else "—"
        lines.append(
            f"| {name} | {_f(m['precision_match'])} | {_f(m['npv_non_match'])} | {_f(m['recall_auto'])} | "
            f"{_f(m['automation'], pct=True)} | {_f(m['pct_match'], True)} / {_f(m['pct_non_match'], True)} / "
            f"{_f(m['pct_review'], True)} | {ths} |")
    lines += [
        "",
        "Para `rules+laya_*`, τ son los umbrales de la etapa Laya (entrenada solo sobre la cola de dev).",
        "",
        "## Laya: rendimiento",
        "",
        "| Checkpoint | AUC p_same en la cola (test) | Respuestas inválidas | Lotes | Carga modelo | Pares/s | USD / 1k pares* |",
        "|---|---|---|---|---|---|---|",
    ]
    for ckpt, m in L.items():
        lines.append(
            f"| {ckpt} | {_f(m['auc_test'])} | {m['invalid']} | {m['batches']} | {_f(m['load_s_avg'], nd=1)} s | "
            f"{_f(m['pairs_per_s'], nd=1)} | ${_f(m['usd_per_1k'], nd=4)} |")
    lines += [
        "",
        f"\\* Tiempo de inferencia × US${ctx['cost_per_hour']:.2f}/h, param `hw_cost_usd_per_hour`.",
        "",
        "## Pipeline",
        "",
        "| Métrica | Valor |",
        "|---|---|",
        f"| Entidades en Silver | {ctx['entities']} |",
        f"| Pares candidatos | {ctx['candidates']} |",
        f"| Blocking recall | {_f(ctx['blocking_recall'])} |",
        f"| Masters en Gold | {ctx['masters']} |",
        f"| Masters con revisión pendiente | {ctx['pending_masters']} |",
        f"| Hash de Gold | `{ctx['gold_hash']}` |",
        f"| Idéntico al run anterior | {ctx['idempotent']} |",
        "",
    ]
    return "\n".join(lines)


def write(ctx: dict, reports_dir: str) -> str:
    Path(reports_dir).mkdir(parents=True, exist_ok=True)
    md = render(ctx)
    (Path(reports_dir) / "benchmark.md").write_text(md, encoding="utf-8")
    (Path(reports_dir) / "benchmark.json").write_text(json.dumps(ctx, indent=2, default=float), encoding="utf-8")
    return md


def now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
