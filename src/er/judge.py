"""Laya: P(misma persona) por par. Salida validada; un error nunca produce un match."""
import math
import os

CHECKPOINTS = {  # nombre -> kwargs de laya.load (validado en el spike, laya==0.3.20)
    "english": {"model_id_or_path": "convaiinnovations/laya"},
    "multilingual": {"model_id_or_path": "convaiinnovations/laya", "subfolder": "multilingual"},
}

QUESTIONS = {
    "same_entity": {
        "type": "noul",
        "instructions": "Do Record A and Record B describe the same real person?",
        "criteria": {"false": "different people", "true": "the same person"},
        "labels": {"false": "B", "true": "A"},  # etiquetas neutras: evita que el modelo siga 'true'/'false' literal
    }
}

STATE_FIELDS = ["full_name_norm", "email", "phone_e164", "birth_date", "city", "address"]
_LABELS = {"full_name_norm": "name", "phone_e164": "phone", "birth_date": "birth"}


def record_text(rec: dict) -> str:
    """Solo campos normalizados; nunca IDs."""
    return "; ".join(f"{_LABELS.get(f, f)}={rec.get(f) or ''}" for f in STATE_FIELDS)


def extract_p_true(answer) -> float | None:
    """Lee P(true) de una respuesta `noul`. Devuelve None si la forma no es la esperada."""
    try:
        p = float(answer["noul"])
    except (KeyError, TypeError, ValueError):
        return None
    return p if math.isfinite(p) and 0.0 <= p <= 1.0 else None


def load(checkpoint: str):
    import laya
    import torch

    torch.set_num_threads(int(os.environ.get("ER_TORCH_THREADS", "4")))
    return laya.load(device="cpu", **CHECKPOINTS[checkpoint])


def judge(agent, lefts: list[dict], rights: list[dict]) -> list[float | None]:
    states = [{"record_a": record_text(a), "record_b": record_text(b)} for a, b in zip(lefts, rights)]
    results = agent.predict_batch(states, QUESTIONS, batch_size=64)
    if len(results) != len(states):
        return [None] * len(states)
    return [extract_p_true(r.get("answers", {}).get("same_entity")) for r in results]
