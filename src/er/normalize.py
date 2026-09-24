"""Bronze -> Silver: esquema común y valores normalizados. Funciones puras."""
import re
from datetime import datetime

import jellyfish
import pandas as pd
import phonenumbers
from unidecode import unidecode

_NON_ALPHA = re.compile(r"[^a-z0-9 ]+")
_SPACES = re.compile(r"\s+")
_FLOOR = re.compile(r"\bpiso \d+\b")


def norm_text(s) -> str | None:
    if not isinstance(s, str) or not s.strip():
        return None
    s = _NON_ALPHA.sub(" ", unidecode(s).lower())
    return _SPACES.sub(" ", s).strip() or None


def display(s) -> str | None:
    if not isinstance(s, str) or not s.strip():
        return None
    return _SPACES.sub(" ", s).strip().title()


def norm_email(s) -> str | None:
    if not isinstance(s, str) or "@" not in s:
        return None
    local, domain = s.strip().lower().split("@", 1)
    local = local.split("+", 1)[0]
    if domain == "gmail.com":
        local = local.replace(".", "")
    return f"{local}@{domain}" if local else None


def norm_phone(s, region: str = "AR") -> str | None:
    if not isinstance(s, str) or not s.strip():
        return None
    try:
        n = phonenumbers.parse(s, region)
    except phonenumbers.NumberParseException:
        return None
    if not phonenumbers.is_valid_number(n):
        return None
    return phonenumbers.format_number(n, phonenumbers.PhoneNumberFormat.E164)


def norm_date(s):
    if not isinstance(s, str) or not s.strip():
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            pass
    return None


def norm_address(s) -> str | None:
    t = norm_text(s)
    if not t:
        return None
    t = _FLOOR.sub("", f" {t} ").replace(" av ", " avenida ").replace(" calle ", " ")
    return _SPACES.sub(" ", t).strip() or None


def split_full_name(full: str) -> tuple[str, str]:
    """'Apellido, Nombre' o 'Nombre Apellido...' -> (given, family)."""
    full = full.strip()
    if "," in full:
        family, given = full.split(",", 1)
        return given.strip(), family.strip()
    parts = full.split()
    return parts[0], " ".join(parts[1:])


def _entities(df: pd.DataFrame, source: str, id_col: str) -> pd.DataFrame:
    family_norm = df["family"].map(norm_text)
    return pd.DataFrame({
        "entity_key": source + ":" + df[id_col],
        "source": source,
        "source_id": df[id_col],
        "given_name": df["given"].map(display),
        "family_name": df["family"].map(display),
        "full_name_norm": (df["given"].fillna("") + " " + df["family"].fillna("")).map(norm_text),
        "family_soundex": family_norm.map(lambda f: jellyfish.soundex(f.split()[0]) if f else None),
        "email": df["email"].map(norm_email),
        "phone_e164": df["phone"].map(norm_phone),
        "birth_date": df["birth_date"].map(norm_date) if "birth_date" in df else None,
        "city": df["city"].map(norm_text),
        "address": df["address"].map(norm_address),
        "updated_at": pd.to_datetime(df["updated_at"], errors="coerce"),
    })


def crm_to_entities(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.rename(columns={"first_name": "given", "last_name": "family"})
    return _entities(df, "crm", "crm_id")


def billing_to_entities(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.rename(columns={"billing_address": "address"}).copy()
    names = df["full_name"].fillna("").map(split_full_name)
    df["given"] = names.map(lambda t: t[0])
    df["family"] = names.map(lambda t: t[1])
    return _entities(df, "billing", "billing_id")
