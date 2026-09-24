"""Genera dos fuentes sintéticas (CRM y Billing) con ruido realista y ground truth.

Cada persona real se proyecta a una o más filas por fuente. Cada proyección se
perturba de forma independiente (typos, apodos, formatos, campos faltantes...).
Un porcentaje de personas tiene un "homónimo": otra persona con el mismo nombre
y ciudad, que el pipeline NO debe unir.
"""
import csv
import hashlib
import random
import string
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from pathlib import Path

from faker import Faker

NICKNAMES = {
    "Roberto": "Beto", "Francisco": "Pancho", "José": "Pepe", "Guillermo": "Memo",
    "Alejandro": "Alejo", "Ignacio": "Nacho", "Eduardo": "Lalo", "Manuel": "Manolo",
    "Enrique": "Quique", "Rafael": "Rafa", "Antonio": "Toño", "Dolores": "Lola",
    "Graciela": "Chela", "María José": "Majo", "Guadalupe": "Lupe", "Mercedes": "Meche",
    "Rosario": "Charo", "Isabel": "Chabela", "Santiago": "Santi", "Florencia": "Flor",
    "Agustina": "Agus", "Valentina": "Valen", "Federico": "Fede", "Gonzalo": "Gonza",
}

CITIES = {  # ciudad -> código de área (móviles AR: área + abonado = 10 dígitos)
    "Buenos Aires": "11", "Córdoba": "351", "Rosario": "341", "Mendoza": "261",
    "La Plata": "221", "Mar del Plata": "223", "Salta": "387", "Tucumán": "381",
    "Santa Fe": "342", "Neuquén": "299",
}
PROVIDERS = ["gmail.com", "hotmail.com", "yahoo.com.ar", "outlook.com"]


@dataclass(frozen=True)
class Person:
    person_id: str
    given: str
    family1: str
    family2: str
    birth: date
    email: str
    phone_area: str
    phone_number: str
    city: str
    street: str
    number: int


def _typo(s: str, rng: random.Random) -> str:
    if len(s) < 4:
        return s
    i = rng.randrange(1, len(s) - 1)
    op = rng.choice(["swap", "drop", "replace"])
    if op == "swap":
        return s[:i] + s[i + 1] + s[i] + s[i + 2:]
    if op == "drop":
        return s[:i] + s[i + 1:]
    return s[:i] + rng.choice(string.ascii_lowercase) + s[i + 1:]


def _strip_accents(s: str) -> str:
    return s.translate(str.maketrans("áéíóúÁÉÍÓÚñÑ", "aeiouAEIOUnN"))


_TAKEN_EMAILS: set[str] = set()


def _email_for(given: str, family: str, rng: random.Random) -> str:
    """Email único por persona: dos personas distintas nunca comparten cuenta."""
    g = _strip_accents(given.split()[0]).lower()
    f = _strip_accents(family).lower().replace(" ", "")
    while True:
        local = rng.choice([f"{g}.{f}", f"{g}{f}", f"{g[0]}{f}", f"{g}.{f}{rng.randint(1, 99)}"])
        domain = rng.choice(PROVIDERS)
        key = f"{local.replace('.', '') if domain == 'gmail.com' else local}@{domain}"  # como lo normaliza Silver
        if key not in _TAKEN_EMAILS:
            _TAKEN_EMAILS.add(key)
            return f"{local}@{domain}"


def _new_phone(area: str, rng: random.Random) -> str:
    return "".join(rng.choice(string.digits[1:]) for _ in range(10 - len(area)))


def _format_phone(area: str, number: str, rng: random.Random) -> str:
    half = len(number) // 2
    return rng.choice([
        f"+54 9 {area} {number[:half]}-{number[half:]}",
        f"+549{area}{number}",
        f"54 9 {area} {number}",
        f"(0{area}) 15-{number[:half]}-{number[half:]}",
    ])


def _make_person(pid: str, fake: Faker, rng: random.Random, given=None, family1=None,
                 family2=None, city=None) -> Person:
    given = given or (rng.choice(list(NICKNAMES)) if rng.random() < 0.35 else fake.first_name())
    family1 = family1 or fake.last_name()
    family2 = family2 or fake.last_name()
    city = city or rng.choice(list(CITIES))
    area = CITIES[city]
    return Person(
        person_id=pid, given=given, family1=family1, family2=family2,
        birth=date(1950, 1, 1) + timedelta(days=rng.randrange(0, 365 * 55)),
        email=_email_for(given, family1, rng), phone_area=area,
        phone_number=_new_phone(area, rng), city=city,
        street=fake.street_name(), number=rng.randint(1, 9999),
    )


def _perturb(p: Person, fake: Faker, rng: random.Random) -> dict:
    """Una vista ruidosa de la persona, como la vería un sistema fuente."""
    given = NICKNAMES.get(p.given, p.given) if rng.random() < 0.15 else p.given
    given = _typo(given, rng) if rng.random() < 0.05 else given
    family1 = _typo(p.family1, rng) if rng.random() < 0.12 else p.family1
    family = family1 if rng.random() < 0.35 else f"{family1} {p.family2}"

    email = p.email
    r = rng.random()
    if r < 0.12:
        email = ""
    elif r < 0.27:
        email = _email_for(p.given, p.family1, rng)          # otra cuenta
    elif r < 0.42:
        local, domain = email.split("@")
        email = f"{local}+{rng.choice(['billing', 'crm', 'x'])}@{domain}"  # alias
    if email and rng.random() < 0.2:
        email = email.upper()

    number = p.phone_number
    r = rng.random()
    phone = ""
    if r >= 0.15:
        if r < 0.23:
            number = _new_phone(p.phone_area, rng)            # cambió de número
        phone = _format_phone(p.phone_area, number, rng)

    street, num = p.street, p.number
    if rng.random() < 0.10:                                   # se mudó
        street, num = fake.street_name(), rng.randint(1, 9999)
    address = f"{street} {num}"
    if rng.random() < 0.3:
        address = address.replace("Avenida", "Av.").replace("Calle ", "")
    if rng.random() < 0.2:
        address += f" piso {rng.randint(1, 12)}"

    birth = "" if rng.random() < 0.10 else (
        p.birth.isoformat() if rng.random() < 0.6 else p.birth.strftime("%d/%m/%Y"))

    if rng.random() < 0.2:
        given, family = given.upper(), family.upper()
    if rng.random() < 0.3:
        given, family = _strip_accents(given), _strip_accents(family)

    updated = datetime(2023, 1, 1) + timedelta(minutes=rng.randrange(0, 60 * 24 * 365 * 3))
    return dict(given=given, family=family, email=email, phone=phone, birth=birth,
                address=address, city=p.city, updated_at=updated.isoformat(timespec="seconds"))


def split_for(person_id: str) -> str:
    return "dev" if int(hashlib.md5(person_id.encode()).hexdigest(), 16) % 10 < 6 else "test"


def generate(out_dir: str, n_people: int = 1500, seed: int = 42) -> dict:
    rng = random.Random(seed)
    _TAKEN_EMAILS.clear()
    fake = Faker(["es_AR", "es_MX"])
    Faker.seed(seed)  # a nivel de clase: también fija la elección de locale

    people = [_make_person(f"P{i:05d}", fake, rng) for i in range(n_people)]
    # Homónimos: mismo nombre completo y ciudad, persona distinta.
    for p in rng.sample(people, k=int(n_people * 0.06)):
        people.append(_make_person(f"P{len(people):05d}", fake, rng, given=p.given,
                                   family1=p.family1, family2=p.family2, city=p.city))

    crm, billing, truth = [], [], []
    for p in people:
        for _ in range(2 if rng.random() < 0.08 else 1):
            v = _perturb(p, fake, rng)
            cid = f"C{len(crm) + 1:05d}"
            crm.append(dict(crm_id=cid, first_name=v["given"], last_name=v["family"],
                            email=v["email"], phone=v["phone"], birth_date=v["birth"],
                            address=v["address"], city=v["city"], updated_at=v["updated_at"]))
            truth.append(dict(entity_key=f"crm:{cid}", true_entity_id=p.person_id,
                              split=split_for(p.person_id)))
        if rng.random() < 0.87:
            for _ in range(2 if rng.random() < 0.08 else 1):
                v = _perturb(p, fake, rng)
                bid = f"B{len(billing) + 1:05d}"
                full = (f"{v['family']}, {v['given']}" if rng.random() < 0.5
                        else f"{v['given']} {v['family']}")
                billing.append(dict(billing_id=bid, full_name=full, email=v["email"],
                                    phone=v["phone"], billing_address=v["address"],
                                    city=v["city"], updated_at=v["updated_at"]))
                truth.append(dict(entity_key=f"billing:{bid}", true_entity_id=p.person_id,
                                  split=split_for(p.person_id)))

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for name, rows in [("crm_users", crm), ("billing_users", billing), ("ground_truth", truth)]:
        with open(out / f"{name}.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
    return {"people": len(people), "crm_rows": len(crm), "billing_rows": len(billing)}
