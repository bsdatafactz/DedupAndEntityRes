import csv, os, random
from datetime import date, timedelta
from faker import Faker

random.seed(42)
Faker.seed(42)
fake = Faker("en_US")

N_PEOPLE = 1100
SOURCES = ["core_banking", "credit_card", "loan"]
NICK = {"William": ["Bill", "Will"], "Robert": ["Bob", "Rob"], "James": ["Jim", "Jimmy"],
        "Elizabeth": ["Liz", "Beth"], "Michael": ["Mike"], "Jennifer": ["Jen", "Jenny"],
        "Richard": ["Rick"], "Katherine": ["Kate", "Katie"], "Thomas": ["Tom"],
        "Margaret": ["Maggie", "Meg"]}
STATES = {"CA": "California", "TX": "Texas", "NY": "New York", "FL": "Florida", "IL": "Illinois"}
DOMAINS = ["gmail.com", "yahoo.com", "outlook.com"]


def typo(s):
    if len(s) < 4:
        return s
    i = random.randrange(1, len(s) - 1)
    return s[:i] + s[i + 1:]


def make_person(pid):
    first = random.choice(list(NICK)) if random.random() < 0.3 else fake.first_name()
    last = fake.last_name()
    st = random.choice(list(STATES))
    return {
        "true_person_id": pid, "first": first, "last": last,
        "dob": date(1950, 1, 1) + timedelta(days=random.randint(0, 20000)),
        "ssn": fake.ssn(),
        "phone": f"{random.randint(200,999)}{random.randint(200,999)}{random.randint(0,9999):04d}",
        "email": f"{first}.{last}{random.randint(1,99)}@{random.choice(DOMAINS)}".lower(),
        "street": fake.street_address(), "city": fake.city(), "state": st,
        "zip": f"{random.randint(10000, 99999)}",
    }


def messy_name(p):
    first = p["first"]
    if first in NICK and random.random() < 0.35:
        first = random.choice(NICK[first])
    if random.random() < 0.05:
        first = typo(first)
    style = random.choice(["fl", "FL", "lf", "spaces"])
    return {"fl": f"{first} {p['last']}", "FL": f"{first} {p['last']}".upper(),
            "lf": f"{p['last']}, {first}", "spaces": f"  {first}   {p['last']} "}[style]


def messy_phone(ph):
    a, b, c = ph[:3], ph[3:6], ph[6:]
    return random.choice([f"{a}-{b}-{c}", f"({a}) {b}-{c}", ph, f"+1 {a}.{b}.{c}"])


def messy_street(s):
    r = random.random()
    if r < 0.3:
        return s.replace("Street", "St").replace("Avenue", "Ave").replace("Road", "Rd")
    if r < 0.5:
        return s.upper()
    return s


def make_record(p, source, src_cust_id):
    moved = random.random() < 0.10  # some customers moved: different address in this system
    street = fake.street_address() if moved else p["street"]
    return {
        "source_system": source,
        "source_customer_id": src_cust_id,
        "full_name": messy_name(p),
        "dob": p["dob"].strftime(random.choice(["%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y"])),
        "ssn": None if random.random() < 0.15 else random.choice([p["ssn"], p["ssn"].replace("-", "")]),
        "phone": None if random.random() < 0.10 else messy_phone(p["phone"]),
        "email": None if random.random() < 0.15 else random.choice([p["email"], p["email"].upper()]),
        "address": messy_street(street),
        "city": p["city"],
        "state": random.choice([p["state"], STATES[p["state"]]]),
        "zip": random.choice([p["zip"], p["zip"] + "-" + f"{random.randint(0,9999):04d}"]),
        "ingested_at": (date(2025, 1, 1) + timedelta(days=random.randint(0, 600))).isoformat(),
        "_true_person_id": p["true_person_id"],
    }


rows = []
for i in range(N_PEOPLE):
    p = make_person(f"P{i+1:05d}")
    n = random.choices([1, 2, 3], weights=[0.4, 0.4, 0.2])[0]
    for src in random.sample(SOURCES, n):
        rec = make_record(p, src, f"{src[:2].upper()}-{random.randint(10000, 99999)}")
        rows.append(rec)
        if random.random() < 0.05:           # exact duplicate inside the same source
            rows.append(dict(rec))

random.shuffle(rows)
os.makedirs("data/raw", exist_ok=True)
cols = ["bronze_id", "source_system", "source_customer_id", "full_name", "dob", "ssn",
        "phone", "email", "address", "city", "state", "zip", "ingested_at"]
with open("data/raw/bronze_raw.csv", "w", newline="") as f, \
     open("data/raw/ground_truth.csv", "w", newline="") as g:
    w = csv.DictWriter(f, fieldnames=cols)
    gw = csv.writer(g)
    w.writeheader()
    gw.writerow(["bronze_id", "true_person_id"])
    for i, r in enumerate(rows, 1):
        bid = f"B{i:05d}"
        w.writerow({"bronze_id": bid, **{k: r[k] for k in cols if k != "bronze_id"}})
        gw.writerow([bid, r["_true_person_id"]])
print(f"{len(rows)} bronze rows, {N_PEOPLE} true people")
