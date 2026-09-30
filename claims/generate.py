"""Synthetic members, providers and claims, with deliberate defects. No real PHI."""
from __future__ import annotations

import random
from datetime import date, timedelta

FIRST = ["Alex", "Sam", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Jamie"]
LAST = ["Lee", "Patel", "Garcia", "Smith", "Nguyen", "Brown", "Khan", "Lopez"]
ICD10 = ["E11.9", "I10", "J45.909", "M54.5", "K21.9", "F41.1", "N39.0", "R51.9"]
CPT = ["99213", "99214", "80053", "93000", "71046", "36415"]
SPECIALTY = ["Family Medicine", "Cardiology", "Radiology", "Laboratory", "Internal Medicine"]
PAYERS = ["PAYER_A", "PAYER_B", "PAYER_C"]

DEFECTS = ["negative_amount", "paid_gt_billed", "bad_icd", "future_service", "orphan_member",
           "paid_before_service", "missing_npi"]


def generate(n_members=200, n_providers=30, n_claims=2000, defect_rate=0.05, seed=11, n_duplicates=20):
    rnd = random.Random(seed)
    members = [{
        "member_id": f"M{i:06d}", "first_name": rnd.choice(FIRST), "last_name": rnd.choice(LAST),
        "ssn": f"{rnd.randint(100, 899)}-{rnd.randint(10, 99)}-{rnd.randint(1000, 9999)}",
        "dob": (date(1945, 1, 1) + timedelta(days=rnd.randint(0, 25000))).isoformat(),
        "zip": f"{rnd.randint(10000, 99999)}", "payer": rnd.choice(PAYERS),
    } for i in range(n_members)]
    providers = [{
        "npi": f"{1000000000 + i * 7919}", "provider_name": f"Clinic {i:03d}",
        "specialty": rnd.choice(SPECIALTY), "state": rnd.choice(["TX", "MO", "KS", "IL"]),
    } for i in range(n_providers)]
    claims, injected = [], {}
    for i in range(n_claims):
        svc = date(2025, 1, 1) + timedelta(days=rnd.randint(0, 364))
        billed = round(rnd.uniform(40, 2500), 2)
        c = {"claim_id": f"CLM{i:08d}", "member_id": rnd.choice(members)["member_id"],
             "npi": rnd.choice(providers)["npi"], "service_date": svc.isoformat(),
             "paid_date": (svc + timedelta(days=rnd.randint(7, 45))).isoformat(),
             "icd10": rnd.choice(ICD10), "cpt": rnd.choice(CPT), "billed_amount": billed,
             "paid_amount": round(billed * rnd.uniform(0.3, 0.9), 2)}
        if rnd.random() < defect_rate:
            d = rnd.choice(DEFECTS)
            injected[c["claim_id"]] = d
            if d == "negative_amount": c["billed_amount"] = -abs(billed)
            elif d == "paid_gt_billed": c["paid_amount"] = round(billed * 1.5, 2)
            elif d == "bad_icd": c["icd10"] = "99X"
            elif d == "future_service": c["service_date"] = "2099-01-01"; c["paid_date"] = "2099-02-01"
            elif d == "orphan_member": c["member_id"] = "M999999"
            elif d == "paid_before_service": c["paid_date"] = (svc - timedelta(days=3)).isoformat()
            elif d == "missing_npi": c["npi"] = None
        claims.append(c)
    # exact duplicate resubmissions (should be deduplicated, not quarantined)
    clean = [c for c in claims if c["claim_id"] not in injected]
    dups = [dict(c) for c in rnd.sample(clean, min(n_duplicates, len(clean)))]
    return members, providers, claims + dups, injected
