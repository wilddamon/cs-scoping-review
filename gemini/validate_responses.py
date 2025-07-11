import os
import pandas

EXPECTED_COLUMNS = [
        "abstract ID",
        "exposure",
        "outcome",
        "cohort",
        "finding",
        "mod_finding",
        "country",
        "followup_time",
        "cross-section timing",
        "birth_weight",
        "gest_age",
]



response_dir = "outputs/gemini-responses/"

fnames = os.listdir(response_dir)
print(f"Found {len(fnames)} files")

missing_field = {k: [] for k in EXPECTED_COLUMNS}
for fname in fnames:
    if not fname.endswith("json"):
        continue

    j = pandas.read_json(f"{response_dir}{fname}")
    for c in EXPECTED_COLUMNS:
        if c not in j.columns:
            missing_field[c].append(fname)

for c in EXPECTED_COLUMNS:
    print(f"Number of files missing '{c}': {len(missing_field[c])}")

