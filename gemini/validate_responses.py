import os
import pandas
from pprint import pprint

import extract_gemini
import merge_gemini

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
missing_file = []
missing_field = {k: [] for k in EXPECTED_COLUMNS}
wrong_ids = {}


def validate_entry(idx):
    fname = extract_gemini.id_for_filename(idx)
    path = f"{response_dir}{fname}.json"

    if not os.path.exists(path):
        missing_file.append(fname)
        return

    try:
        j = merge_gemini.read_json(path)
        assert len(j) == 1
        if j["abstract ID"][0] != idx:
            wrong_ids[idx] = fname
    except Exception as e:
        print(path)
        raise e
    for c in EXPECTED_COLUMNS:
        if c not in j.columns:
            missing_field[c].append(fname)


data = pandas.read_csv("outputs/basic-processing/merged-abstracts.csv")
data["dedup_index"].apply(validate_entry)

for c in EXPECTED_COLUMNS:
    print(f"Number of files missing '{c}': {len(missing_field[c])}")
print(f"Missing files {len(missing_file)}")
print(missing_file)
print("Wrong IDs - correct : filename")
pprint(wrong_ids)

