import json
import os
import re
import sys
import pandas

def insert_results(pubmed_data, gemini_data):
    if gemini_data["abstract ID"].dtype == object:
        gemini_data["abstract ID"] = gemini_data["abstract ID"].str.lower()
    gemini_data.set_index("abstract ID", inplace=True)
    pubmed_data.update(gemini_data)


def tidy_string(s):
    if isinstance(s, list):
        s = ",".join(s)
    if pandas.isna(s):
        return s
    return re.sub(r"\s+", " ", s)


def tidy_list_of_strings(l):
    if isinstance(l, list):
        return [tidy_string(s) for s in l]
    return [tidy_string(l)]


def tidy_strings(str_columns, list_columns, data):
    for col_name in str_columns:
        print(f"Tidying {col_name}")
        data[col_name] = data[col_name].apply(lambda s: tidy_string(s))
    for col_name in list_columns:
        print(f"Tidying {col_name}")
        data[col_name] = data[col_name].apply(tidy_list_of_strings)


def read_json(path):
    # Read the corrected file.
    try:
        return pandas.read_json(path)
    except Exception as e:
        print(f"Failed to parse json in {path}. Trying json decoder for more info")
        with open(path, "r") as f:
            json.load(f)
        raise e


def main():
    data = pandas.read_csv("outputs/basic-processing/merged-abstracts.csv")
    data.set_index("dedup_index", inplace=True)

    list_columns = [
        "exposure",
        "outcome",
    ]
    str_columns = [
        "follow-up time",
        "study type",
        "cohort",
        "finding",
        "mod_finding",
        "country",
        "gest_age",
        "followup_time",
        "birth_weight",
        "added_2025",
    ]
    for column in list_columns + str_columns:
        data[column] = None

    dirname = "outputs/gemini-responses/"
    fnames = os.listdir(dirname)
    fnames.sort()

    missing = []
    num_merged = 0
    for fname in fnames:
        if not fname.endswith("json"):
            continue
        f = dirname + fname
        if os.path.exists(f):
            gemini_data = read_json(dirname + fname)
            insert_results(data, gemini_data)
        else:
            missing.append(i)
            print(f"Couldn't find file {i}")
        num_merged += 1
        if num_merged % 100 == 0:
            print(f"Merged {num_merged}/{len(fnames)} files")
    print(missing)

    tidy_strings(str_columns, list_columns, data)
    data.to_csv("outputs/merged-abstracts-gemini-appended.csv")


if __name__ == "__main__":
    sys.exit(main())
