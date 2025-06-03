import re
import sys

import pandas


def read_single(path):
    print(f"Reading {path}")
    data = pandas.read_excel(path, "citations")
    data = data[
        ["UI", "TI", "DO", "AU", "JN", "CP", "AB", "PT", "LG", "YR"]
    ]
    data = data.rename(
        columns={
            "UI": "pmid",
            "TI": "title",
            "DO": "doi",
            "AU": "authors",
            "JN": "journal",
            "CP": "country",
            "AB": "abstract",
            "PT": "publication types",
            "LG": "language",
            "YR": "year",
        }
    )
    data["year"] = data["year"].apply(tidy_year)
    return data


def tidy_year(s):
    if pandas.isna(s) or not isinstance(s, str):
        return s
    year = re.findall(r"(19\d{2}|20\d{2})", s)
    if len(year) == 0:
        return s
    return year[0]


def get_data(path, num_files):
    data = []
    for i in range(num_files):
        data.append(read_single(f"{path}/citation({i}).xls"))

    return pandas.concat(data)


def main():
    get_data("database-search-results/OVID-Medline", 8).to_csv("outputs/database-search-results//medline.csv")
    get_data("database-search-results/Embase", 7).to_csv("outputs/database-search-results//embase.csv")


if __name__ == "__main__":
    sys.exit(main())
