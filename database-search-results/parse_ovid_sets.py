import re
import sys

import pandas


def read_single(path, has_pmid):
    print(f"Reading {path}")
    data = pandas.read_excel(path, sheet_name="citations", header=1, engine='xlrd')
    if has_pmid:
        data = data[
            ["UI", "TI", "DO", "AU", "JN", "AB", "PT", "LG", "YR"]
        ]
        data = data.rename(
            columns={
                "UI": "pmid",
                })
    else:
        data = data[
            ["TI", "DO", "AU", "JN", "AB", "PT", "LG", "YR"]
        ]
    data = data.rename(
        columns={
            "TI": "title",
            "DO": "doi",
            "AU": "authors",
            "JN": "journal",
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


def get_data(path, num_files, has_pmid=True):
    data = []
    for i in range(num_files):
        data.append(read_single(f"{path}/citation({i}).xls", has_pmid))

    return pandas.concat(data)


def main():
    get_data("database-search-results/OVID-Medline", 6).to_csv("outputs/database-search-results/medline.csv")
    get_data("database-search-results/Embase", 4).to_csv("outputs/database-search-results/embase.csv")
    get_data("database-search-results/PsycINFO", 1, has_pmid=False).to_csv("outputs/database-search-results/psycinfo.csv")


if __name__ == "__main__":
    sys.exit(main())
