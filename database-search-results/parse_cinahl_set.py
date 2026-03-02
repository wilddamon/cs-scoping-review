import re
import sys

import pandas


def read_single(path):
    scopus_data = pandas.read_csv(path)
    columns = [
        "title",
        "abstract",
        "coverDate",
        "contributors",
        "source",
        "doi",
        "pubTypes",
    ]

    scopus_data = scopus_data[columns]
    scopus_data = scopus_data.rename(
        columns={
            "contributors": "authors",
            "coverDate": "year",
            "source": "journal",
            "pubTypes": "publication types",
        }
    )
    scopus_data["year"] = scopus_data["year"].apply(tidy_year)
    return scopus_data


def tidy_year(s):
    if pandas.isna(s) or not isinstance(s, str):
        return s
    year = re.findall(r"(19\d{2}|20\d{2})", s)
    if len(year) == 0:
        return s
    return year[0]


def main():
    data = []
    data.append(read_single(f"database-search-results/CINAHL/cinahl_export.csv"))
    pandas.concat(data).to_csv("outputs/database-search-results/cinahl.csv", encoding='utf-8')


if __name__ == "__main__":
    sys.exit(main())
