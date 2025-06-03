import sys

import pandas


def read_single(path):
    scopus_data = pandas.read_csv(path)
    columns = [
        "Authors",
        "Title",
        "Year",
        "Source title",
        "DOI",
        "Abstract",
        "PubMed ID",
        "Document Type",
    ]

    scopus_data = scopus_data[columns]
    scopus_data = scopus_data.rename(
        columns={
            "Authors": "authors",
            "Title": "title",
            "Year": "year",
            "Source title": "journal",
            "DOI": "doi",
            "Abstract": "abstract",
            "PubMed ID": "pmid",
            "Document Type": "publication types",
        }
    )
    return scopus_data


def main():
    data = []
    for i in range(2):
        data.append(read_single(f"database-search-results/Scopus/scopus({i}).csv"))
    pandas.concat(data).to_csv("outputs/database-search-results/scopus.csv")


if __name__ == "__main__":
    sys.exit(main())
