import re
import sys

import pandas


def drop_non_na_duplicates(data, column):
    dupe = data[column].duplicated() & data[column].notna()
    return data[~dupe], data[dupe]


def merge_set(name, path, combined_data):
    before_length = len(combined_data)

    print(f"\nMerging {name}")
    data = pandas.read_csv(path)
    data["source"] = name

    original_data_length = len(data)
    print(f"Found {len(data)} records.")

    result = pandas.concat([combined_data, data], ignore_index=True)
    original_combined_length = len(result)
    print(f"Combined length: {original_combined_length}")

    result, pmid_dupes = drop_non_na_duplicates(result, "pmid")
    result, doi_dupes = drop_non_na_duplicates(result, "doi")
    print(
        f"Removed {original_combined_length - len(result)} duplicated pmids or dois, now {len(result)}"
    )

    l = len(result)
    result, dedup_dupes = drop_non_na_duplicates(result, "dedup_index")
    print(
        f"Removed {l - len(result)} duplicate title/year/first author combos, now {len(result)}"
    )

    l = len(result)
    result, ab_dupes = drop_non_na_duplicates(result, "normalised_abstract")
    print(f"Removed {l - len(result)} identical abstracts, now {len(result)}")

    with pandas.ExcelWriter(f"outputs/basic-processing/dupes-removed/{name}.xlsx") as writer:
        pmid_dupes.to_excel(writer, sheet_name="pmid")
        doi_dupes.to_excel(writer, sheet_name="doi")
        dedup_dupes.to_excel(writer, sheet_name="dedup")
        ab_dupes.to_excel(writer, sheet_name="abstract")

    print(f"Removed {original_combined_length - len(result)} records")
    print(f"Added {len(result) - before_length} records")
    print(f"Total records: {len(result)}")
    return result


def main():
    pubmed_data = pandas.read_csv("outputs/basic-processing/pubmed.csv")
    pubmed_data["source"] = "pubmed"
    print(f"Pubmed: {len(pubmed_data)}")

    combined_data = merge_set(
        "cinahl",
        "outputs/basic-processing/cinahl.csv",
        pubmed_data,
    )
    combined_data = merge_set(
        "medline",
        "outputs/basic-processing/medline.csv",
        combined_data,
    )
    combined_data = merge_set(
        "psycinfo",
        "outputs/basic-processing/psycinfo.csv",
        combined_data,
    )
    combined_data = merge_set(
        "embase",
        "outputs/basic-processing/embase.csv",
        combined_data,
    )
    combined_data = merge_set(
        "scopus",
        "outputs/basic-processing/scopus.csv",
        combined_data,
    )

    combined_data.set_index("dedup_index", inplace=True)

    title_vc = combined_data["title"].value_counts()
    print(title_vc[title_vc > 1])

    combined_data[["title", "authors", "year", "abstract", "journal", "pmid", "doi", "publication types", "language", "source"]].to_csv(
        "outputs/basic-processing/merged-abstracts.csv"
    )
    print(
        f"Found {len(combined_data) - len(pubmed_data)} additional records from non-PubMed sources"
    )


if __name__ == "__main__":
    sys.exit(main())
