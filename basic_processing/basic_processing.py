# Basic filtering to get rid of easy-to-identify irrelevantabstracts.

from functools import partial
import re
import sys

from num2words import num2words
import pandas
from unidecode import unidecode


def excel_sheet_name(s):
    if len(s) > 31:
        return s[:31]
    else:
        return s


def match_phrases(df, colname, phrases, excelwriter=None, sheet_name=None):
    result = None
    for phrase in phrases:
        matches = df[colname].str.lower().str.contains(phrase, regex=True)
        if result is None:
            result = matches
        else:
            result |= matches
    if excelwriter is not None:
        df[result].to_excel(excelwriter, sheet_name=excel_sheet_name(sheet_name))

    return result


def remove_publication_type(df, t, excelwriter=None, result_series=None):
    result = match_phrases(df, "publication types", [t], excelwriter, t)
    print(f"Removed {result.sum()} {t}; was {len(df)} is now {len(df) - result.sum()}")
    if result_series is not None:
        result_series.append(result.sum())
    return df[~result]


def remove_title_phrases(df, title_phrases, name, excelwriter=None, result_series=None):
    result = match_phrases(df, "title", title_phrases, excelwriter, name)
    print(
        f"Removed {result.sum()} {name}; was {len(df)} is now {len(df) - result.sum()}"
    )
    if result_series is not None:
        result_series.append(result.sum())
    return df[~result]


def remove_journal_phrases(df, phrases, name, excelwriter=None, result_series=None):
    result = match_phrases(df, "journal", phrases, excelwriter, name)
    print(
        f"Removed {result.sum()} {name}; was {len(df)} is now {len(df) - result.sum()}"
    )
    if result_series is not None:
        result_series.append(result.sum())
    return df[~result]


def remove_title_publication_type(
    df,
    title_phrases,
    pub_types,
    name,
    journal_phrase=None,
    excelwriter=None,
    result_series=None,
):
    result = match_phrases(
        df,
        "publication types",
        pub_types,
        excelwriter,
        sheet_name=excel_sheet_name(f"{name}-pub-types"),
    )
    print(f"pub_types: {result.sum()}")
    title_result = match_phrases(df, "title", title_phrases)
    if excelwriter is not None:
        df[title_result & ~result].to_excel(
            excelwriter, sheet_name=excel_sheet_name(f"{name}-title")
        )
    result |= title_result
    print(f"title: {title_result.sum()}")
    if journal_phrase is not None:
        journal_result = match_phrases(df, "journal", [journal_phrase])
        if excelwriter is not None:
            df[journal_result & ~result].to_excel(
                excelwriter, sheet_name=excel_sheet_name(f"{name}-journal")
            )
        result |= journal_result
        print(f"journal: {journal_result.sum()}")
    print(
        f"Removed {result.sum()} {name}; was {len(df)} is now {len(df) - result.sum()}"
    )
    if result_series is not None:
        result_series.append(result.sum())
    return df[~result]


def tidy_doi(s):
    if pandas.isna(s):
        return s
    prefixes = [
        "https://dx.doi.org/",
    ]
    for p in prefixes:
        if s.startswith(p):
            return s[len(p) :]
    return s


def tidy_title(s):
    s = re.sub(r"\s+", " ", s)
    s = s.lower().strip()
    s = unidecode(s)
    if s[0] == '"':
        s = s[1:]
    if s[-1] == '"':
        s = s[:-1]
    if s[-1] == ".":
        s = s[:-1]
    return s


def remove_line_breaks(s):
    if pandas.isna(s):
        return s
    return re.sub(r"(?:\r\n|\r|\n)+", ";", s)


def tidy_authors(s, sep=";"):
    if pandas.isna(s):
        return s
    s = remove_line_breaks(s)
    s = unidecode(s)
    authors = s.split(sep)
    result = []
    for name in authors:
        name = name.strip()
        if len(name) == 0:
            continue
        # Add a comma in the last space.
        p = name.rfind(" ")
        if name[p - 1] == ",":
            # Don't insert another comma
            result.append(name)
        else:
            # No comma, insert one.
            result.append(name[:p] + "," + name[p:])
    return ";".join(result)


def first_author_surname(s):
    # Assumes tidy_authors had been run first.
    if pandas.isna(s):
        return s
    author = s.split(";")[0]
    names = author.split(",")
    if len(names) > 0:
        return names[0]
    return author


def make_dedup_index(row):
    s = f'{row["title"].strip()};{row["year"]:.0f};{row["first_author_surname"]}'.lower()
    return re.sub(r"\s+", "", s)


def process(name, path):
    print(f"\n\nProcessing {name}")
    df = pandas.read_csv(path)
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns="Unnamed: 0")
    print(f"Found {len(df)} records")

    pre_2014 = df["year"] < 2014
    df = df[~pre_2014]
    print(f"Removed {pre_2014.sum()} entries published before 2014")

    result_series = [len(df)]

    # Exclude anything without a title, abstract, or journal name
    no_abstract_or_title = (
        df["abstract"].isna()
        | df["title"].isna()
        | df["journal"].isna()
        | df["year"].isna()
    )
    no_abstract_or_title |= df["abstract"].str.contains("No abstract available")
    df = df[~no_abstract_or_title]
    print(
        f"Removed {no_abstract_or_title.sum()} entries with no available title, abstract, journal name, or year."
    )
    result_series.append(no_abstract_or_title.sum())

    abstract_single_sentence = df["abstract"].apply(lambda a: len(a.split(".")) == 2)
    df = df[~abstract_single_sentence].copy()
    print(
        f"Removed {abstract_single_sentence.sum()} entries with a single sentence in the abstract."
    )
    result_series.append(abstract_single_sentence.sum())

    if "language" in df.columns:
        non_eng = ~(
            df["language"].str.contains("eng")
            | df["language"].str.contains("English")
            | df["language"].isna()
        )
        if non_eng.sum() > 0:
            print(df[non_eng]["language"].value_counts())
        df = df[~non_eng]
        print(f"Removed {non_eng.sum()} entries where the language was not English.")
        result_series.append(non_eng.sum())
    else:
        result_series.append(0)

    # Remove surrounding quotes and full stops from titles.
    df["title"] = df["title"].apply(tidy_title)

    # Remove newlines in authors and abstracts
    sep = "," if name == "pubmed" else ";"
    df["authors"] = df["authors"].apply(partial(tidy_authors, sep=sep))
    df["first_author_surname"] = df["authors"].apply(first_author_surname)
    df["abstract"] = df["abstract"].apply(remove_line_breaks).apply(lambda s: unidecode(s))
    df["dedup_index"] = df.apply(make_dedup_index, axis=1)

    with pandas.ExcelWriter(
        f"outputs/basic-processing/basic-exclusions/{name}-exclusions.xlsx"
    ) as excelwriter:
        l = len(df)
        dropped_dupes = df.drop_duplicates(subset=["title", "year", "first_author_surname"])
        # dropped_dupes = df.drop_duplicates(subset=["title", "year"])
        # Save the duplicates
        pandas.concat([df, dropped_dupes]).drop_duplicates(keep=False).to_excel(
            excelwriter, sheet_name="duplicates"
        )
        df = dropped_dupes
        print(f"Dropped {l - len(df)} duplicate records")
        result_series.append(l - len(df))

        df.set_index("dedup_index", inplace=True)
        df = remove_journal_phrases(
            df, ["opinion"], "opinion", excelwriter, result_series
        )
        df = remove_journal_phrases(
            df, ["medical hypotheses"], "hypotheses", excelwriter, result_series
        )
        df = remove_journal_phrases(
            df,
            [
                "veterinary",
                "animals",
                "cattle",
                "equine",
                "wildlife",
                "ruminants",
            ],
            "animal-focused journals",
            excelwriter,
            result_series,
        )
        df = remove_journal_phrases(
            df, ["transplantation"], "transplant", excelwriter, result_series
        )
        df = remove_journal_phrases(
            df, ["tropical"], "tropical medicine", excelwriter, result_series
        )
        df = remove_journal_phrases(
            df,
            ["surgical infection"],
            "surgical infection",
            excelwriter,
            result_series,
        )
        df = remove_journal_phrases(
            df, ["resuscitation"], "resuscitation", excelwriter, result_series
        )
        df = remove_journal_phrases(
            df,
            [
                r"\baids\b",
                r"\bhiv\b",
                "sexually transmitted disease",
            ],
            "HIV-AIDS specific journals",
            excelwriter,
            result_series,
        )
        df = remove_journal_phrases(
            df,
            [
                "engineering",
                "acta mechanica",
                "aerospace",
                "thin-walled structures",
                "technologies",
                "steel construction",
                "revista materia",
                "physics of fluids",
            ],
            "engineering",
            excelwriter,
            result_series,
        )
        df = remove_journal_phrases(
            df,
            [
                "anaesthesia",
                "anesthesia",
                "anaesthesiology",
                "anesthesiology",
                "acta anaesthesiologica",
                "anestezi dergisi",
                "anestesiologica",
                "anesteziologiia",
                "anestezjologia",
            ],
            "anaesthesia specific journals",
            excelwriter,
            result_series,
        )
        df = remove_publication_type(
            df, "book", excelwriter, result_series
        )  # book chapter
        df = remove_publication_type(df, "news", excelwriter, result_series)
        df = remove_publication_type(
            df,
            "guideline",
            excelwriter,
            result_series,
        )  # guideline, practice guideline
        df = remove_publication_type(df, "biography", excelwriter, result_series)
        df = remove_publication_type(
            df, "legal", excelwriter, result_series
        )  # legal case
        df = remove_publication_type(df, "proceedings", excelwriter, result_series)
        df = remove_publication_type(df, "exam questions", excelwriter, result_series)
        df = remove_publication_type(
            df, "teaching material", excelwriter, result_series
        )
        df = remove_publication_type(df, "preprint", excelwriter, result_series)
        df = remove_title_publication_type(
            df,
            title_phrases=[
                "poster presentation",
            ],
            pub_types=[
                "conference",
            ],
            name="conference",
            excelwriter=excelwriter,
            result_series=result_series,
        )
        df = remove_title_publication_type(
            df,
            title_phrases=[
                "statement of retraction",
            ],
            pub_types=[
                "retract",
            ],
            name="retracted",
            excelwriter=excelwriter,
            result_series=result_series,
        )
        df = remove_title_publication_type(
            df,
            title_phrases=[
                ": protocol",
                "study protocol",
                "protocol for a",
            ],
            pub_types=[
                "protocol",  # clinical trial protocol
            ],
            name="protocol",
            excelwriter=excelwriter,
            result_series=result_series,
        )
        df = remove_title_publication_type(
            df,
            title_phrases=[
                "author",  # author's reply, author's response
                "editor",  # editorial, editor's reply, letter to the editor
                "comment",  # comment on, commentary, response to comments
                "letter",  # letter of reply, letter to, response to letter
                "^re:",
                "committee opinion",
            ],
            pub_types=[
                "comment",
                "editorial",
                "erratum",  # <- will find articles with erratum when retrieving full text.
                "letter",
            ],
            name="commentary",
            excelwriter=excelwriter,
            result_series=result_series,
        )
        df = remove_title_phrases(
            df,
            [
                "design of a",  # Study design / methodology
                "methodology",
            ],
            "methodology",
            excelwriter=excelwriter,
            result_series=result_series,
        )
        df = remove_title_publication_type(
            df,
            title_phrases=[
                ": a meta.?analysis",
                "^(?:a )? meta.?analysis",
                "narrative review",
                "systematic review",
                "scoping review",
                "umbrella review",
                "a (?:systematic )?literature review",
                "state.of.the.art review",
                "review of the literature",
                "overview",
            ],
            pub_types=[
                "review",
                "meta.?analysis",
            ],
            name="systematic review",
            journal_phrase="systematic review",
            excelwriter=excelwriter,
            result_series=result_series,
        )
        df = remove_title_phrases(
            df,
            [
                "cohort profile",  # Profile of a cohort
            ],
            "cohort profile",
            excelwriter=excelwriter,
            result_series=result_series,
        )

        df = remove_title_publication_type(
            df,
            title_phrases=[
                r"case.(?:report|description|summary|study|series)",
                "conse[cq]utive.case",
                "conse[cq]utive.patient",
                r"series of (?:\d{1,2} )case",
                r"series of (?:\d{1,2} )patient",
                "review of case",
                r"\b\d{1,2} (?:new )?case",
            ]
            + [f"[^-]{num2words(n)} case" for n in range(1, 21)],
            pub_types=[
                "case",  # case reports
            ],
            name="case report or case series",
            journal_phrase="case",
            excelwriter=excelwriter,
            result_series=result_series,
        )

    print(f"Continuing analysis with {len(df)} remaining records...")
    result_series.append(len(df))

    df = df.rename(columns={"country": "publish_country"})
    df["doi"] = df["doi"].apply(tidy_doi)
    df["source"] = name

    df.to_csv(f"outputs/basic-processing/{name}.csv")

    return result_series


def main():
    result_df = pandas.DataFrame()
    result_df["pubmed"] = process("pubmed", "outputs/database-search-results/pubmed.csv")
    result_df["cinahl"] = process("cinahl", "outputs/database-search-results/cinahl.csv")
    result_df["medline"] = process("medline", "outputs/database-search-results/medline.csv")
    result_df["psycinfo"] = process("psycinfo", "outputs/database-search-results/psycinfo.csv")
    result_df["embase"] = process("embase", "outputs/database-search-results/embase.csv")
    result_df["scopus"] = process("scopus", "outputs/database-search-results/scopus.csv")

    result_df["index"] = [
        "total records",
        "missing abstract, title, or year",
        "single sentence in abstract",
        "not published in English",
        "duplicates",
        "journal name: opinion",
        "journal name: hypotheses",
        "journal name: veterinary",
        "journal name: transplant",
        "journal name: tropical medicine",
        "journal name: surgical infection",
        "journal name: resuscitation",
        "journal name: HIV-AIDs",
        "journal name: engineering",
        "journal name: anaesthesia",
        "publication type: book chapter",
        "publication type: news article",
        "publication type: guideline",
        "publication type: biography",
        "publication type: legal case",
        "publication type: conference proceedings",
        "publication type: exam question",
        "publication type: teaching material",
        "publication type: preprint",
        "publication type: conference or poster presentation",
        "detected article type: retracted",
        "detected article type: protocol",
        "detected article type: commentary",
        "detected article type: methodology",
        "detected article type: systematic review",
        "detected article type: cohort profile",
        "detected article type: case report or case series",
        "remaining articles",
    ]
    result_df.set_index("index", inplace=True)
    result_df.to_csv("outputs/basic-processing/basic-processing-summary.csv")


if __name__ == "__main__":
    sys.exit(main())
