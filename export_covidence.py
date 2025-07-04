import pandas


data = pandas.read_csv("outputs/ranked_abstracts-2025_02_25T04_30.csv")


def export_endnote_txt(group, name):
    g = group[["title", "abstract", "year", "authors", "journal", "doi"]].copy()
    g["title"] = g["title"].apply(lambda t: t.title())
    g["abstract"] = g["abstract"].apply(lambda a: a.replace('"', ""))
    g["year"] = g["year"].apply(lambda y: f"{y:.0f}")
    g = g.rename(
        columns={
            "title": "Title",
            "abstract": "Abstract",
            "year": "Year",
            "authors": "Author",
            "journal": "Journal",
            "doi": "DOI",
        }
    )
    # path_or_buf=None -> return as string.
    # sep="\t" -> tab delimit
    # index=False -> do not output the index column
    s = g.to_csv(path_or_buf=None, sep="\t", index=False)
    s = "*Journal Article\n" + s
    with open(f"outputs/for_endnote_import/{name}.txt", "w") as f:
        f.write(s)


export_endnote_txt(data[data["relevance"] > 0], f"included")
#export_endnote_txt(data[data["relevance"] <= 0], f"exclded")


data[(data["manual_assessment"] == "YES") & (data["relevance"] <= 0)].to_csv(
    "outputs/false_negative.csv"
)
