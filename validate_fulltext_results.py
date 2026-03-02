import collections
from functools import partial
import pandas
from pprint import pprint

t = "2026-2-27T16-5"
whos = ("offspring", "women", "society", "dyad")

all_reviewed_ids = set()
not_relevant_after_review_ids = set()

num_reviewed_by_who = {}
num_to_review_by_who = {}
lines_reviewed = {}

outcomes_by_id = collections.defaultdict(list)
notes_by_id = collections.defaultdict(list)


def save_outcome_row(outcome, row):
    colnames = (
        "difference observed past 12 months",
        "CS good/bad/neutral",
    )
    for colname in colnames:
        if colname in row and pandas.notna(row[colname]) and row[colname] != "-":
            outcomes_by_id[row["dedup_index"]].append(outcome)


def save_note_by_row(row):
    if pandas.notna(row["Note"]):
        notes_by_id[row["dedup_index"]].append(row["Note"])


for who in whos:
    num_reviewed_by_who[who] = set()
    num_to_review_by_who[who] = set()
    lines_reviewed[who] = [0, 0]
    xls = pandas.read_excel(f"outputs/outcome_maps/{who}-{t}.xlsx", sheet_name=None)
    for sheet_name in xls:
        d = xls[sheet_name]
        num_to_review_by_who[who] |= set(d["dedup_index"])
        lines_reviewed[who][1] += len(d)

        if sheet_name == "microbiome-mycobiome-virome":
            lines_reviewed[who][0] += pandas.notna(
                d["difference observed past 12 months"]
            ).sum()
            reviewed = d[
                pandas.notna(d["difference observed past 12 months"])
                & (d["difference observed past 12 months"] != "-")
            ]
        else:
            lines_reviewed[who][0] += pandas.notna(d["CS good/bad/neutral"]).sum()
            reviewed = d[
                pandas.notna(d["CS good/bad/neutral"])
                & (d["CS good/bad/neutral"] != "-")
            ]
        all_reviewed_ids |= set(reviewed["dedup_index"])
        num_reviewed_by_who[who] |= set(reviewed["dedup_index"])

        if "Updated relevance" in d.columns:
            marked_irrelevant = d[d["Updated relevance"] == "NO"]
            all_reviewed_ids |= set(marked_irrelevant["dedup_index"])
            not_relevant_after_review_ids |= set(marked_irrelevant["dedup_index"])

        d.apply(partial(save_outcome_row, sheet_name), axis=1)
        d.apply(save_note_by_row, axis=1)


yes_data = pandas.read_csv("outputs/yes.csv")
all_ids = set(yes_data["dedup_index"])
print(f"Total number of items for fulltext review: {len(all_ids)}")

print(f"Total number of items reviewed: {len(all_reviewed_ids)}")
print(
    f"Number found to be irrelevant after review: {len(not_relevant_after_review_ids)}"
)

unreviewed = all_ids - all_reviewed_ids
print(f"IDs in yes.csv that aren't reviewed yet: {len(unreviewed)}")

for who in whos:
    pc = len(num_reviewed_by_who[who]) / len(num_to_review_by_who[who]) * 100
    print(
        f"Num papers reviewed in {who}: {len(num_reviewed_by_who[who])}/{len(num_to_review_by_who[who])} ({pc:.2f}%)"
    )
    print(
        f"Num lines reviewed in {who}: {lines_reviewed[who][0]}/{lines_reviewed[who][1]} ({lines_reviewed[who][0]/lines_reviewed[who][1]*100:.2f}%)"
    )
    print()

pprint(unreviewed)


# Output a sheet that shows all the outcomes listed per paper, and also excluded papers
all_data = pandas.read_csv("outputs/validation_results/all_data.csv", low_memory=False)
all_data.set_index("dedup_index", inplace=True)
for dedup_index in all_data.index:
    all_data.at[dedup_index, "reviewed_outcomes"] = ";".join(
        outcomes_by_id[dedup_index]
    )
    all_data.at[dedup_index, "fulltext_review_notes"] = ";".join(
        notes_by_id[dedup_index]
    )
    if dedup_index in not_relevant_after_review_ids:
        all_data.at[dedup_index, "updated_manual_assessment"] = "NO"


d = "2026-02-12"
prev_all_data = pandas.read_csv(f"outputs/validation_results/all_results-{d}.csv")
prev_all_data.set_index("dedup_index", inplace=True)
all_data["exclusion reason"] = None
all_data.update(prev_all_data, overwrite=False)

all_data.to_csv("outputs/validation_results/all_results.csv")
all_data[
    (all_data["relevance"] > 0)
    & (all_data["manual_assessment"] != "NO")
    & (all_data["updated_manual_assessment"] != "NO")
    & (all_data["fulltext_screening"] != "NO")
].to_csv("outputs/validation_results/yes_results.csv")
