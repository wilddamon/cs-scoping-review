# cs-scoping-review
Code for running a Large Language Model-assisted scoping review on the topic of "Long-term health outcomes following caesarean section".

## Workflow:
1. Results were downloaded from broad searches in PubMed, Medline, Cinahl, psycINFO, Embase, and Scopus.
2. These results were converted from the database download format into csv.
3. Some basic processing was applied to the data from each database to remove records with the following:
   - Missing title or abstract
   - Abstracts containing only a single sentence
   - The language was not English
   - Duplicate records
   - Journals that only publish irrelevant types of articles:
     - Medical hypotheses
     - Opinions
     - Veterinary/animal studies only
     - Transplants
     - Tropical medicine
     - Surgical Infection
     - Resuscitation
     - HIV/AIDs
     - Mechanical engineering
     - Anaesthesia
   - Systematic reviews
   - Retracted articles
   - Study protocols
   - Editorials and other commentary
   - Methodology
   - Reprints
   - Case studies and case series
   - Cohort profiles
4. The results were then merged into one large csv file, and duplicates removed.
5. Data was extracted from each abstract using Gemini 1.5-pro:
   - Lists of exposures and outcomes
   - One-sentence descriptions of the study cohort, the study findings, and the study finding related to method of birth (if applicable)
   - The country the study was performed in, if specified
   - The study type, follow-up length (if applicable), timing of cross-section (if applicable), the gestational age at birth (if applicable), the birth weight of the babies (if applicable)
7. Validataion was applied to the extracted data to make sure nothing was missing.
8. The data were merged into the large csv file.
9. The country income level (published by the World Bank) was applied to the extracted countries.
10. The data were used to exclude studies that were irrelevant to the review.
11. The results for studies identified as potentially relevant were exported into a format that can be imported into EndNote.

# Usage
To run the whole process on my existing data, run:
```
    sh run.sh
```
* To start from scratch, replace the contents of folders inside the `database-search-results` folder, and clear all files out of the `outputs` directory (but maintain the directory structure); then run run.sh. You may also need to adjust the `parse_X_set.py` scripts accordingly.
* To fetch everything again from Gemini, clear the files out of `outputs/gemini-queries` and `outputs/gemini-responses` directories; then run run.sh.

# Explanation of individual scripts
```
   database-search-results/parse_*.py
```
Takes the contents of the subdirectories of `database-search-results/`, and parses them to be in a consistent format.
Outputs the following files:
* `outputs/database-search-results/cinahl.csv`
* `outputs/database-search-results/embase.csv`
* `outputs/database-search-results/medline.csv`
* `outputs/database-search-results/psycinfo.csv`
* `outputs/database-search-results/pubmed.csv`
* `outputs/database-search-results/scopus.csv`

```
   basic_processing/basic_processing.py
```
Does pre-processing on each individual set returned from the `parse_*_set.py` scripts. Outputs anything excluded at this stage into the `outputs/basic-processing/basic-exclusions/` directory.

```
   basic_processing/merge_datasets.py
```
Merges the pre-processed files into a single file (`outputs/basic-processing/merged-abstracts.csv`), removing duplicates.

```
    gemini/extract_gemini.py
```
Inspects the contents of `merged-abstracts.csv`, and sends a query to Google Gemini if one hasn't been sent yet.
* When a query is sent, a file is created in `outputs/gemini-queries/`. If that file exists, the script will not send another one. Deleting the file will cause the script to re-send the query next time it is run.
* The json response from Gemini is placed into `outputs/gemini-responses`.

```
    gemini/validate_responses.py
```
Inspects the contents of `merged-abstracts.csv` and the directory `outputs/gemini-responses/` and ensures that a response has been obtained for every line, and that the JSON is valid and contains all the right fields.
* A common failure mode is that Gemini "helpfully" "fixes" the abstract ID. This is detected by the script for manual fixing.

```
    gemini/merge_gemini.py
```
Reads all the json files in `outputs/gemini-responses` and merges the data with `merged-abstracts.csv`. The result is output into a file named `merged-abstracts-gemini-appended.csv`.

```
    gemini/add_country_income_status.py
```
Normalises and adds the country income status (from the file `data/world_bank_country_groups.xlsx`). Outputs a file named `merged-abstracts-gemini-appended-country-appended.csv`.

```
    ranking/rank_relevance.py
```
Applies filters based on the study's inclusion and exclusion criteria. The most important part for the study!
* Outputs a file `outputs/ranked-abstracts.csv`.
* A manually edited version of this is checked in `outputs/ranked-abstracts-with-manual-assessments-2026-3-18.csv`. In this file I have entered the manual title/abstract reviews from Covidence.

```
    export_endnote.py
```
Outputs the articles included by programmatic filtering into a file format that can be imported into EndNote (`outputs/for_endnote_import/included.txt`). Once imported, you can export as an XML file to be imported into Covidence, use the full text finder etc.


```
    map_outcomes.py
```
Takes the file with the manually assessments (`outputs/ranked-abstracts-with-manual-assessments-2026-3-18.csv`), and maps it to various known outcomes.
* Outputs four files corresponding to different affected groups.
    * Offspring (`outputs/outcome_maps/offspring.xlsx`)
    * Women (`outputs/outcome_maps/women.xlsx`)
    * Dyad (things affecting both offspring and women) `outputs/outcome_maps/dyad.xlsx`)
    * Society (`outputs/outcome_maps/society.xlsx`)

I have manually edited these files with my full text reviews and data extractions (suffix `-2026-3-18T15-50`), which are checked in for inspection.

```
    validate_fulltext_results.py
```
Checks that everything in the outcome map files (currently points to my manual versions (suffix `-2026-3-18T15-50`) has been reviewed, and that there are no articles left that do not have data extracted.
