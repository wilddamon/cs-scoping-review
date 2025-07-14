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
