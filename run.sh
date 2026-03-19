#!/bin/bash

set -x -e

if [ ! -d outputs/database-search-results ]; then
    mkdir outputs/database-search-results
    mkdir outputs/basic-processing
    mkdir outputs/basic-processing/basic-exclusions
    mkdir outputs/basic-processing/dupes-removed
    mkdir outputs/for_covidence_import
    mkdir outputs/for_endnote_import
fi

python database-search-results/parse_cinahl_set.py
python database-search-results/parse_ovid_sets.py
python database-search-results/parse_pubmed_set.py
python database-search-results/parse_scopus_set.py

python basic_processing/basic_processing.py
python basic_processing/merge_datasets.py

python gemini/extract_gemini.py
python gemini/validate_responses.py
python gemini/merge_gemini.py
python gemini/add_country_income_status.py

python ranking/rank_relevance.py
python export_endnote.py
