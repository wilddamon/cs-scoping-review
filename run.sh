#!/bin/bash

echo_then_run() {
    echo "\n\n$1"
    $1
}

echo_then_run "python basic-processing/basic_processing.py"
echo_then_run "python basic-processing/merge_datasets.py"

echo_then_run "python gemini/extract_gemini.py"
echo_then_run "python gemini/validate_responses.py"
echo_then_run "python gemini/merge_gemini.py"
echo_then_run "python gemini/add_country_income_status.py"

echo_then_run "python ranking/rank_relevance.py"
echo_then_run "python export_endnote.py"
