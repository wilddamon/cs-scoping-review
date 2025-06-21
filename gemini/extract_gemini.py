import os
import pandas
import re
import sys
import time

from google import genai

import api_keys


BASE_QUERY = """
Analyse these academic abstracts and output in JSON format using the following keys:
"abstract ID",
"study type" (the type of study, for example cross-sectional/cohort/retrospective review),
"exposure" (list of exposures measured in the study),
"outcome" (list of outcomes mentioned),
"cohort" (a 1-sentence description of the participants in the study),
"finding" (a 1-sentence summary of the primary finding),
"mod_finding (a 1-sentence summary of any findings related to mode of birth, or N/A if none),
"country" (the country that the study was performed in, or N/A if not specified).
"gest_age"  (the gestational age of the babies included in the study, or N/A if not mentioned),
"followup_time" (the length of time after birth that the last observation was taken, or N/A if not mentioned),
"birth_weight" (the weight of the babies included in the study, or N/A if not mentioned)
"""


def generate_query(abstracts, index):
    query = BASE_QUERY
    a = abstracts.iloc[index]
    a_escaped = re.sub(r"\s+", " ", a["abstract"])
    query += f'"""abstract ID: {a["dedup_index"]}\n"{a_escaped}"""\n\n\n'

    return query


def send_query(client, query, save_path, index):
    for i in range(3):
        print(f"Sending query {index}")
        response = client.models.generate_content(
            model="gemini-1.5-pro", contents=query
        )

        finish_reason = response.candidates[0].finish_reason
        if finish_reason == genai.types.FinishReason.STOP:
            break
        else:
            print(response)
            print("Retrying... in 30 seconds.")
            time.sleep(30)

    if finish_reason != genai.types.FinishReason.STOP:
        print(f"No valid response for {index}, try again later.")
        return

    t = response.text
    # Save the response to a text file.
    with open(save_path, "w") as f:
        n = f.write(t)
        print(f"Wrote to {n} characters to {save_path}")


def send_queries(
    client,
    response_folder,
    wait_seconds=10,
):
    abstracts = pandas.read_csv("outputs/basic-processing/merged-abstracts.csv")
    #abstracts = pandas.read_csv("outputs/basic-processing/test.csv")
    for i in range(len(abstracts)):
        # Exists - skip.
        path = f"{response_folder}/gemini-response-{i}.json"
        if os.path.exists(path) or os.path.exists(f"outputs/gemini-responses/new-responses/gemini-response-{i}.json"):
            continue

        query = generate_query(abstracts, i)
        send_query(client, query, path, i)

        print(f"Waiting for {wait_seconds} seconds.")
        time.sleep(wait_seconds)


def main():
    client = genai.Client(api_key=api_keys.GEMINI_KEY)

    send_queries(
        client, "outputs/gemini-responses", wait_seconds=0
    )


if __name__ == "__main__":
    sys.exit(main())
