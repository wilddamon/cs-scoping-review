import collections
import re
import pandas


WHOS = [
    "offspring",
    "women",
    "dyad",
    "society",
]


# Maps of concept to terms.
outcome_mapping = {
    "offspring": {
        "obesity/overweight/bmi": [
            "obes",
            "weight status",
            "rate of weight change",
            "weight trajectory",
            "excess weight",
            "overweight",
            "body mass index",
            "bmi",
            "^(?!.*fetal)(?!.*foetal).*growth(?! restriction)",
            "adipos",
            "waist",
            "fat",
            "circumference",
        ],
        "asthma": ["asthma", "airflow"],
        "wheeze": ["wheez"],
        "microbiome/mycobiome/virome": [
            "microbiome",
            "microbiota",
            "mycobiome",
            "virome",
            "bifidobacterium",
            "lactobacillus",
            "gut bacteria",
            "gut microb",
            "infant gut",
            "gastrointestinal tract colonization",
            "microbial community richness",
            "gut colon",
            "yeast",
            "oral bacterial colonization",
            "urobiome",
            "abundance",
        ],
        "allergy/atopy": [
            "allerg",
            "atop",
            "sensitization",
            "sensitisation",
            "multiple chemical sensitivity",
            "cmpa",
            "otolaryngology",
            "eczema",
            "rhinitis",
        ],
        "autism": ["autis", "asperger"],
        "cognitive development": [
            "developmental delay",
            "cognitive",
            "school performance",
            "school achievement",
            "reading proficiency",
            "intelligence",
            "intellect",
            "learning disabilit",
            r"\biq\b",
            "education",
            "neurodevelopment",
            "neuropsychological",
            "motor skill",
            "motor development",
            "academic",
            "mental development",
            "infant motor scale",
            "developmental",
            "slow learning",
        ],
        "cancer": [
            "cancer",
            "neoplas",
            "leuka?emia",
            "tumour",
            "tumor",
            "lymphoma",
            "blastoma",
            "malignanc",
            "meningioma",
            "b-cell precursor all",
            "early.onset all",
            "rhabdomyosarcoma",
        ],
        "behaviour problems": ["behaviour", "behavior"],
        "precocious puberty": [
            "pubert",
            "menarcheal age",
        ],
        "diabetes": [
            "(?<!gestational )(?<!non)(?<!non-)diabet",
            "^(?!.*neonat).*glucose",
            "islet autoimmun",
        ],
        "vaccine efficacy": ["vaccin"],
        "cardiovascular risk": [
            "^(?!.*pregnancy).*hypertension(?! in pregnancy)",
            "blood pressure",
            "vascular risk",
            "cardiorespiratory",
            "premature atherosclerosis",
        ],
        "metabolic dysregulation": [
            "metabolic syndrome",
            "lipid metabolism",
            "adiponectin",
            "insulin resistance",
        ],
        "bowel disease": [
            "crohn",
            "irritable bowel",
            "ibd",
            "ulcerative colitis",
            "irritable bowel",
            "inflammatory bowel",
            "gastrointestinal",
            "faecal calprotectin",
        ],
        "coeliac disease": ["celiac", "coeliac"],
        "ADHD": ["adhd", "attention", "impulsiv"],
        "multiple sclerosis": [r"multiples? sclerosis", "ms susceptibility"],
        "constipation": ["constipation"],
        "hip dysplasia": ["hip dysplasia", "dysplasia of the hip", "ddh"],
        "refractive disorder": [
            "myopia",
            "hypermetropia",
            "astigmatism",
            "refractive disorder",
            "visual impairment without correction",
        ],
        "ear infection": [
            "otitis media",
        ],
        "gastrointestinal infection": ["gastroenteritis", "gastrointestinal infect"],
        "respiratory infection": [
            "respiratory (?:tract )?infect",
            "bronchiolitis",
            "pneumonia",
            "otolaryngology",
            "rsv",
        ],
        "psychosis": ["psychosis", "psychotic"],
        "esophagitis": ["esophag"],
        "wheezing": ["wheezing"],
        "dental issues": [
            "caries",
            "dental",
            "mutans",
            "cariogenic",
            "hypominerali[sz]ation",
            "teeth",
            "malocclusion",
        ],
        "hospitalisation": [
            r"^(?!.*neonatal)(?!.*duration)(?!.*length).*admission(?: \S+ ){0,5}hospital",
            r"^(?!.*duration)(?!.*length)(?: \S+ ){0,5}hospital.*admission",
            "hospitali[sz]ation",
            "(?>!neonatal )in.?patient",
            "hospital stays",
        ],
        "epigenetics": ["epigenetic", "methylation"],
        "iron status": [
            "iron",
            "ferritin",
            "ana?emia",
        ],
        "tic disorders": [r"\btic disorder"],
        "eating disorders": ["anorexia", "disordered eating"],
        "mood disorders": [
            "mood disorder",
            "depression",
            "bipolar",
            "affective disorder",
        ],
        "kawasaki disease": ["kawasaki"],
        "anxiety disorders": [
            "anxiety",
            "stress-related disorder",
            "obsessive-compulsive disorder",
        ],
        "self-harm": ["self.harm"],
        "thyroid hormone levels": [
            r"^(?!.*cord blood).*\btsh\b",
            "t^(?!.*cord blood).*hyroid.stimulating hormone",
            "hypothyroidism",
        ],
        "ankylosing spondylitis": ["ankylosing spondylitis"],
        "brain parameters": ["gray matter volume", "brain development"],
        "intestinal permeability": ["zonulin"],
        "sensory issues": [
            "sensory",
            "tactile sensitivity",
        ],
        "semen quality": ["semen"],
        "abdominal pain": ["abdominal pain"],
        "red blood cell parameters": [
            "red blood cell parameters",
            "erythrocyte",
        ],
        "septal deviation": ["septal deviation"],
        "nocturnal enuresis": ["nocturnal enuresis"],
        "cost of illness": ["cost of illness"],
        "headache": ["migraine"],
        "analysis of feces": [
            "short-chain fatty acid",
            "scfa",
            "calprotectin",
        ],
        "endometriosis": [
            "endometriosis",
            "endometrial",
        ],
        "lung function": [
            "lung function",
        ],
    },
    "women": {
        "urinary incontinence": [
            "urinary incontinence",
            "bladder(?! injur)(?! lesion)",
            "double incontinence",
            "postpartum incontinence",
            r"\bluts\b",
            "stress incontinence",
            "urine incontinence",
            "persistent ui",
        ],
        "faecal incontinence": [
            "faecal incontinence",
            "fecal incontinence",
            "double incontinence",
            "anal incont",
        ],
        "constipation": ["constipation"],
        "nocturia": ["nocturia"],
        "pelvic floor": [
            "pelvic floor",
            "manometry",
            "sphincter pressure",
            "vaginal support",
            "vaginal angle",
        ],
        "prolapse": [
            "prolapse",
            "recto.?vaginal septum",
            "rectocele",
            "anal canal anatomy",
            r"\bpop\b",
            "cystocele",
        ],
        "sexual dysfunction": [
            "sexual",
            "sex life",
            "dyspareunia",
        ],
        "quality of life/self-rated health": [
            "quality of life",
            "qol",
            "self-rated health",
        ],
        "birth experience": [
            "birth experience",
            "birth satisfaction",
        ],
        "chronic pain": [
            "pain(?! relief)",
            "dyspareunia",
            "dysmenorrhea",
            "period pain",
        ],
        "fistula": ["fistula"],
        "endometriosis": [
            "endometriosis",
            "endometrial",
        ],
        "mood disorders": [
            "ppd",
            "depression",
            "mood disorder",
            "psychotropic medication",
            "anti.?depressant",
            "coping strateg",
        ],
        "anxiety disorders": ["anxiety"],
        "PTSD": [
            "ptsd",
            "post.?traumatic stress",
        ],
        "menstrual issues": ["menstrua", "dysmenorrhea", "period pain"],
        "cancer": ["cancer", "cervical dysplasia", "neoplas"],
        "new opioid use": ["opioid use"],
        "diastasis recti": ["diastasis recti"],
        "adhesions": ["adhesion"],
        "small bowel obstruction": ["small bowel obstruction"],
        "sacroiliac joint issue": ["sacroiliac joint"],
        "hypertension": [
            "^(?!.*pregnancy).*hypertension(?! in pregnancy)",
        ],
        "BMI/postpartum weight loss": [
            "body mass index",
            "bmi",
            "weight retention",
        ],
        "coronary heart disease": ["coronary heart disease"],
        "adenomyosis": ["adenomyosis"],
        "metabolic syndrome": ["metabolic syndrome"],
        "vaginal laxity": ["vaginal laxity"],
        "parenting stress": ["parenting stress"],
        "postural control": ["postural control"],
        "abnormal uterine bleeding": [
            "abnormal uterine bleeding",
            "dysfunctional uterine bleeding",
        ],
        "hospitalisation": [
            r"^(?!.*neonatal)(?!.*duration)(?!.*length).*admission(?: \S+ ){0,5}hospital",
            r"^(?!.*duration)(?!.*length)(?: \S+ ){0,5}hospital.*admission",
            "hospitali[sz]ation",
            "(?>!neonatal )in.?patient",
        ],
        "uterine microbiome": ["uterus microbial flora"],
        "vaginal microbiome": ["vaginal microbi"],
        "uterine scarring": ["uterine niche", "uterine wall", "smooth muscle volume"],
        "degenerative spondylolisthesis": ["spondylolisthesis"],
        "spinal issues": ["lumbar disc", "lumbar spin"],
        "abdominal muscle differences": [
            "abdominal muscle thickness",
            "inter-rectus distance",
        ],
        "adenomyosis": ["adenomyosis", "TGF-β1 expression in endometri"],
        "venous thromboembolism": ["venous thromboembolism"],
        "pulmonary embolism": ["pulmonary embolism"],
        "suicide": ["suicide"],
        "cervical elasticity": ["elastography"],
        "hernia": ["hernia"],
        "interstitial cystitis": ["interstitial cystitis"],
        "all-cause mortality": ["all-cause mortality"],
        "abdominal appearance": ["monal height"],
        "genetic expression": ["antimicrobial peptide"],
        "autoimmune disorders": [
            "autoimmune",
            "systemic sclerosis",
            "biliary cholangitis",
        ],
    },
    "dyad": {
        "breastfeeding": [
            "breast.?feeding(?! initiation)",
            "breast.?fed",
            "bottle.?feeding",
            "formula",
            "presepsin",
        ],
        "bonding": ["bond"],
    },
    "society": {
        "antimicrobial resistance": ["resistome", "antibiotic resistance", r"\besbl\b"],
    },
}
outcome_mapping["offspring"]["asthma-wheeze"] = (
    outcome_mapping["offspring"]["asthma"] + outcome_mapping["offspring"]["wheeze"]
)
outcome_mapping["offspring"]["autoimmune disorders"] = [
    "autoimmune",
    "immune-mediated inflammatory",
]
outcome_mapping["offspring"]["psychiatric disorders"] = ["psychiatric"] + (
    outcome_mapping["offspring"]["mood disorders"]
    + outcome_mapping["offspring"]["eating disorders"]
    + outcome_mapping["offspring"]["anxiety disorders"]
    + outcome_mapping["offspring"]["self-harm"]
    + outcome_mapping["offspring"]["ADHD"]
)
outcome_mapping["offspring"]["infection"] = [
    "infection burden",
    "infection-related hospitalisation",
    "hospitalization with infection",
    "hospitalised infection",
    "risk of infection",
    "parechovirus",
] + (
    outcome_mapping["offspring"]["ear infection"]
    + outcome_mapping["offspring"]["gastrointestinal infection"]
    + outcome_mapping["offspring"]["respiratory infection"]
)
outcome_mapping["women"]["psychiatric disorders"] = ["psychiatric"] + (
    outcome_mapping["women"]["mood disorders"]
    + outcome_mapping["women"]["anxiety disorders"]
    + outcome_mapping["women"]["PTSD"]
)
outcome_mapping["women"]["pelvic floor related issues"] = (
    outcome_mapping["women"]["urinary incontinence"]
    + outcome_mapping["women"]["faecal incontinence"]
    + outcome_mapping["women"]["prolapse"]
    + outcome_mapping["women"]["pelvic floor"]
)


def outcome_of_kind(term_map, s):
    result = []
    if pandas.isna(s):
        return result
    for o in term_map:
        for t in term_map[o]:
            if re.search(t, s, flags=re.I) is not None:
                result.append(o)
                break
    return result


def infer_who_by_row(row):
    outcome = row["outcome"]
    result = []
    for who in WHOS:
        classification = outcome_of_kind(outcome_mapping[who], outcome)
        if len(classification) > 0:
            result.append((who, classification))
    return result


def save_outcomes(data, who, outcomes_counter):
    # remove blanks and irrelevant
    f = data[who].notna()
    data = data[f]

    save_columns = [
        "title",
        "abstract",
        "year",
        "authors",
        "journal",
        "doi",
        "country",
        "who",
        who,
    ]
    with pandas.ExcelWriter(f"outputs/outcome_maps/{who}.xlsx") as excelwriter:
        # The keys of the outcomes_counter are the things we care about.
        # Sort them by most common
        outcomes = [o[0] for o in outcomes_counter.most_common()]
        for outcome in outcomes:
            group = data[data[who].apply(lambda x: outcome in x)]
            group[save_columns].to_excel(
                excelwriter,
                sheet_name=outcome.replace("/", "-")[: min(len(outcome), 31)],
            )


def insert_fulltext_result(row):
    if row["manual_assessment"] == "YES" and not row["only_hic"]:
        return "NO"
    if row["updated_manual_assessment"] == "NO":
        return "NO"
    return None

data = pandas.read_csv("outputs/ranked-abstracts-with-manual-assessments-2026-3-18.csv", low_memory=False)

data["outcome"] = data["outcome"].str.lower()
yes_data = data[
    (data["relevance"] > 0)
    & (data["manual_assessment"] != "NO")
    & (data["fulltext_assessment"] != "NO")
].copy()

yes_data["who"] = None
yes_data["offspring"] = None
yes_data["women"] = None
yes_data["dyad"] = None
yes_data["society"] = None

who_counters = {who: collections.Counter() for who in WHOS}

for idx in yes_data.index:
    who_results = infer_who_by_row(yes_data.loc[idx])
    yes_data.at[idx, "who"] = [r[0] for r in who_results]
    for who_result in who_results:
        who = who_result[0]
        outcomes = who_result[1]
        yes_data.at[idx, who] = outcomes
        for outcome in outcomes:
            who_counters[who][outcome] += 1
    if len(who_results) == 0:
        print(yes_data.loc[idx])


for who in WHOS:
    save_outcomes(yes_data, who, who_counters[who])


