import re

from num2words import num2words
import pandas

import classify_women_children_society

# caesarean or c-section or -el/em/e/cs
CS_REGEX = r"(?<!vaginal birth after )(?:ca?esar[ei]an|c.?section)"
CS_TYPE_REGEX = f"(?:type of {CS_REGEX}|{CS_REGEX} type)"
MODE_REGEX = "(?:mode|method|type|route)"
BIRTH_REGEX = "(?:birth|deliver(?:y|ies)|child.?birth)"
OPERATIVE_REGEX = f"(?:assisted|operative|vacuum|forceps)(?: vaginal)? {BIRTH_REGEX}"

BIRTH_TYPE_REGEXES = [
    CS_REGEX,
    OPERATIVE_REGEX,
    f"{MODE_REGEX} of {BIRTH_REGEX}",
    f"{BIRTH_REGEX} {MODE_REGEX}",
    f"vaginal? {BIRTH_REGEX}",
    f"{BIRTH_REGEX} history",
    "obstetric history",
    "obstetric factors",
    "reproductive history",
    "factors related to the first birth",
    "perinatal risk factors",
    "perinatal variables",
    "prior abdomino-pelvic surgery",
    "childbirth history",
    "type of parturition",
]

_13_PLUS_MONTHS_REGEX = r"(?:1[2-9]|[2-9][0-9])(?:\.\d+)? months?"
THIRTEEN_TO_THIRTYSIX = (
    "(?:"
    + "|".join([f"{num2words(n)}" for n in range(13, 36 + 1)]).replace("-", ".")
    + ")"
)

OTHER_PROCEDURES = [
    # To differentiate from caesarean/intrapartum/obstetric hysterectomy
    "benign hysterectom",
    "hysterectom(?:y|ies) for (?:a )benign",  # reasons/disease
    "laparoscopically (?:assisted )hysterectom",
    "vaginal hysterectom",
    r"\bvhs?\b",  # Vaginal hysterectomy
    "abdominal hysterectom",
    "laparoscopic hysterectom",
    r"\btlhs?\b",  # Total laparoscopic hysterectomy
    # To differentiate from myomectomy during CS
    "robotic myomectom",
    "laparoscopic myomectom",
    "abdominal myomectom",
    # "dacryoscintigraphy",
    "breast reconstruction",
    "endometrial ablation",
    # IUD insertion
    "iuc?d?s? (?:placement|insertion)",  # iud, ius, iucd, iucds
    "received iuc?d?s?",
    "colonoscopy",
    # Termination of pregnancy
    "terminations? of pregnancy",
    r"\btop\b",
    "medical abortion",
]

SHORT_TERM_REGEXES = [
    "peri.?operative",
    "intra.?partum",
    "minute",
    "hour",
    r"(?:\W\d{1,2}|^\d{1,2}|[12]\d\d|3[0-5]\d|36[0-4])(?:\.\d+)? day",  # < 365 days
    r"(?<!years and)(?<!year and)(?:\s\d|^\d|1[0-1])(?:\.\d+)? month(?!s of \d{4})",
    r"(?:\W\d|^\d|[0-4]\d|5[01])(?:\.\d+)? week",  # < 52 weeks
    f"until {BIRTH_REGEX}",
    f"before {BIRTH_REGEX}",
    "postpartum period",
    "during pregnancy",
    "duration of pregnancy",
    "through pregnancy",
    "throughout pregnancy",
    "during hospital stay",
    "until (?:the )?(?:onset of )labour",
    "pre.?operative period",
    "until(?: hospital)? discharge",
    r"^postpartum$",
    "hospital stay",
    "hospitalization after delivery ",
    "immediately after birth",
    "after postpartum perineum consultation",
    "six.?week",
    "^post.?delivery",
    "in.?hospital",
    "duration of cesarean section",
]

FIVE_TO_NINETEEN_YEARS = (
    "(?:"
    + "|".join([f"{num2words(n)}" for n in range(5, 19 + 1)]).replace("-", ".")
    + ") years"
)
VERY_LONG_TERM_REGEXES = [
    FIVE_TO_NINETEEN_YEARS,
    "twenty.*years",
    "thirty.*years",
    "forty.*years",
    r"[5-9] years",
    r"\d{2} years",
    "five years",
    "ten years",
    "twenty years",
    "decade",
    "mid.?life",
    "later.in.life",
    r"to age \d{2}",
    r"to age [5-9]",
]

LONG_TERM_REGEXES = [
    "year",
    _13_PLUS_MONTHS_REGEX,
    f"{THIRTEEN_TO_THIRTYSIX} months",
    r" to age \d+",
]


COHORT_EXCLUSION_TERMS = [
    "twin",
    "breech",
    "spina bifida",
    "fistula",
    # r"gestation(?:al)?( ages?)? (?:of |between |<.?)?(?:[12]\d|3[0-6])",
    "scar defect",
    "niche",
    "congenital diaphragmatic hernia",
    # "neonates",
    "scar pregnanc",
    r"\bcsps?\b",  # caesarean scar preganc(y|ies)
    "delivering women",
    # "pregnant women",
    "trisomy",
    r"\bmares",
    "mice",
    "sterilization",
    "ha?emophilia",
    f"at time of {CS_REGEX}",
    r"pregnancies in \d+ women with a diagnosis of",
    r"women (?:\w+ )*received a (?:\w+ )*transplant",
    r"\bhiv\b",
    "congenital ntd",
    "neural tube defect",
    "women with crohn",
    "women with chd",  # congenital heart disease
    r"infants aged (?:\d+ ?- ?)?(?:[0-9]|1[0-1]) months",
    r"\bhcv\b",
    "perinatal stroke",
    "brachial plexus",
    "women with ibd",
    "cns inflammatory demyelinating attacks",
    f"(?:undergoing|admitted for) (?:primary )?(?:first )?(?:repeat )?(?:planned )?(?:elective )?(?:or )?(?:emergency )?{CS_REGEX}",
    f"(?:undergoing|admitted for)(?: a)? (?:primary )?(?:first )?(?:repeat )?(?:emergency )?(?:or )?(?:planned )?(?:elective )?{CS_REGEX}",
    f"who underwent (?:a )?(?:primary )?(?:first )?(?:repeat )?(?:planned )?(?:elective )?(?:or )?(?:emergency )?{CS_REGEX}",
    f"at (?:primary )?(?:first )?(?:repeat )?(?:planned )?(?:elective )?(?:or )?(?:emergency )?{CS_REGEX}",
    f"who had a (?:primary )?{CS_REGEX}",
    f"with a (?:primary )?{CS_REGEX}",
    f"after (?:primary )?{CS_REGEX}",
    f"who had given birth (?:by|via) {CS_REGEX}",
    f"who delivered (?:by|via) (?:their )?(?:primary )?(?:first )?{CS_REGEX}",
    f"scheduled for (?:elective )?(?:planned )?{CS_REGEX}",
    f"with a history of (?:one )?(?:or more )?{CS_REGEX}",
    "post.?operative patients",
    "within one week after delivery",
    "post.?partum ha?emorrhage",
    "post.?partum bleeding",
    "intensive care unit",
    "spinal anaesthesia",
    "syphilis",
    "during (?:their ?)hospital stay",
    "under 1 year old",
    "women who successfully had a vaginal birth",
    "retained fo?etal lung fluid syndrome",
    "myelomeningocele",
    "during ca?esar[ei]an delivery",
    "isthmocele",
    "nicu",
    "congenital heart disease",
    f"post.?{CS_REGEX} section abdominal wall endometriosis",
    "obstetric anal sphincter injury",
    "women with perianal crohn",
    "fathers",
    "husbands",
    "perinatal asphyxia",
    "nursing students",
    "women with oasi",
    "acute kidney injury",
    "cord blood",
    "infants aged under (?:1|one) year",
    "women with inflammatory bowel disease",
    "placenta accreta",
    "ectopic pregnanc",
    "placenta pra?evia",
    "abnormally invasive placenta",
    r"\baip\b",
    "healthcare providers",
    "sepsis",
    "peri.?partum hysterectomy",
    # "post.?partum women",
    "mediate post.?partum period",
    "pregnancy termination",
    "clinician",
    "midwives",
    "doctors",
    "ana?esthetists",
    "obstetricians",
    "gyna?ecologists",
    "obstetrics and gyna?ecology residents",
    "obstetrics and gyna?ecology trainees",
    "physicians",
    "healthcare professionals",
    "staff",
    "operating room personnel",
    "hbsag",
    f"^{CS_REGEX} deliveries",
    "newborn infants",
    "hepatitis",
    "with respiratory distress syndrome",
    # "abortion",
    "established perianal disease",
    "diamniotic",
    "still.?birth",
    r"\beph\b",  # emergency peripartum hysterectomy
    r"\bsmm\b",  # severe maternal morbidity
    "maternal death",
    "with (?:previoius )obstetric anal sphincter injur",
    "with (?:complete )uterine rupture",
    r"\bcdh\b",  # congenital diaphragm hernia
    "very.?low.?birth.?weight",
    "very.pre.?term",
    "gastroschisis",
    "covid.?19",
    "sars-cov-2",
    "who died",
    "ccmv",  # congenital cytomegalovirus
    "with iugr",
    "suture",
    "ecmo",  # extra-corporeal membrane oxygenation
    "with pphn",  # persistent pulmonary hypertension of the newborn
    "induction",
    "accreta",
    "percreta",
    r"\bpas\b",  # placenta accreta spectrum
    r"with nh\n"  # neonatal hypoglycaemia
    "cervical ripening",
    "post.?partum (?:hormonal )iuc?d",
    r"post.?placental (?:\S+ ){0,5}iuc?d",
    "ppiuc?d",
    "abnormal placental? insertion",
    "post.?partum hysterectomy",
    r"at \d weeks",
    "peri.?viable",
    "scar endometriosis",
    "necroti[sz]ing enterocolitis",
    r"\bapcs\b",  # amnion-protective CS
    r"\bpph\b",
    "morbidly adherent placenta",
    f"at time of {CS_REGEX}",
    f"during {CS_REGEX}",
    "uterine artery emboli[sz]ation",
    "hyper.?bilirubina?emia",
    r"with ttn\b",  # transient tachypnoea fo the newborn
    r"\bataad\b",  # aortic repair after acute type A aortic dissection
    r"with a history of s?ptb\b"  # pre-term birth
    r"\bpcos\b",
    r"\bhbv\b",  # heptatis b virus
    "bowel atresia",
    "critically ill",
    r"rpoc",  # retained products of conception
    "achondroplasia",
    "anal sphincter rupture",
    r"\bvbb\b",  # vaginal breech birth
    "pulmonary arterial hypertension",
    "pa-vte",  # pregnancy associated vte
    "pptl",  # postpartum tubal ligation
    "neuromyelitis optica",
    "and subsequently became pregnant",
    "prior uterine rupture",
    "patients with at least one previous cesarean delivery",
]

COHORT_INCLUSION_TERMS = [
    "full.?term",
    "years? post.?partum",
    # Where the cohort includes both CS and VB.
    f"{CS_REGEX}.*(?:vaginal|natural|normal) {BIRTH_REGEX}",
    f"(?:vaginal|natural) {BIRTH_REGEX}.*{CS_REGEX}",
]

EXPOSURE_EXCLUSION_TERMS = [
    # "intrauterine device",
    # f"during {CS_REGEX}",
    f"{CS_REGEX} scar diverticulum",
]

OUTCOME_EXCLUSION_TERMS = [
    "peri.?natal",
    "neo.?nat",
    "post.?partum ha?emorrhage",
    r"\bpph\b",
    "low.birth.?weight",
    "apgar",
    "endometritis",
    "obstetric intervention",
    "vertical transmission",
    "mother-to-child transmission",
    "hiv",
    "oasis",
    "anal sphincter injury",
    "levator ani",
    "levator avulsion",
    "congenital",
    "adherence to guidelines",
    "breech presentation",
    "maternal post.?operative complication",
    "fo?etal post.?operative complication",
    "cord blood",
    f"during {BIRTH_REGEX}",
    "fracture",
    "early.onset sepsis",
    "post.?partum morbidity",
    "pre.?lacteal feeding",
    "pregnancy outcome",
    "newborn health",
    "length of (?:hospital )?stay",
    "jaundice",
    "gastroschisis",
    "hirschsprung",
    "antenatal care",
    "severe (?:acute )?maternal morbidity",
    "pre.?term",
    "brachial plexus",
    "still.?birth",
    "torticollis",
    "meconium aspiration",
    "shoulder dystocia",
    "recall",
    "pyloric stenosis",
    "transfusion",
    "maternal mortality",
    "foal",
    "intracranial hemorrhage",
    "(?:birth|perinatal) hypoxia",
    "(?:birth|perinatal) asphyxia",
    "birth trauma",
    "obstetric trauma",
    "neuraxial block",
    # "scar",
    "diverticula",
    "breast.?feeding initiation",
    "surgical.site infection",
    r"\bssi\b",
    "perineal tear",
    "early discharge",
    "health literacy",
    "post.?partum sterili[sz]ation",
    "ectopic pregnancy",
    "placenta pra?evia",
    "chorioamnionitis",
    "hearing screening",
    "amniotic fluid",
    "uterine dehiscence",
    "gestational age",
    "lus thickness",
    "fundus ha?emorrhage",
    "respiratory distress",
    "newborn respiratory",
    "peri.?partum hysterectomy",
    "uterine rupture",
    "induction of labou?r",
    "blood loss",
    "macrosomia",
    "lacerations",
    r"\bhie\b",
    "abnormally invasive placenta",
    r"\baip\b",
    "incisional hernia",
    "cephalopelvic disproportion",
    "head circumference",
    "eclampsia",
    "vbac",
    "tolac",
    "meconium",
    "placenta accreta",
    "termination",
    "post.?partum uterine cavity volume",
    "perineum",
    "perineal",
    "episiotomy",
    "labou?r",
    "umbilical cord",
    "fees",
    "at discharge",
    "early initiation of breast.?feeding",
    "prophylactic",
    "prophylaxis",
    "hyperbilirubinemia",
    "intra.?partum",
    "atony",
    "near.?miss",
    f"repeat {CS_REGEX}",
    "abortion",
]

STUDY_TYPE_EXCLUSIONS = [
    "meta.?analysis",
    "systematic review",
    "guideline update",
    "commentary",
    "review article",
    "literature review",
    "review of three prospective cohort studies",
    r"^review$",
    "primate model",
    "simulation study",
    "cost analysis",
    "cost-effectiveness analysis",
    "hypothesis",
    "case report",
    "microsimulation",
    "pictorial review",
]

YOUNG_AGE_REGEXES = [
    r"(?<!±)(?<!± )(?<!years and )(?:\b\d|1[012]) month",
    "neonatal period",
    "week",
    "day",
    "hour",
    "minute",
    # "infancy",
    "postoperative",
    "duration of hospital stay",
    "infant mortality",
    "puerperium",
    "first months",
    "post.?partum",
    "discharge",
]

GOOD_AGE_REGEXES = [
    r"(?:1[3-9]|[2-9][0-9])(?:\.\d+)? months",
    "year",
    "school",
    "childhood",
    "adolescence",
    "young adult",
    r"(?:3[6-9][5-9]|[4-9]\d{2}|\d{4})(?:\.\d+)? days",
    "birthday",
]

NOT_TERM_REGEXES = [
    "pre.?term",
    "premature",
    r"(?<!>|≥)(?<!at least )(?<!greater than )(?<!more than )(?:[12][0-9]|3[0-5])(?: ?[.+] ?\d+)? (?:completed )?weeks",
    "post.?term",
    "post.?dates",
    "antenatal period",
    "near.term",
    "before 37 weeks",
]

TERM_REGEXES = [
    r"\bterm",
    "full.?term",
    r"(?:3[7-9]|4[0-2])(?: ?[.+] ?\d+)? (?:completed )?weeks",
    "at recruitment",
    "or greater",
]

ABNORMAL_BIRTH_WEIGHT_REGEXES = [
    "small.for.gestational.age",
    "low.birth.?weight",
    "900.?g",
    r"(?<! \+\/\- )(?:1.?[0-9]{3}|\b\d{3})(?:\.\d+)?[\s-]?g(?:rams?)?\b",
    r"2.?300.?g or less",
    r"<2.?300.?g",
    r"1.?500.?g or less",
    r"<1.?500.?g",
    "<.?10th (?:per)?centile",
    "below the (?:tenth|10th) (?:per)?centile",
    r"\blbw\b",
    r"\bsga\b",
    "macrosomia",
    "over 4.?kg",
    r"4.?[0-9]{3} g",
    "high.birth.?weight",
    "Large.for.gestational.age",
    r"\blga\b",
]

NORMAL_BIRTH_WEIGHT_REGEXES = [
    "normal birth weight",
]


def any_known_outcome(s):
    if pandas.isna(s):
        return False

    for who in ("women", "offspring", "dyad", "societal"):
        for outcome in classify_women_children_society.outcome_mapping[who]:
            for term in classify_women_children_society.outcome_mapping[who][outcome]:
                if re.search(term, s, flags=re.I) is not None:
                    return True
    return False


def any_regex_in_str(regexes, s):
    if pandas.isna(s):
        return False
    for r in regexes:
        if re.search(r, s, flags=re.I) is not None:
            return True
    return False


def other_procedure(row):
    return any_regex_in_str(OTHER_PROCEDURES, row["cohort"]) or any_regex_in_str(
        OTHER_PROCEDURES, row["exposure"]
    )


def cs_included(s):
    if pandas.isna(s):
        return False
    return any_regex_in_str(BIRTH_TYPE_REGEXES, s)


def is_short_term(s):
    if pandas.isna(s):
        return False

    return (
        any_regex_in_str(SHORT_TERM_REGEXES, s)
        or any_regex_in_str(YOUNG_AGE_REGEXES, s)
        or any_regex_in_str(NOT_TERM_REGEXES, s)
    ) and not (
        any_regex_in_str(LONG_TERM_REGEXES, s)
        or any_regex_in_str(VERY_LONG_TERM_REGEXES, s)
        or any_regex_in_str(GOOD_AGE_REGEXES, s)
        or any_regex_in_str(TERM_REGEXES, s)
    )


def rank_relevance(row):
    cohort = row["cohort"]
    if pandas.notna(cohort):
        # Remove anything after the word "excluding" or "excluded" etc
        cohort = cohort.split("exclud")[0]
        # Remove anything after the word "without"
        cohort = cohort.split("without")[0]

    if pandas.notna(row["year"]) and row["year"] < 2014:
        return 0, "Before 2014"
    if any_regex_in_str(["LIC", "LMIC", "UMIC"], row["country_income_group"]):
        return 0, "not HIC"
    if any_regex_in_str(STUDY_TYPE_EXCLUSIONS, row["study type"]):
        return 0, "Wrong study type"

    if any_regex_in_str(
        YOUNG_AGE_REGEXES, row["followup_time"]
    ) and not any_regex_in_str(GOOD_AGE_REGEXES, row["followup_time"]):
        return 0, "followup_time too young"
    if (
        other_procedure(row)
        or (
            any_regex_in_str(COHORT_EXCLUSION_TERMS, cohort)
            and not any_regex_in_str(COHORT_INCLUSION_TERMS, cohort)
        )
        or (
            any_regex_in_str(NOT_TERM_REGEXES, cohort)
            and not any_regex_in_str(TERM_REGEXES, cohort)
        )
        or (
            any_regex_in_str(ABNORMAL_BIRTH_WEIGHT_REGEXES, cohort)
            and not any_regex_in_str(NORMAL_BIRTH_WEIGHT_REGEXES, cohort)
        )
    ):
        return 0, "excluded cohort type"
    if not (cs_included(row["exposure"])):
        return 0, "CS not in exposure"
    if any_regex_in_str(BIRTH_TYPE_REGEXES, row["outcome"]) and not any_known_outcome(
        row["outcome"]
    ):
        return 0, "CS is outcome"
    if is_short_term(row["follow-up time"]):
        return 0, "short term and not long term"
    if is_short_term(row["cross-section timing"]):
        return 0, "Cross section timing short term, young age, or not term"

    result = 1
    reason = ""

    if any_regex_in_str(
        OUTCOME_EXCLUSION_TERMS,
        row["outcome"],
    ):
        reason += ("included and excluded outcome type,")
        if not any_known_outcome(row["outcome"]):
            return 0, "excluded outcome type"

    # Long-term follow-up detected: +1 (as opposed to none found)
    if any_regex_in_str(LONG_TERM_REGEXES, row["follow-up time"]):
        long_term = True
        result += 1
        reason += ",long-term followup time"
    if any_regex_in_str(VERY_LONG_TERM_REGEXES, row["follow-up time"]):
        long_term = True
        result += 2
        reason += ",very long-term followup time"

    if any_regex_in_str(EXPOSURE_EXCLUSION_TERMS, row["exposure"]):
        result -= 1
        reason += ",excluded exposure"

    if any_regex_in_str(COHORT_INCLUSION_TERMS, cohort):
        result += 1
        reason += ",cohort inclusion term"

    return result, reason


data = pandas.read_csv("outputs/merged-abstracts-gemini-country-appended.csv")
data.set_index("dedup_index", inplace=True)

relevance_pairs = data.apply(rank_relevance, axis=1)
data.insert(0, "relevance", relevance_pairs.apply(lambda x: x[0]))
data.insert(1, "reason", relevance_pairs.apply(lambda x: x[1]))
data.to_csv("outputs/ranked-abstracts.csv")

print((data["added_2025"] & (data["relevance"] > 0)).sum())

print(f"Included {(data['relevance'] > 0).sum()}")
