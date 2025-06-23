import pandas
import re


INCOME_GROUPINGS = {
    "LIC": pandas.read_excel("data/world_bank_country_groups.xlsx", "LIC")[
        "LICs"
    ].to_list(),
    "LMIC": pandas.read_excel("data/world_bank_country_groups.xlsx", "LMIC")[
        "LMICs"
    ].to_list(),
    "UMIC": pandas.read_excel("data/world_bank_country_groups.xlsx", "UMIC")[
        "UMICs"
    ].to_list(),
    "HIC": pandas.read_excel("data/world_bank_country_groups.xlsx", "HIC")[
        "HICs"
    ].to_list(),
}


COUNTRY_ALIASES = {
    "Australia": [
        "New South Wales",
        "Grampian region",
        "Western Australia",
    ],
    "Belgium": ["8 European countries"],
    "Brunei": ["Brunei Darussalam"],
    "Canada": ["Newfoundland and Labrador, Canada", "CANADA", "Quebec"],
    "China": [
        "Hunan Province",
        "Shanghai",
        "Mainland China",
        "Peoples Republic of China",
        "china",
        "Chinese",
    ],
    "Côte d'Ivoire": ["Ivory Coast", "Côte dIvoire"],
    "Croatia": ["Vukovar"],
    "Cyprus": ["8 European countries"],
    "Democratic Republic of the Congo": ["DR Congo", "Democratic Republic of Congo"],
    "Denmark": ["Nordic countries"],
    "Estonia": ["8 European countries"],
    "Finland": ["Nordic countries"],
    "Germany": [
        "Munich",
        "Dresden",
        "8 European countries",
        "Multiple European countries",
    ],
    "Greece": ["Multiple European countries"],
    "Hungary": ["8 European countries"],
    "Iceland": ["Multiple European countries"],
    "India": [
        "bangalore",
        "Babylon",
        "india",
        "Tamilnadu",
        "Punjab",
        "Southern Punjab",
        "Gujarat",
        "Delhi",
        "Bihar",
        "Indian",
        "Kashmir",
    ],
    "Indonesia": ["Jakarta"],
    "Iran": ["Isfahan", "Shiraz"],
    "Iraq": [
        "Thi-Qar province",
        "Iraqi",
        "Basrah",
        "IRAQ",
        "Baghdad",
        "baghdad",
    ],
    "Israel": ["Jerusalem"],
    "Italy": [
        "Rome",
        "8 European countries",
        "Multiple European countries",
        "Italian",
        "Campania",
        "Sardinia",
    ],
    "Jordan": ["Jordanian"],
    "Lao People's Democratic Republic": ["Lao PDR"],
    "Lithuania": ["Multiple European countries"],
    "Kazakhstan": ["Astana"],
    "Kyrgyzstan": ["Kyrgyz"],
    "Malaysia": ["Sabah", "Perak"],
    "Malta": ["Gozo"],
    "Mexico": ["Yucatan"],
    "Netherlands": ["Multiple European countries", "Dutch"],
    "Nigeria": ["Ebonyi", "Nigerian"],
    "Norway": ["Nordic countries", "Scandinavia", "Norwegian"],
    "Pakistan": [
        "Lahore",
        "Sialkot",
        "Peshawar Khyber Pakhtunkhwa",
        "Peshawar",
    ],
    "Poland": ["Multiple European countries"],
    "Puerto Rico": ["Puerto Rican"],
    "Romania": ["Cluj-Napoca"],
    "Saudi Arabia": ["KSA", "Riyadh", "Saudi"],
    "Spain": ["8 European countries", "Multiple European countries", "Catalonia"],
    "South Africa": ["KwaZulu-Natal"],
    "South Korea": ["Korea", "Republic of Korea", "Korean"],
    "Sweden": [
        "Göteborg",
        "8 European countries",
        "Nordic countries",
        "Scandinavia",
        "Swede",
        "Stockholm",
    ],
    "Switzerland": ["Swiss"],
    "Tanzania": ["Zanzibar"],
    "Timor-Leste": ["Timor Leste"],
    "Türkiye": ["Turkey", "T√ºrkiye", "Turkiye", "Torkiye", "Turkish"],
    "United Arab Emirates": ["UAE"],
    "United Kingdom": [
        "UK",
        "England",
        "england",
        "Ireland",
        "Scotland",
        "Wales",
        "London",
        "Great Britain",
        "Northern Ireland",
        "Multiple European countries",
        "Irish",
    ],
    "United States of America": [
        "America",
        "American",
        "United States",
        "USA",
        "US",
        "U.S.",
        "U.S.A.",
        "Boston",
        "California",
        "Florida",
        "Iowa",
        "Kentucky",
        "Maryland",
        "Missouri",
        "New York",
        "North Carolina",
        "Pennsylvania",
        "Southern California",
        "Texas",
        "Utah",
        "Wisconsin",
    ],
    "Uzbekistan": ["Bukhara"],
}

NON_COUNTRY_STRS = [
    "150 countries",
    "Africa",
    "African",
    "Asia",
    "Asia-Pacific",
    "Asian continent",
    "Asian countries",
    "Black African",
    "Caribbean region",
    "developing countries",
    "Developing countries",
    "developing nations",
    "Europe",
    "European",
    "European Union",
    "Global",
    "Indian subcontinent",
    "International",
    "Latin America",
    "LMICs",
    "low-income countries",
    "Newborn Health",
    "North America",
    "Northern America",
    "Oceania",
    "South Asia",
    "south asian region",
    "South East Asia",
    "Southeast Asia",
    "Sub-Sahara Africa",
    "Sub-Saharan Africa",
    "Twenty-nine countries from the World Health Organization Multicountry Survey on Maternal",
    "Western European countries",
    "Western countries",
    "worldwide",
]


def country_income_group(single_country_str):
    for classification in INCOME_GROUPINGS:
        if single_country_str in INCOME_GROUPINGS[classification]:
            return classification
    return None

def country_income_group_list(country_list):
    result = set()
    for country in country_list:
        ig = country_income_group(country)
        if ig is not None:
            result.add(ig)
    return list(result)



def country_list(country_str):
    if pandas.isna(country_str):
        return []

    if not isinstance(country_str, str):
        raise Exception(f"Not a country string: {type(country_str)}")

    country_str = country_str.strip()
    if len(country_str) == 0 or country_str == "N/A":
        return []

    # Remove surrounding [] if json formatting snuck in.
    if country_str[0] == "[" and country_str[-1] == "]":
        country_str = re.sub("'", "", country_str[1:-1])
        country_str = re.sub('"', "", country_str)

    # Remove "and" and "the"
    country_str = re.sub(r"(?<!Antigua)(?<!Bosnia) and ", ",", country_str)
    country_str = re.sub(r";", ",", country_str)
    country_str = re.sub(r"\(", ",", country_str)
    country_str = re.sub(r"\)", ",", country_str)
    country_str = re.sub(r"(?<!of )\bthe\b", " ", country_str)
    country_str = re.sub(r"\s+", " ", country_str)

    countries = country_str.split(",")

    result = set()
    for country in countries:
        country = country.strip()
        if country == "" or country == "N/A":
            continue
        alias_found = False
        for aliased_country in COUNTRY_ALIASES:
            if country in COUNTRY_ALIASES[aliased_country]:
                alias_found = True
                result.add(aliased_country)
        if not alias_found:
            result.add(country)
    return list(result)



def is_lic_lmic(country_str):
    countries = country_list(country_str)

    for country in countries:
        country = country.strip()

        if (
            country in INCOME_GROUPINGS["LIC"]
            or country in INCOME_GROUPINGS["LMIC"]
            or re.search("sub.?saharan? africa", country, flags=re.I) is not None
            or re.search("low.?income", country, flags=re.I) is not None
            or (
                re.search("low-? and middle.income countries", country, flags=re.I)
                is not None
            )
            or re.search("developing countries", country, flags=re.I) is not None
            or re.search("developing nations", country, flags=re.I) is not None
            or re.search("lmic", country, flags=re.I) is not None
        ):
            return True
    return False


data = pandas.read_csv("outputs/merged-abstracts-gemini-appended.csv")
data.set_index("dedup_index", inplace=True)
data["lic/lmic"] = data["country"].apply(is_lic_lmic)
countries = data["country"].apply(country_list)
data["country"] = countries.apply(lambda l: ";".join(l))

income_groups = countries.apply(country_income_group_list)
print(income_groups.value_counts(dropna=False))
data["country_income_group"] = income_groups.apply(lambda l: ";".join(l))

data.to_csv("outputs/merged-abstracts-gemini-country-appended.csv")
