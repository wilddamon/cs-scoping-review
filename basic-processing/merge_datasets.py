import re
import sys

import pandas


def scan_opening_brace(s):
    depth = 0
    for i in range(len(s) - 2, 0, -1):
        c = s[i]
        if c == ")":
            depth += 1
        if c == "(" and depth == 0:
            return i
        elif c == "(":
            depth -= 1


def normalise_abstract(s):
    s = s.lower().strip()
    s = s.split("copyright")[0]
    s = s.split("!(c)")[0]
    s = s.split("(c)")[0]
    s = re.sub(r"(?:\s+|;|:|\+\/-|\+-|-)", "", s)
    s = re.sub(r"\(s\)", "", s)
    s = re.sub(r"s", "", s)
    if s[-1] == ")":
        s = s[: scan_opening_brace(s)]
    return s


def drop_non_na_duplicates(data, column):
    dupe = data[column].duplicated() & data[column].notna()
    return data[~dupe], data[dupe]


def merge_set(name, path, combined_data):
    print(f"\nMerging {name}")
    data = pandas.read_csv(path)

    original_data_length = len(data)
    print(f"Found {len(data)} records.")

    result = pandas.concat([combined_data, data], ignore_index=True)
    original_combined_length = len(result)
    print(f"Combined length: {original_combined_length}")

    result, pmid_dupes = drop_non_na_duplicates(result, "pmid")
    result, doi_dupes = drop_non_na_duplicates(result, "doi")
    print(
        f"Removed {original_combined_length - len(result)} duplicated pmids or dois, now {len(result)}"
    )

    l = len(result)
    result, dedup_dupes = drop_non_na_duplicates(result, "dedup_index")
    print(
        f"Removed {l - len(result)} duplicate title/year/first author combos, now {len(result)}"
    )

    l = len(result)
    result["lower_abstract"] = result["abstract"].apply(normalise_abstract)
    result, ab_dupes = drop_non_na_duplicates(result, "lower_abstract")
    print(f"Removed {l - len(result)} identical abstracts, now {len(result)}")

    with pandas.ExcelWriter(f"outputs/basic-processing/dupes-removed/{name}.xlsx") as writer:
        pmid_dupes.to_excel(writer, sheet_name="pmid")
        doi_dupes.to_excel(writer, sheet_name="doi")
        dedup_dupes.to_excel(writer, sheet_name="dedup")
        ab_dupes.to_excel(writer, sheet_name="abstract")

    print(f"Added {original_combined_length - len(result)} unique records")
    print(f"Total records: {len(result)}")
    return result


def main():
    pubmed_data = pandas.read_csv("outputs/basic-processing/pubmed.csv")
    print(f"Pubmed: {len(pubmed_data)}")

    combined_data = merge_set(
        "cinahl",
        "outputs/basic-processing/cinahl.csv",
        pubmed_data,
    )
    combined_data = merge_set(
        "medline",
        "outputs/basic-processing/medline.csv",
        combined_data,
    )
    combined_data = merge_set(
        "psycinfo",
        "outputs/basic-processing/psycinfo.csv",
        combined_data,
    )
    combined_data = merge_set(
        "embase",
        "outputs/basic-processing/embase.csv",
        combined_data,
    )
    combined_data = merge_set(
        "scopus",
        "outputs/basic-processing/scopus.csv",
        combined_data,
    )

    combined_data.set_index("dedup_index", inplace=True)

    # Drop some dupes manually discovered by inspecting articles with identical titles
    manual_dupes = [
        "fetalheartrateabnormalitiesduringandafterexternalcephalicversionwhichfetusesareatriskandhowaretheydelivered2018kuppens",
        "stresssleepqualityandunplannedcaesareansectioninpregnantwomen2017yili",
        "combinedlaparoscopyandhysteroscopyvsuterinecurettageintheuterinearteryembolizationbasedmanagementofcesareanscarpregnancyacohortstudy2014xue",
        "revisitingheadcircumferenceofbraziliannewbornsinpublicandprivatematernityhospitals2017dosocorroteixeiraamorim",
        "theshapeofuterinecontractionsandlaborprogressinthespontaneousactivelabor2015ebrahimzadehzagami",
        "thecomparisonofseruminterleukin6ofmothersinvaginalandelectivecesareandelivery2014mojaveri",
        "methadonedoseasadeterminantofinfantoutcomeduringtheperiandpostnatalperiod2018mei",
        "clinicalassociationofserumcalciumlevelsinpreeclampsiaandgestationalhypertensionpatientsaprospectiveobservationalstudy2019lakshmikanthamma",
        "evaluationofpostplacentaltranscaesareanvaginaldeliveryintrauterinedeviceppiucdintermsofawarenessacceptanceandexpulsioninserviceshospitallahore2016tariq",
        "theincidenceandriskfactorsofsurgicalwoundinfectionafterabdominalhysterectomyincancerouswomen2021mahdavi",
        "preferredmodeofdeliveryiniraqiprimiparouswomen2021salihalasadi",
        "evaluationoftheanalgesicefficacyofmelatonininpatientsundergoingcesareansectionunderspinalanesthesiaaprospectiverandomizeddoubleblindstudy2016khezri",
        "employmentrelatedphysicalactivityduringpregnancybirthweightandstillbirthdeliveryinkarachipakistan2022alirizvi",
        "comparisonofintrathecallowdoselevobupivacainewithlevobupivacainefentanylandlevobupivacainesufentanilcombinationsforcesareansection2019sahin",
        "previousexposuretoanesthesiaandautismspectrumdisorderasdapuertoricanpopulationbasedsiblingcohortstudy2015creagh",
        "implementationofclinicalpathwaysinmalaysiacanclinicalpathwaysimprovethequalityofcare2016i",
        "doubleballooncathetercomparedtovaginaldinoprostoneforcervicalripeninginobesewomenattermcomparaisonsondeadoubleballonnetdinoprostonepourlamaturationcervicalechezlesfemmesobesesaterme2018grange",
        "menstrualpatternfollowingtuballigationahistoricalcohortstudy2016sadatmahalleh",
        "predictorsformoderatetosevereacutepostoperativepainaftercesareansection2016decarvalhoborges",
        "managementofbreechpresentationattermaretrospectivecohortstudyof10yearsofexperience2016rodriguez",
        "racialdisparityinpostpartumreadmissionduetohypertensionamongwomenwithpregnancyassociatedhypertension2020chornock",
        "portablerespiratorypolygraphymonitoringofobesemothersthefirstnightaftercaesareansectionwithbupivacainemorphinefentanylspinalanaesthesia2017hein",
        "womenspelvicfloormusclestrengthandurinaryandanalincontinenceafterchildbirthacrosssectionalstudy2017priscilatavares",
        "pregnancyparturitionparityandpositioninthefamilyanyinfluenceonthedevelopmentofpaediatricinguinalherniahydrocele2014irabor",
        "relationshipbetweengestationalriskandtypeofdeliveryinhighriskpregnancy2020benattiantunes",
    ]

    l = len(combined_data)
    combined_data = combined_data.drop(index=manual_dupes)

    title_vc = combined_data["title"].value_counts()
    print(title_vc[title_vc > 1])

    combined_data[["title", "authors", "year", "abstract", "journal", "pmid", "doi"]].to_csv(
        "outputs/basic-processing/merged-abstracts.csv"
    )
    print(
        f"Removed {l - len(combined_data)} manually identified duplicates, now {len(combined_data)}"
    )
    print(
        f"Found {len(combined_data) - len(pubmed_data)} additional records from non-PubMed sources"
    )


if __name__ == "__main__":
    sys.exit(main())
