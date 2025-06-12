import re
import sys

import pandas


def scan_opening_brace(s):
    depth = 0
    print(s)
    for i in range(len(s)-2, 0, -1):
        c= s[i]
        if c == ")":
            print(s[:i])
            depth += 1
        if c == "(" and depth == 0:
            print(s[:i])
            return i
        elif c == "(":
            depth -= 1


def normalise_abstract(s):
    s = s.lower().strip()
    s = re.sub(r"(?:\s+|;|:|\+\/-|\+-|-)", "", s)
    s = re.sub(r"\(s\)", "", s)
    s = re.sub(r"s", "", s)
    if s[-1] == ")":
        s = s[:scan_opening_brace(s)]
    s = s.split("copyright")[0]
    s = s.split("!(c)")[0]
    s = s.split("(c)")[0]
    return s


def drop_non_na_duplicates(data, column):
    return data[~data[column].duplicated() | data[column].isna()]


def merge_set(name, path, combined_data):
    print(f"\nMerging {name}")
    data = pandas.read_csv(path)

    original_data_length = len(data)
    print(f"Found {len(data)} records.")

    result = pandas.concat([combined_data, data], ignore_index=True)
    original_combined_length = len(result)
    print(f"Combined length: {original_combined_length}")

    result = drop_non_na_duplicates(result, "pmid")
    result = drop_non_na_duplicates(result, "doi")
    print(
        f"Removed {original_combined_length - len(result)} duplicated pmids or dois, now {len(result)}"
    )

    l = len(result)
    result["lower_abstract"] = result["abstract"].apply(normalise_abstract)
    result = drop_non_na_duplicates(result, "lower_abstract")
    print(f"Removed {l - len(result)} identical abstracts, now {len(result)}")

    l = len(result)
    result = result.drop_duplicates(["dedup_index"])
    print(
        f"Removed {l - len(result)} duplicate title/year/first author combos, now {len(result)}"
    )

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
        "fetalheartrateabnormalitiesduringandafterexternalcephalicversion:whichfetusesareatriskandhowaretheydelivered?;2018;kuppens",
        "stress,sleepqualityandunplannedcaesareansectioninpregnantwomen;2017;yi-li",
        "combinedlaparoscopyandhysteroscopyvs.uterinecurettageintheuterinearteryembolization-basedmanagementofcesareanscarpregnancy:acohortstudy;2014;xue",
        "revisitingheadcircumferenceofbraziliannewbornsinpublicandprivatematernityhospitals;2017;dosocorroteixeiraamorim",
        "theshapeofuterinecontractionsandlaborprogressinthespontaneousactivelabor;2015;ebrahimzadehzagami",
        "thecomparisonofseruminterleukin-6ofmothersinvaginalandelectivecesareandelivery;2014;mojaveri",
        "methadonedoseasadeterminantofinfantoutcomeduringtheperiandpostnatalperiod;2018;mei",
        "clinicalassociationofserumcalciumlevelsinpre-eclampsiaandgestationalhypertensionpatients:aprospectiveobservationalstudy;2019;lakshmikanthamma",
        "evaluationofpostplacentaltranscaesarean/vaginaldeliveryintrauterinedevice(ppiucd)intermsofawareness,acceptanceandexpulsioninserviceshospital,lahore;2016;tariq",
        "theincidenceandriskfactorsofsurgicalwoundinfectionafterabdominalhysterectomyincancerouswomen;2021;mahdavi",
        "preferredmodeofdeliveryiniraqiprimiparouswomen;2021;salihal-asadi",
        "evaluationoftheanalgesicefficacyofmelatonininpatientsundergoingcesareansectionunderspinalanesthesia:aprospectiverandomizeddouble-blindstudy;2016;khezri",
        "employment-relatedphysicalactivityduringpregnancy:birthweightandstillbirthdeliveryinkarachi,pakistan;2022;alirizvi",
        "comparisonofintrathecallow-doselevobupivacainewithlevobupivacaine-fentanylandlevobupivacaine-sufentanilcombinationsforcesareansection;2019;sahin",
        "previousexposuretoanesthesiaandautismspectrumdisorder(asd):apuertoricanpopulation-basedsiblingcohortstudy;2015;creagh",
        "implementationofclinicalpathwaysinmalaysia:canclinicalpathwaysimprovethequalityofcare?;2016;i.",
        "double-ballooncathetercomparedtovaginaldinoprostoneforcervicalripeninginobesewomenatterm;[comparaisonsondeadoubleballonnet-dinoprostonepourlamaturationcervicalechezlesfemmesobesesaterme];2018;grange",
        "menstrualpatternfollowingtuballigation:ahistoricalcohortstudy;2016;sadatmahalleh",
        "predictorsformoderatetosevereacutepostoperativepainaftercesareansection;2016;decarvalhoborges",
        "managementofbreechpresentationatterm:aretrospectivecohortstudyof10yearsofexperience;2016;rodriguez",
        "racialdisparityinpostpartumreadmissionduetohypertensionamongwomenwithpregnancy-associatedhypertension;2020;chornock",
        "portablerespiratorypolygraphymonitoringofobesemothersthefirstnightaftercaesareansectionwithbupivacaine/morphine/fentanylspinalanaesthesia;2017;hein",
        "women'spelvicfloormusclestrengthandurinaryandanalincontinenceafterchildbirth:across-sectionalstudy;2017;priscilatavares",
        "pregnancy,parturition,parityandpositioninthefamily.anyinfluenceonthedevelopmentofpaediatricinguinalhernia/hydrocele?;2014;irabor",
        "relationshipbetweengestationalriskandtypeofdeliveryinhighriskpregnancy;2020;benattiantunes",
    ]

    l = len(combined_data)
    combined_data = combined_data.drop(index=manual_dupes)

    title_vc = combined_data["title"].value_counts()
    print(title_vc[title_vc > 1])

    combined_data.to_csv("outputs/basic-processing/merged-abstracts.csv")
    print(f"Removed {l - len(combined_data)} manually identified duplicates, now {len(combined_data)}")
    print(
        f"Found {len(combined_data) - len(pubmed_data)} additional records from non-PubMed sources"
    )


if __name__ == "__main__":
    sys.exit(main())
