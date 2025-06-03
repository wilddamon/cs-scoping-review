import re
import sys

import pandas

XSL = """<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="xml" omit-xml-declaration="no" indent="yes"/>
<xsl:strip-space elements="*"/>
<xsl:template match="/records">
    <xsl:copy>
        <xsl:apply-templates select="rec"/>
    </xsl:copy>
</xsl:template>
<xsl:template match="rec">
    <xsl:copy>
        <xsl:copy-of select="header/controlInfo/artinfo/tig/atl"/>
        <year><xsl:value-of select="header/controlInfo/pubinfo/dt/@year"/></year>
        <authors>
            <xsl:for-each select="header/controlInfo/artinfo/aug/au">
                <xsl:value-of select="." />;
            </xsl:for-each>
        </authors>
        <xsl:copy-of select="header/controlInfo/artinfo/ab"/>

        <xsl:for-each select="header/controlInfo/artinfo/ui">
             <xsl:variable name="ui_type" select="./@type"/>
             <xsl:element name="{$ui_type}">
                 <xsl:value-of select="." />
             </xsl:element>
        </xsl:for-each>

        <xsl:copy-of select="header/controlInfo/jinfo/jtl" />
        <xsl:copy-of select="header/controlInfo/artinfo/pubtype"/>
        <doctypes>
             <xsl:for-each select="header/controlInfo/artinfo/doctype">
                  <xsl:value-of select="." />;
             </xsl:for-each>
        </doctypes>
        <xsl:copy-of select="header/controlInfo/language"/>
    </xsl:copy>
</xsl:template>
</xsl:stylesheet>"""


def get_data(path):
    data = pandas.read_xml(path, stylesheet=XSL)
    data["authors"] = data["authors"].apply(tidy_list_str)
    data["doctypes"] = data["doctypes"].apply(tidy_list_str)
    data = data.rename(
        columns={
            "atl": "title",
            "dt": "year",
            "ab": "abstract",
            "jtl": "journal",
            "doctypes": "publication types",
        }
    )
    data["pmid"] = data["pmid"].apply(tidy_pmid)
    return data


def tidy_list_str(s):
    if pandas.isna(s):
        return s
    authors = s.split(";")
    result = []
    for a in authors:
        result.append(a.strip())
    return ";".join(result)


def tidy_pmid(s):
    # Remove "NLM" prefix
    if (
        pandas.isna(s)
        or isinstance(s, int)
        or isinstance(s, float)
        or not s.startswith("NLM")
    ):
        return s
    return s[3:]


def main():
    get_data("database-search-results/CINAHL/cinahl_export.xml").to_csv("outputs/database-search-results/cinahl.csv")
    get_data("database-search-results/PsycINFO/psycinfo_export.xml").to_csv("outputs/database-search-results/psycinfo.csv")


if __name__ == "__main__":
    sys.exit(main())
