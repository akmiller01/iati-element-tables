from lxml import etree
from lxml.etree import XMLParser
import pandas as pd


XPATH_SEPERATOR = "/"
ATTRIB_SEPERATOR = "@"
SORT_ORDER = {
    "iati-activities/iati-activity": 0,
    "iati-activity/iati-identifier": 1,
    "iati-activity/reporting-org": 2,
    "reporting-org/narrative": 3,
    "iati-activity/title": 4,
    "title/narrative": 5,
    "iati-activity/description": 6,
    "description/narrative": 7,
    "iati-activity/participating-org": 8,
    "participating-org/narrative": 9,
    "iati-activity/other-identifier": 10,
    "other-identifier/owner-org": 11,
    "owner-org/narrative": 12,
    "iati-activity/activity-status": 13,
    "iati-activity/activity-date": 14,
    "activity-date/narrative": 15,
    "iati-activity/contact-info": 16,
    "contact-info/organisation": 17,
    "organisation/narrative": 18,
    "contact-info/department": 19,
    "department/narrative": 20,
    "contact-info/person-name": 21,
    "person-name/narrative": 22,
    "contact-info/job-title": 23,
    "job-title/narrative": 24,
    "contact-info/telephone": 25,
    "contact-info/email": 26,
    "contact-info/website": 27,
    "contact-info/mailing-address": 28,
    "mailing-address/narrative": 29,
    "iati-activity/activity-scope": 30,
    "iati-activity/recipient-country": 31,
    "recipient-country/narrative": 32,
    "iati-activity/recipient-region": 33,
    "recipient-region/narrative": 34,
    "iati-activity/location": 35,
    "location/location-reach": 36,
    "location/location-id": 37,
    "location/name": 38,
    "name/narrative": 39,
    "location/description": 40,
    "location/activity-description": 41,
    "activity-description/narrative": 42,
    "location/administrative": 43,
    "location/point": 44,
    "point/pos": 45,
    "location/exactness": 46,
    "location/location-class": 47,
    "location/feature-designation": 48,
    "iati-activity/sector": 49,
    "sector/narrative": 50,
    "iati-activity/tag": 51,
    "tag/narrative": 52,
    "iati-activity/country-budget-items": 53,
    "country-budget-items/budget-item": 54,
    "budget-item/description": 55,
    "iati-activity/humanitarian-scope": 56,
    "humanitarian-scope/narrative": 57,
    "iati-activity/policy-marker": 58,
    "policy-marker/narrative": 59,
    "iati-activity/collaboration-type": 60,
    "iati-activity/default-flow-type": 61,
    "iati-activity/default-finance-type": 62,
    "iati-activity/default-aid-type": 63,
    "iati-activity/default-tied-status": 64,
    "iati-activity/budget": 65,
    "budget/period-start": 66,
    "budget/period-end": 67,
    "budget/value": 68,
    "iati-activity/planned-disbursement": 69,
    "planned-disbursement/period-start": 70,
    "planned-disbursement/period-end": 71,
    "planned-disbursement/value": 72,
    "iati-activity/capital-spend": 73,
    "iati-activity/transaction": 74,
    "transaction/transaction-type": 75,
    "transaction/transaction-date": 76,
    "transaction/value": 77,
    "transaction/description": 78,
    "transaction/provider-org": 79,
    "provider-org/narrative": 80,
    "transaction/receiver-org": 81,
    "receiver-org/narrative": 82,
    "transaction/disbursement-channel": 83,
    "transaction/sector": 84,
    "transaction/recipient-country": 85,
    "transaction/recipient-region": 86,
    "transaction/flow-type": 87,
    "transaction/finance-type": 88,
    "transaction/aid-type": 89,
    "transaction/tied-status": 90,
    "iati-activity/document-link": 91,
    "document-link/title": 92,
    "document-link/category": 93,
    "document-link/language": 94,
    "document-link/document-date": 95,
    "iati-activity/related-activity": 96,
    "iati-activity/legacy-data": 97,
    "iati-activity/conditions": 98,
    "conditions/condition": 99,
    "condition/narrative": 100,
    "iati-activity/result": 101,
    "result/title": 102,
    "result/description": 103,
    "result/document-link": 104,
    "result/reference": 105,
    "result/indicator": 106,
    "indicator/title": 107,
    "indicator/description": 108,
    "indicator/document-link": 109,
    "indicator/reference": 110,
    "indicator/baseline": 111,
    "baseline/comment": 112,
    "comment/narrative": 113,
    "indicator/period": 114,
    "period/period-start": 115,
    "period/period-end": 116,
    "period/target": 117,
    "target/comment": 118,
    "period/actual": 119,
    "actual/comment": 120,
    "iati-activity/crs-add": 121,
    "crs-add/other-flags": 122,
    "crs-add/loan-terms": 123,
    "loan-terms/repayment-type": 124,
    "loan-terms/repayment-plan": 125,
    "loan-terms/commitment-date": 126,
    "loan-terms/repayment-first-date": 127,
    "loan-terms/repayment-final-date": 128,
    "crs-add/loan-status": 129,
    "loan-status/interest-received": 130,
    "loan-status/principal-outstanding": 131,
    "loan-status/principal-arrears": 132,
    "loan-status/interest-arrears": 133,
    "iati-activity/fss": 134,
    "fss/forecast": 135,
    "iati-organisations/iati-organisation": 136,
    "iati-organisation/organisation-identifier": 137,
    "iati-organisation/name": 138,
    "iati-organisation/reporting-org": 139,
    "iati-organisation/total-budget": 140,
    "total-budget/period-start": 141,
    "total-budget/period-end": 142,
    "total-budget/value": 143,
    "total-budget/budget-line": 144,
    "budget-line/value": 145,
    "budget-line/narrative": 146,
    "iati-organisation/recipient-org-budget": 147,
    "recipient-org-budget/recipient-org": 148,
    "recipient-org/narrative": 149,
    "recipient-org-budget/period-start": 150,
    "recipient-org-budget/period-end": 151,
    "recipient-org-budget/value": 152,
    "recipient-org-budget/budget-line": 153,
    "iati-organisation/recipient-region-budget": 154,
    "recipient-region-budget/recipient-region": 155,
    "recipient-region-budget/period-start": 156,
    "recipient-region-budget/period-end": 157,
    "recipient-region-budget/value": 158,
    "recipient-region-budget/budget-line": 159,
    "iati-organisation/recipient-country-budget": 160,
    "recipient-country-budget/recipient-country": 161,
    "recipient-country-budget/period-start": 162,
    "recipient-country-budget/period-end": 163,
    "recipient-country-budget/value": 164,
    "recipient-country-budget/budget-line": 165,
    "total-expenditure/period-start": 166,
    "total-expenditure/period-end": 167,
    "total-expenditure/value": 168,
    "total-expenditure/expense-line": 169,
    "expense-line/value": 170,
    "expense-line/narrative": 171,
    "iati-organisation/document-link": 172,
    "document-link/recipient-country": 173,
}
REQUIRED_CHILDREN = [
    ("//iati-activities", "iati-activity"),
    ("//iati-activity", "iati-identifier"),
    ("//iati-activity", "reporting-org"),
    ("//iati-activity", "title"),
    ("//iati-activity", "description"),
    ("//iati-activity", "participating-org"),
    ("//iati-activity", "activity-status"),
    ("//iati-activity", "activity-date"),
    ("//iati-activity", "transaction"),
    ("//iati-activity", "budget"),
    ("//iati-activity", "planned-disbursement"),
    ("//iati-activity", "recipient-country"),
    ("//iati-activity", "recipient-region"),
    ("//iati-activity", "sector"),
    ("//iati-activity", "tag"),
    ("//iati-activity", "policy-marker"),
    ("//iati-activity", "humanitarian-scope"),
    ("//iati-activity", "default-flow-type"),
    ("//iati-activity", "default-finance-type"),
    ("//iati-activity", "default-aid-type"),
    ("//point", "pos"),
    ("//country-budget-items", "budget-item"),
    ("//budget", "period-start"),
    ("//budget", "period-end"),
    ("//budget", "value"),
    ("//planned-disbursement", "period-start"),
    ("//planned-disbursement", "period-end"),
    ("//planned-disbursement", "value"),
    ("//transaction", "transaction-type"),
    ("//transaction", "transaction-date"),
    ("//transaction", "value"),
    ("//transaction", "provider-org"),
    ("//transaction", "receiver-org"),
    ("//transaction", "recipient-country"),
    ("//transaction", "recipient-region"),
    ("//transaction", "sector"),
    ("//transaction", "flow-type"),
    ("//transaction", "finance-type"),
    ("//transaction", "aid-type"),
    ("//document-link", "title"),
    ("//document-link", "category"),
    ("//document-link", "language"),
    ("//reporting-org", "narrative"),
    ("//result", "title"),
    ("//result", "indicator"),
    ("//indicator", "title"),
    ("//period", "period-start"),
    ("//period", "period-end"),
    ("//title", "narrative"),
    ("//description", "narrative"),
    ("//organisation", "narrative"),
    ("//department", "narrative"),
    ("//person-name", "narrative"),
    ("//job-title", "narrative"),
    ("//mailing-address", "narrative"),
    ("//name", "narrative"),
    ("//activity-description", "narrative"),
    ("//condition", "narrative"),
    ("//comment", "narrative"),
    ("//iati-organisation", "total-budget"),
    ("//iati-organisation", "total-expenditure")
]
REQUIRED_ATTRIBUTES = [
    ("//iati-activity", "humanitarian"),
    ("//document-link", "format"),
    ("//category", "code"),
    ("//language", "code"),
    ("//budget", "type"),
    ("//budget", "status"),
    ("//value", "currency"),
    ("//value", "value-date"),
    ("//transaction-type", "code"),
    ("//transaction-date", "iso-date"),
    ("//period-start", "iso-date"),
    ("//period-end", "iso-date"),
    ("//planned-disbursement", "type"),
    ("//recipient-country", "code"),
    ("//recipient-country", "percentage"),
    ("//recipient-region", "code"),
    ("//recipient-region", "percentage"),
    ("//recipient-region", "vocabulary"),
    ("//sector", "code"),
    ("//sector", "percentage"),
    ("//sector", "vocabulary"),
    ("//tag", "code"),
    ("//tag", "vocabulary"),
    ("//humanitarian-scope", "type"),
    ("//humanitarian-scope", "vocabulary"),
    ("//humanitarian-scope", "code"),
    ("//default-flow-type", "code"),
    ("//default-finance-type", "code"),
    ("//default-aid-type", "code"),
    ("//default-aid-type", "vocabulary"),
    ("//flow-type", "code"),
    ("//finance-type", "code"),
    ("//aid-type", "code"),
    ("//aid-type", "vocabulary"),
]


def iati_order(xml_element):
    family_tag = "{}{}{}".format(xml_element.getparent().tag, XPATH_SEPERATOR, xml_element.tag)
    try:
        return SORT_ORDER[family_tag]
    except KeyError:
        return 999


def iati_order_xpath(xpath_key):
    xpath_without_attribute = xpath_key.split(ATTRIB_SEPERATOR)[0]
    xpath_split = [elem_xpath.split("[")[0] for elem_xpath in xpath_without_attribute.split(XPATH_SEPERATOR)]
    sort_orders = [0]
    if len(xpath_split) > 1:
        for i in range(1, len(xpath_split)):
            elem_tag = xpath_split[i]
            parent_tag = xpath_split[i-1]
            family_tag = "{}{}{}".format(parent_tag, XPATH_SEPERATOR, elem_tag)
            try:
                sort_orders.append(SORT_ORDER[family_tag])
            except KeyError:
                sort_orders.append(999)
    return sort_orders, xpath_key


def remove_xpath_index(relative_xpath):
    split_path = relative_xpath.split("[")
    indexless_path = "[".join(split_path[:-1])
    return indexless_path


def fetch_xpath_number(absolute_xpath):
    split_path = absolute_xpath.split("[")
    path_index = int(split_path[-1][:-1])
    return path_index


def increment_xpath(absolute_xpath):
    split_path = absolute_xpath.split("[")
    indexless_path = "[".join(split_path[:-1])
    path_index = int(split_path[-1][:-1])
    path_index += 1
    incremented_xpath = "{}[{}]".format(indexless_path, path_index)
    return incremented_xpath


def find_identifier(element, root):
    parent = element
    while parent.tag != 'iati-activity':
        if parent == root:
            return ""
        parent = parent.getparent()
    identifier = parent.xpath('iati-identifier/text()')
    return identifier[0] if identifier else ""


def recursive_tree_traversal(element, absolute_xpath, element_dictionary, max_depth, depth, max_siblings):
    # Main value
    element_value = str(element.text) if element.text else ""
    while absolute_xpath in element_dictionary:
        absolute_xpath = increment_xpath(absolute_xpath)
        if fetch_xpath_number(absolute_xpath) > max_siblings:
            return element_dictionary

    element_dictionary[absolute_xpath] = element_value

    # Attribute values
    element_attributes = element.attrib
    for attrib_key in element_attributes.keys():
        attribute_xpath = ATTRIB_SEPERATOR.join([absolute_xpath, attrib_key])
        attribute_value = str(element_attributes[attrib_key]) if element_attributes[attrib_key] else ""
        element_dictionary[attribute_xpath] = attribute_value

    # Child values
    element_children = element.getchildren()
    if not element_children or depth == max_depth:
        return element_dictionary
    else:
        for child_elem in element_children:
            child_depth = depth + 1
            child_elem_tag = child_elem.tag
            child_absolute_xpath = XPATH_SEPERATOR.join([absolute_xpath, child_elem_tag]) + "[1]"
            element_dictionary = recursive_tree_traversal(child_elem, child_absolute_xpath, element_dictionary, max_depth, child_depth, max_siblings)

    return element_dictionary


def melt_iati(xml_filename, extract_xpath, max_depth=1, max_siblings=10):
    large_parser = XMLParser(huge_tree=True)

    parser = etree.XMLParser(remove_blank_text=True)
    tree = etree.parse(xml_filename, parser=large_parser)

    # Sort IATI order
    for parent in tree.xpath('//*[./*]'):  # Search for parent elements, reorder
        parent[:] = sorted(parent, key=lambda x: iati_order(x))

    # Add missing mandatory elements
    for required_child in REQUIRED_CHILDREN:
        parent_xpath, child_tag = required_child
        matching_parents = tree.xpath(parent_xpath)
        for matching_parent in matching_parents:
            child_query = matching_parent.xpath(child_tag)
            if not child_query:
                etree.SubElement(matching_parent, child_tag)

    # Add missing mandatory attributes
    for required_attrib in REQUIRED_ATTRIBUTES:
        parent_xpath, attrib_key = required_attrib
        matching_parents = tree.xpath(parent_xpath)
        for matching_parent in matching_parents:
            if attrib_key not in matching_parent.attrib:
                matching_parent.attrib[attrib_key] = ""

    root = tree.getroot()
    extracts = root.xpath(extract_xpath)
    extracts_list = []
    for extract in extracts:
        depth = 0
        extract_dict = dict()
        if extract_xpath != "iati-activity":
            extract_dict["iati-activity/iati-identifier[1]"] = find_identifier(extract, root)
        extract_dict = recursive_tree_traversal(extract, extract_xpath, extract_dict, max_depth, depth, max_siblings)
        extracts_list.append(extract_dict)

    e_df = pd.DataFrame(extracts_list, dtype=str)
    e_df = e_df.reindex(sorted(e_df.columns, key=iati_order_xpath), axis=1)
    return e_df
