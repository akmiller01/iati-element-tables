import argparse
import json
import glob
import os
import progressbar
import pandas as pd
import requests
from tabulize import melt_iati
from lxml import etree


table_descriptions = [
    ("activities.csv", "iati-activity", 1),
    ("transactions.csv", "iati-activity/transaction", 10),
    ("budgets.csv", "iati-activity/budget", 10),
    ("planned_disbursements.csv", "iati-activity/planned-disbursement", 10),
    ("participating_orgs.csv", "iati-activity/participating-org", 10),
    ("transact_provider_orgs.csv", "iati-activity/transaction/provider-org", 10),
    ("transact_receiver_orgs.csv", "iati-activity/transaction/receiver-org", 10),
    ("activity_recipient_countrys.csv", "iati-activity/recipient-country", 10),
    ("activity_recipient_regions.csv", "iati-activity/recipient-region", 10),
    ("transact_recipient_countrys.csv", "iati-activity/transaction/recipient-country", 10),
    ("transact_recipient_regions.csv", "iati-activity/transaction/recipient-region", 10),
    ("locations.csv", "iati-activity/location", 10),
    ("activity_sectors.csv", "iati-activity/sector", 10),
    ("transaction_sectors.csv", "iati-activity/transaction/sector", 10),
    ("tags.csv", "iati-activity/tag", 10),
    ("policy_markers.csv", "iati-activity/policy-marker", 10),
    ("humanitarian_scopes.csv", "iati-activity/humanitarian-scope", 10),
    ("organisations.csv", "iati-organisation", 10),
    ("organisation_total_budget.csv", "iati-organisation/total-budget", 10),
    ("organisation_total_expenditure.csv", "iati-organisation/total-expenditure", 10),
]


validator_url = "http://stage.iativalidator.iatistandard.org/api/v1/stats?date=2020-12-31"
all_validation = json.loads(requests.get(validator_url).content)



if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description='Create publisher metadata')
    arg_parser.add_argument('publisher', type=str, help='Publisher\'s ID from the IATI Registry')
    args = arg_parser.parse_args()
    output_dir = os.path.join("output", args.publisher)
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    try:
        pub_validation = [val for val in all_validation if val['publisher'] == args.publisher]
        summary_validation = pd.io.json.json_normalize(pub_validation[0]["summaryStats"])
        summary_validation.to_csv(os.path.join(output_dir,"validation_summary.csv"), index=False)
        message_validation = pd.io.json.json_normalize(pub_validation[0]["messageStats"])
        message_validation.to_csv(os.path.join(output_dir,"validation_message.csv"), index=False)
        activity_validation = pd.io.json.json_normalize(pub_validation[0]["activityStats"])
        activity_validation.to_csv(os.path.join(output_dir,"validation_activity.csv"), index=False)
    except:
        pass
    xml_path = os.path.join("/home/alex/git/IATI-Registry-Refresher/data", args.publisher, '*')
    xml_files = glob.glob(xml_path)
    table_dict = dict()
    bar = progressbar.ProgressBar()
    for xml_file in bar(xml_files):
        for table_name, xpath, max_depth in table_descriptions:
            try:
                extract = melt_iati(xml_file, xpath, max_depth)
            except etree.XMLSyntaxError:
                continue
            if len(extract) > 0:
                extract["filename"] = os.path.basename(xml_file)
                if table_name in list(table_dict.keys()):
                    table_dict[table_name].append(extract)
                else:
                    table_dict[table_name] = [extract]
    for table_name in list(table_dict.keys()):
        total = pd.concat(table_dict[table_name], sort=False)
        cols = list(total)
        cols.insert(0, cols.pop(cols.index('filename')))
        total = total.loc[:, cols]
        table_path = os.path.join(output_dir, table_name)
        total.to_csv(table_path, index=False)
    for table_name in [td[0] for td in table_descriptions]:
        if table_name not in list(table_dict.keys()):
            table_path = os.path.join(output_dir, table_name)
            with open(table_path, "w") as table_file:
                table_file.write("\n")