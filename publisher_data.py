import argparse
import json
import glob
import os
import progressbar
import pandas as pd
import requests
from tabulize import melt_iati


table_descriptions = [
    ("activities.csv", "iati-activity"),
    ("transactions.csv", "iati-activity/transaction"),
    ("budgets.csv", "iati-activity/budget"),
    ("planned_disbursements.csv", "iati-activity/planned-disbursement"),
    ("participating_orgs.csv", "iati-activity/participating-org"),
    ("transact_provider_orgs.csv", "iati-activity/transaction/provider-org"),
    ("transact_receiver_orgs.csv", "iati-activity/transaction/receiver-org"),
    ("activity_recipient_countrys.csv", "iati-activity/recipient-country"),
    ("activity_recipient_regions.csv", "iati-activity/recipient-region"),
    ("transact_recipient_countrys.csv", "iati-activity/transaction/recipient-country"),
    ("transact_recipient_regions.csv", "iati-activity/transaction/recipient-region"),
    ("locations.csv", "iati-activity/location"),
    ("activity_sectors.csv", "iati-activity/sector"),
    ("transaction_sectors.csv", "iati-activity/transaction/sector"),
    ("tags.csv", "iati-activity/tag"),
    ("policy_markers.csv", "iati-activity/policy-marker"),
    ("humanitarian_scopes.csv", "iati-activity/humanitarian-scope"),
    ("organisations.csv", "iati-organisation"),
    ("organisation_total_budget.csv", "iati-organisation/total-budget"),
    ("organisation_total_expenditure.csv", "iati-organisation/total-expenditure"),
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
    pub_validation = [val for val in all_validation if val['publisher'] == args.publisher]
    summary_validation = pd.io.json.json_normalize(pub_validation[0]["summaryStats"])
    summary_validation.to_csv(os.path.join(output_dir,"validation_summary.csv"), index=False)
    message_validation = pd.io.json.json_normalize(pub_validation[0]["messageStats"])
    message_validation.to_csv(os.path.join(output_dir,"validation_message.csv"), index=False)
    activity_validation = pd.io.json.json_normalize(pub_validation[0]["activityStats"])
    activity_validation.to_csv(os.path.join(output_dir,"validation_activity.csv"), index=False)
    xml_path = os.path.join("/home/alex/git/IATI-Registry-Refresher/data", args.publisher, '*')
    xml_files = glob.glob(xml_path)
    table_dict = dict()
    bar = progressbar.ProgressBar()
    for xml_file in bar(xml_files):
        for table_name, xpath in table_descriptions:
            extract = melt_iati(xml_file, xpath)
            if len(extract) > 0:
                extract["filename"] = os.path.basename(xml_file)
                if table_name in list(table_dict.keys()):
                    table_dict[table_name].append(extract)
                else:
                    table_dict[table_name] = [extract]
    for table_name in list(table_dict.keys()):
        total = pd.concat(table_dict[table_name])
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