#HMIS Event Data Extractor
#This Script does the extraction of all Event Program Data
#You should have the Program ID in place for you to download this data.

# Import modules
import requests
import csv
import os
from getpass import getpass


# Input required fields

base_url = input("Enter the specific DHIS2 Instance (e.g., https://hmis.mohcc.org.zw/cbs): ").strip()
username = input("Enter your Username: ").strip()
password = getpass("Enter your Password (input hidden): ").strip()
program_id = input("Enter the Program ID: ").strip()
org_unit_id = input("Enter the Org Unit ID (leave blank for all org units): ").strip()

# Period of review

start_date = input("Enter start date (YYYY-MM-DD): ").strip()
end_date = input("Enter end date (YYYY-MM-DD): ").strip()

# Naming the output file

output_file = input("Enter output CSV file name (e.g., dhis2_events.csv): ").strip()
if not output_file:
    output_file = "dhis2_program_events_colab.csv"


# Fetching events from DHIS2

params = {
    "program": program_id,
    "fields": "event,orgUnit,programStage,trackedEntityInstance,eventDate,dataValues[dataElement,value]",
    "pageSize": 10000,
     "startDate": start_date,
    "endDate": end_date
}
if org_unit_id:
    params["orgUnit"] = org_unit_id

response = requests.get(f"{base_url}/api/events", params=params, auth=(username, password))
if response.status_code != 200:
    print(f"Error fetching events: {response.status_code} - {response.text}")
    raise SystemExit

events = response.json().get("events", [])


# Fetching the actual data element names

data_element_ids = set()
for event in events:
    for dv in event.get("dataValues", []):
        data_element_ids.add(dv["dataElement"])

data_element_map = {}
if data_element_ids:
    ids_string = ",".join(data_element_ids)
    de_response = requests.get(
        f"{base_url}/api/dataElements",
        params={"fields": "id,name", "filter": f"id:in:[{ids_string}]", "paging": "false"},
        auth=(username, password)
    )
    if de_response.status_code == 200:
        for de in de_response.json().get("dataElements", []):
            data_element_map[de["id"]] = de["name"]
    else:
        for de_id in data_element_ids:
            data_element_map[de_id] = de_id


# Mapping the event data to rows

flattened_rows = []
all_columns = set()

for event in events:
    row = {
        "event": event.get("event"),
        "orgUnit": event.get("orgUnit"),
        "programStage": event.get("programStage"),
        "trackedEntityInstance": event.get("trackedEntityInstance"),
        "eventDate": event.get("eventDate")
    }
    for dv in event.get("dataValues", []):
        col_name = data_element_map.get(dv["dataElement"], dv["dataElement"])
        row[col_name] = dv.get("value")
        all_columns.add(col_name)
    flattened_rows.append(row)


# Prepare CSV headers

headers = ["event", "orgUnit", "programStage", "trackedEntityInstance", "eventDate"] + sorted(all_columns)


# Write CSV

write_mode = "a" if os.path.exists(output_file) else "w"

with open(output_file, mode=write_mode, newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=headers)
    if write_mode == "w":
        writer.writeheader()
    for row in flattened_rows:
        writer.writerow(row)

print(f"Saved {len(flattened_rows)} events to {output_file}")


# Optional: download CSV

from google.colab import files
files.download(output_file)
