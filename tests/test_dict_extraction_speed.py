# test fastest way to extract specific values from
# list of dictionaries
import json
# load data
filename = "/usr/local/applications/gro-hackathon/data/22aed06218e64bdd35d850097871917a009f2f21.json"

with open(filename, "r") as f:
    data = json.load(f)

required_data = []
required_keys = ["domain_desc", "commodity_desc", "statisticcat_desc", "agg_level_desc", "country_name", "state_name", "county_name", "unit_desc", "Value", "year"]

for ed in data:
    required_data.append({your_key.upper(): ed[your_key] for your_key in required_keys})

print required_data
