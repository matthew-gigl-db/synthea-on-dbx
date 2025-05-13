import os
import json
data_sources_dir = "../data_sources"
json_files = [f for f in os.listdir(data_sources_dir) if f.endswith('.json')]

table_definitions = []
for json_file in json_files:
    with open(os.path.join(data_sources_dir, json_file), 'r') as file:
        table_definitions.append(json.load(file))

from utilities.silver import Silver

for table_definition in table_definitions:
    SilverPipeline = Silver(
        spark = spark
        ,table_definition = table_definition
    )

    SilverPipeline.apply_changes()