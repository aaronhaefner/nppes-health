import json

schema = {
    "tables": {
        "nppes": {
            "columns": {
                "npi": "TEXT",
                "entity": "TEXT",
                "replacement_npi": "TEXT",
                "ein": "TEXT",
                "porgname": "TEXT",
                "pcredential": "TEXT",
                "plocstatename": "TEXT",
                "ploczip": "TEXT",
                "penumdate": "TEXT",
                "lastupdate": "TEXT",
                "npideactreason": "TEXT",
                "npideactdate": "TEXT",
                "npireactdate": "TEXT",
                "pgender": "TEXT",
                "ptaxcode": "TEXT"
            },
            "primary_key": "npi"
        },
        "taxonomy": {
            "columns": {
                "ptaxcode": "TEXT",
                "physician": "INTEGER",
                "student": "INTEGER",
                "np_type": "TEXT",
                "np": "INTEGER",
                "Type": "TEXT"
            },
            "primary_key": "ptaxcode"
        },
        "medicare": {
            "columns": {
                "npi": "TEXT",
                "mdcr_provider": "INTEGER"
            },
            "primary_key": "npi"
        }
    }
}

# Convert to JSON string
schema_json = json.dumps(schema, indent=4)

# Save to a file
with open("schema.json", "w") as f:
    f.write(schema_json)

print("Schema has been converted to JSON and saved to schema.json")
