import numpy as np
import pandas as pd
import json
import http.client
import time  

# List of match IDs
match_ids = [
    13265827, 13265828, 13265829, 13265830, 13265831, 13265832, 13265833, 
    13265834, 13265836, 13265837, 13265838, 13265840, 13558575, 13558576, 13569002
]

# API connection setup
conn = http.client.HTTPSConnection("allsportsapi2.p.rapidapi.com")

headers = {
    'x-rapidapi-key': "693b14936cmshb4229d5afd9d447p186b67jsn499928bbef0a",
    'x-rapidapi-host': "allsportsapi2.p.rapidapi.com"
}

# Parent dictionary to store all match data
parent_json = {}

# Loop through each match ID
for match_id in match_ids:
    try:
        # Make API request
        conn.request("GET", f"/api/cricket/match/{match_id}", headers=headers)
        res = conn.getresponse()
        data = res.read()
        
        # Convert response to JSON
        data_json = json.loads(data.decode("utf-8"))
        
        # Store JSON in the parent dictionary with match_id as key
        parent_json[f"match_details_{match_id}"] = data_json

        print(f"Data collected for Match ID {match_id}")

    except Exception as e:
        print(f"Error fetching data for Match ID {match_id}: {e}")

    time.sleep(1)  # Optional: Delay to prevent rate limits

# Save all match data in a single JSON file
with open("all_match_data.json", "w", encoding="utf-8") as f:
    json.dump(parent_json, f, indent=4)

print("All match data saved in all_match_data.json")


# ******************************************************************************
# Loading json file
with open("all_match_data.json", "r", encoding="utf-8") as f:
    data = json.load(f)
data


# **********************************************************************************
# Function for Convert a nested json dictionary to flatten json or dictionary (child function 1)
def flatten_json(nested_json, parent_key='', sep='_'):

    items = []
    
    # If the value is a dictionary
    if isinstance(nested_json, dict):
        for key, value in nested_json.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            items.extend(flatten_json(value, new_key, sep=sep).items())
    
    # If the value is a list
    elif isinstance(nested_json, list): 
        for index, item in enumerate(nested_json):
            new_key = f"{parent_key}{sep}{index}" if parent_key else str(index)
            items.extend(flatten_json(item, new_key, sep=sep).items())
            
    # If the value is neither dict nor list
    else:
        items.append((parent_key, nested_json))

    return dict(items)


# **********************************************************************************
# Flatten json to pandas dataframe. (child function 2)

def process_json_file(data):
    try:
        # Normalize JSON data
        if isinstance(data, list):  # JSON is an array of objects
            flat_data = [flatten_json(item) for item in data]
        elif isinstance(data, dict):  # JSON is a single dictionary
            flat_data = [flatten_json(data)]
        else:
            raise ValueError("Unsupported JSON format")

        # Convert to DataFrame
        df = pd.DataFrame(flat_data)
        return df


    except json.JSONDecodeError:
        print("Error: Invalid JSON file.")
    except Exception as e:
        print(f"Error: {str(e)}")


# ***********************************************************************************
# Main Function to feed the row json
def process_dictionary(data):
    all_dataframe = {}
    parent_json = {}

    for key in data:
        if data[key] is not None and data[key] != "" and data[key] != {}:
            if isinstance(data[key], list):  # If value is a list
                dataframe = pd.DataFrame(columns=process_json_file(flatten_json(data[key][0])).columns)
                for i in range(len(data[key])):
                    flatten_dict = flatten_json(data[key][i])
                    new_dataframe = process_json_file(flatten_dict)
                    dataframe = pd.concat([dataframe, new_dataframe], ignore_index=True)

                    # Deleting field translation columns
                    dataframe = dataframe.loc[:, ~dataframe.columns.str.contains("fieldtranslations|translation", case=False, regex=True)]

                if dataframe.shape[1] >= 10:  # If columns are 10 or more
                    all_dataframe[key] = dataframe
                else:
                    parent_json[key] = data[key]

            elif isinstance(data[key], dict):  # If value is a dictionary
                flatten_dict = flatten_json(data[key])
                dataframe = process_json_file(flatten_dict)

                # Deleting field translation columns
                dataframe = dataframe.loc[:, ~dataframe.columns.str.contains("fieldtranslations|translation", case=False, regex=True)]

                if dataframe.shape[1] >= 10:  # If columns are 5 or more
                    all_dataframe[key] = dataframe
                else:
                    parent_json[key] = data[key]
            else:
                parent_json[key] = data[key]

    # Convert parent_json into a DataFrame and store it in all_dataframe
    all_dataframe["parent_df"] = process_json_file(flatten_json(parent_json))

    return all_dataframe


# Creating a empty Dataframe by taking one match all columns
champ_trophy_match_details = pd.DataFrame(columns=process_dictionary(data[list(data.keys())[0]])['event'].columns)
champ_trophy_match_details

# concatenating all match data
for i in data.keys():
    match_data = process_dictionary(data[i])['event']
    champ_trophy_match_details = pd.concat([champ_trophy_match_details, match_data])


# Saving the data in excel file
champ_trophy_match_details.to_csv('champ_trophy_match_details.csv', index=False)