# ICC Championship-Trophy 2025 All Data Collection

# Importing all essential Libraries
import numpy as np
import pandas as pd
import json
import http.client
import time

import warnings
warnings.simplefilter("ignore")

# Empty Dataframe to store each Data Table
match_details = pd.DataFrame()
match_incidents = pd.DataFrame()
match_graph = pd.DataFrame()
match_stats = pd.DataFrame()
battingTeam = pd.DataFrame()
bowlingTeam = pd.DataFrame()
battingLine = pd.DataFrame()
bowlingLine = pd.DataFrame()
partnerships = pd.DataFrame()
match_lineups = pd.DataFrame()
match_h2h_duel = pd.DataFrame()
match_odds = pd.DataFrame()
head_to_head_match = pd.DataFrame()

# **********************************************************************************************
match_ids = [
    13265827, 13265828, 13265829, 13265830, 13265831, 13265832, 13265833, 
    13265834, 13265836, 13265837, 13265838, 13265840, 13558575, 13558576, 13569002
]


# Develop a code to collect all match_ids from tournament 


# ***********************************************************************

# Function for Convert a nested json dictionary to flatten json or dictionary
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


# Function to convert  flatten_json to Row column 
def flatten_json_to_row_column(data):
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



# Main process code to convert json to row data
def process_json(data):
    all_dataframe = {}
    parent_json = {}

    for key in data:
        if data[key] is not None and data[key] != "" and data[key] != {}:
            if isinstance(data[key], list):  # If value is a list
                dataframe = pd.DataFrame(columns=flatten_json_to_row_column(flatten_json(data[key][0])).columns)
                for i in range(len(data[key])):
                    flatten_dict = flatten_json(data[key][i])
                    new_dataframe = flatten_json_to_row_column(flatten_dict)
                    dataframe = pd.concat([dataframe, new_dataframe], ignore_index=True)

                    # Deleting field translation columns
                    dataframe = dataframe.loc[:, ~dataframe.columns.str.contains("fieldtranslations|translation", case=False, regex=True)]

                if dataframe.shape[1] >= 20:  # If columns are 10 or more
                    all_dataframe[key] = dataframe
                else:
                    parent_json[key] = data[key]

            elif isinstance(data[key], dict):  # If value is a dictionary
                flatten_dict = flatten_json(data[key])
                dataframe = flatten_json_to_row_column(flatten_dict)

                # Deleting field translation columns
                dataframe = dataframe.loc[:, ~dataframe.columns.str.contains("fieldtranslations|translation", case=False, regex=True)]

                if dataframe.shape[1] >= 20:  # If columns are 5 or more
                    all_dataframe[key] = dataframe
                else:
                    parent_json[key] = data[key]
            else:
                parent_json[key] = data[key]

    # Convert parent_json into a DataFrame and store it in all_dataframe
    all_dataframe["parent_df"] = flatten_json_to_row_column(flatten_json(parent_json))

    return all_dataframe


# for match innings some special case handling 
def process_json_for_innings(data):
    all_dataframe = {}
    parent_json = {}

    for key in data:
        if data[key] is not None and data[key] != "" and data[key] != {}:
            if isinstance(data[key], list):  # If value is a list
                dataframe = pd.DataFrame(columns=flatten_json_to_row_column(flatten_json(data[key][0])).columns)
                for i in range(len(data[key])):
                    flatten_dict = flatten_json(data[key][i])
                    new_dataframe = flatten_json_to_row_column(flatten_dict)
                    dataframe = pd.concat([dataframe, new_dataframe], ignore_index=True)

                    # Deleting field translation columns
                    dataframe = dataframe.loc[:, ~dataframe.columns.str.contains("fieldtranslations|translation", case=False, regex=True)]

                if dataframe.shape[1] >= 10:  # If columns are 10 or more
                    all_dataframe[key] = dataframe
                else:
                    parent_json[key] = data[key]

            elif isinstance(data[key], dict):  # If value is a dictionary
                flatten_dict = flatten_json(data[key])
                dataframe = flatten_json_to_row_column(flatten_dict)

                # Deleting field translation columns
                dataframe = dataframe.loc[:, ~dataframe.columns.str.contains("fieldtranslations|translation", case=False, regex=True)]

                if dataframe.shape[1] >= 5:  # If columns are 5 or more
                    all_dataframe[key] = dataframe
                else:
                    parent_json[key] = data[key]
            else:
                parent_json[key] = data[key]

    # Convert parent_json into a DataFrame and store it in all_dataframe
    all_dataframe["match_stats"] = flatten_json_to_row_column(flatten_json(parent_json))

    return all_dataframe


# ******************************************************************************************
# API connection setup  
conn = http.client.HTTPSConnection("allsportsapi2.p.rapidapi.com")

headers = {
    'x-rapidapi-key': "693b14936cmshb4229d5afd9d447p186b67jsn499928bbef0a",
    'x-rapidapi-host': "allsportsapi2.p.rapidapi.com"
}

# ******************************************************************************************

# for each match id collecting data
for match_id in match_ids:
    
    ## ************************Data collection for match_details***************************
    try:
        # Make API request for match_details
        conn.request("GET", f"/api/cricket/match/{match_id}", headers=headers)
        res = conn.getresponse()
        data = res.read()
            
        # Convert response to JSON
        data_json = json.loads(data.decode("utf-8"))
        # converting data_json into row and columns dataframe
        match_details_data = process_json(data_json)['event']
        match_details = pd.concat([match_details, match_details_data], ignore_index=True)

    except Exception as e:
        print(f"Error fetching match_details for Match ID {match_id}: {e}")

    time.sleep(1)

    ## **************************Data collection for match_incidents*************************
    try:
        # Make API request for match_incidents
        conn.request("GET", f"/api/cricket/match/{match_id}/incidents", headers=headers)
        res = conn.getresponse()
        data = res.read()
            
        # Convert response to JSON
        data_json = json.loads(data.decode("utf-8"))
        # converting data_json into row and columns dataframe
        match_incidents_data = process_json(data_json)['incidents']
        # create a new column by match_id so that it can be unique identifier
        match_incidents_data['match_id'] = match_id
        match_incidents = pd.concat([match_incidents, match_incidents_data], ignore_index=True)

    except Exception as e:
        print(f"Error fetching match_incidents for Match ID {match_id}: {e}")

    time.sleep(1)

    ## **************************Data collection for match_graph***************************
    try:
        # Make API request for match_graph
        conn.request("GET", f"/api/cricket/match/{match_id}/graph", headers=headers)
        res = conn.getresponse()
        data = res.read()
            
        # Convert response to JSON
        data_json = json.loads(data.decode("utf-8"))
        # converting data_json into row and columns dataframe
        homeinnings = flatten_json_to_row_column(data_json['graphPoints']['homeInnings'][0]['runs'])
        homeinnings['inning'] = 'home'
        homeinnings['match_id'] = match_id
        
        # awayinnings data_json extractiong
        awayinnings = flatten_json_to_row_column(data_json['graphPoints']['awayInnings'][0]['runs'])
        awayinnings['inning'] = 'away'
        awayinnings['match_id'] = match_id 

        # concatenating home and away inings data
        match_graph_data = pd.concat([homeinnings, awayinnings],ignore_index=True)
        # storing in match_graph dataframe
        match_graph = pd.concat([match_graph, match_graph_data], ignore_index=False)

    except Exception as e:
        print(f"Error fetching match_graph for Match ID {match_id}: {e}")

    time.sleep(1)


    ## **************************Data collection for match_innings**************************
    try:
        # Make API request for match_innings
        conn.request("GET", f"/api/cricket/match/{match_id}/innings", headers=headers)
        res = conn.getresponse()
        data = res.read()
            
        # Convert response to JSON
        data_json = json.loads(data.decode("utf-8"))
        # converting data_json into row and columns dataframe

        # extracting innings1 and innings2 data separately from parent data
        innings1_dataframes = process_json_for_innings(data_json['innings'][0])
        innings2_dataframes = process_json_for_innings(data_json['innings'][1])

        # storing data properly
        # ************* For innings 1 *******
        # battingTeam
        innings1_battingTeam = innings1_dataframes['battingTeam']
        innings1_battingTeam['match_id'] = match_id
        innings1_battingTeam['innings'] = 1
        battingTeam = pd.concat([battingTeam, innings1_battingTeam], ignore_index=True)

        # bowlingTeam
        innings1_bowlingTeam = innings1_dataframes['bowlingTeam']
        innings1_bowlingTeam['match_id'] = match_id
        innings1_bowlingTeam['innings'] = 1
        bowlingTeam = pd.concat([bowlingTeam, innings1_bowlingTeam], ignore_index=True)

        # battingLine
        innings1_battingLine = innings1_dataframes['battingLine']
        innings1_battingLine['match_id'] = match_id
        innings1_battingLine['innings'] = 1
        battingLine = pd.concat([battingLine, innings1_battingLine], ignore_index=True)

        # bowlingLine
        innings1_bowlingLine = innings1_dataframes['bowlingLine']
        innings1_bowlingLine['match_id'] = match_id
        innings1_bowlingLine['innings'] = 1
        bowlingLine = pd.concat([bowlingLine, innings1_bowlingLine], ignore_index=True)

        # partnerships
        innings1_partnerships = innings1_dataframes['partnerships']
        innings1_partnerships['match_id'] = match_id
        innings1_partnerships['innings'] = 1
        partnerships = pd.concat([partnerships, innings1_partnerships], ignore_index=True)

        # match_stats
        innings1_match_stats = innings1_dataframes['match_stats']
        innings1_match_stats['match_id'] = match_id
        innings1_match_stats['innings'] = 1
        match_stats = pd.concat([match_stats, innings1_match_stats], ignore_index=True)


        # *********For innings 2 ******

        # battingTeam
        innings2_battingTeam = innings2_dataframes['battingTeam']
        innings2_battingTeam['match_id'] = match_id
        innings2_battingTeam['innings'] = 2
        battingTeam = pd.concat([battingTeam, innings2_battingTeam], ignore_index=True)

        # bowlingTeam
        innings2_bowlingTeam = innings2_dataframes['bowlingTeam']
        innings2_bowlingTeam['match_id'] = match_id
        innings2_bowlingTeam['innings'] = 2
        bowlingTeam = pd.concat([bowlingTeam, innings2_bowlingTeam], ignore_index=True)

        # battingLine
        innings2_battingLine = innings2_dataframes['battingLine']
        innings2_battingLine['match_id'] = match_id
        innings2_battingLine['innings'] = 2
        battingLine = pd.concat([battingLine, innings2_battingLine], ignore_index=True)


        # bowlingLine
        innings2_bowlingLine = innings2_dataframes['bowlingLine']
        innings2_bowlingLine['match_id'] = match_id
        innings2_bowlingLine['innings'] = 2
        bowlingLine = pd.concat([bowlingLine, innings2_bowlingLine], ignore_index=True)

        # partnerships
        innings2_partnerships = innings2_dataframes['partnerships']
        innings2_partnerships['match_id'] = match_id
        innings2_partnerships['innings'] = 2
        partnerships = pd.concat([partnerships, innings2_partnerships], ignore_index=True)

        # match_stats
        innings2_match_stats = innings2_dataframes['match_stats']
        innings2_match_stats['match_id'] = match_id
        innings2_match_stats['innings'] = 2
        match_stats = pd.concat([match_stats, innings2_match_stats], ignore_index=True)

    except Exception as e:
        print(f"Error fetching match_innings for Match ID {match_id}: {e}")

    time.sleep(1)
    
    # ********************* Data collection for Match Lineups ***********************
    try:
        # Make API request for match_lineups
        conn.request("GET", f"/api/cricket/match/{match_id}/lineups", headers=headers)
        res = conn.getresponse()
        data = res.read()
            
        # Convert response to JSON
        data_json = json.loads(data.decode("utf-8"))

        # Home lineups details
        home_lineups = process_json(data_json['home'])
        home_lineups['players']['match_id'] = match_id
        home_lineups['players']['innings'] = 'home'
        home_lineups['parent_df'] = home_lineups['parent_df'].loc[home_lineups['parent_df'].index.repeat(len(home_lineups['players']))].reset_index(drop=True) 
        home_lineups_details = pd.concat([home_lineups['players'], home_lineups['parent_df']], axis=1)

        # Away lineups details
        away_lineups = process_json(data_json['away'])
        away_lineups['players']['match_id'] = match_id
        away_lineups['players']['innings'] = 'away'
        away_lineups['parent_df'] = away_lineups['parent_df'].loc[away_lineups['parent_df'].index.repeat(len(away_lineups['players']))].reset_index(drop=True) 
        away_lineups_details = pd.concat([away_lineups['players'], away_lineups['parent_df']], axis=1)

        # Home and Away broth lineups details concatenating
        lineups_details = pd.concat([home_lineups_details, away_lineups_details], ignore_index=True)
        # inserting into match_lineups
        match_lineups = pd.concat([match_lineups, lineups_details], ignore_index=True)
    except Exception as e:
        print(f"Error fetching match_lineups for Match ID {match_id}: {e}")

    time.sleep(1)

    ## ***********************Data collection for match_odds**********************
    try:
        # Make API request for match_odds
        conn.request("GET", f"/api/cricket/match/{match_id}/odds", headers=headers)
        res = conn.getresponse()
        data = res.read()
            
        # Convert response to JSON
        data_json = json.loads(data.decode("utf-8"))
        # converting data_json into row and columns dataframe
        match_odds_details = process_json(data_json)['markets']
        match_odds_details['match_id'] = match_id
        match_odds = pd.concat([match_odds_details, match_odds], ignore_index=True)


    except Exception as e:
        print(f"Error fetching match_odds for Match ID {match_id}: {e}")

    time.sleep(1)

    ## *******************Data collection for match_duel*************************
    try:
        # Make API request for match_duel
        conn.request("GET", f"/api/cricket/match/{match_id}/duel", headers=headers)
        res = conn.getresponse()
        data = res.read()
            
        # Convert response to JSON
        data_json = json.loads(data.decode("utf-8"))
        # converting data_json into row and columns dataframe
        match_duel = flatten_json_to_row_column(flatten_json(data_json))
        match_duel['match_id'] = match_id
        match_h2h_duel = pd.concat([match_h2h_duel, match_duel], ignore_index=True)

    except Exception as e:
        print(f"Error fetching match_duel for Match ID {match_id}: {e}")

    time.sleep(1)

#  ********************************************************************************

# Save all collected data  to an Excel file
file_path = "ICC championship Trophy 2025.xlsx"
with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
        match_details.to_excel(writer, sheet_name='match_details', index=False)
        match_incidents.to_excel(writer,sheet_name='match_incidents', index=False)
        match_graph.to_excel(writer,sheet_name='match_graph', index=False)
        match_stats.to_excel(writer,sheet_name='match_stats', index=False)
        battingTeam.to_excel(writer,sheet_name='battingTeam', index=False)
        bowlingTeam.to_excel(writer,sheet_name='bowlingTeam', index=False)
        battingLine.to_excel(writer,sheet_name='battingLine', index=False)
        bowlingLine.to_excel(writer,sheet_name='bowlingLine', index=False)
        partnerships.to_excel(writer,sheet_name='partnerships', index=False)
        match_lineups.to_excel(writer,sheet_name='match_lineups', index=False)
        match_h2h_duel.to_excel(writer,sheet_name='match_h2h_duel', index=False)
        match_odds.to_excel(writer,sheet_name='match_odds', index=False)

print(f"Excel file '{file_path}' saved successfully!")