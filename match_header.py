import numpy as np
import pandas as pd
import requests


match_id_list = [117404, 126669, 108793]   # add more match IDs here

# Your API details
headers = {
    "x-rapidapi-key": "d1f52dc134msh606acc9612e6217p15c440jsnb9910abbb802",
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

def flatten_column(df, colname, prefix):
    if colname in df.columns:
        nested = pd.json_normalize(df[colname].iloc[0])
        nested = nested.add_prefix(prefix)
        df = df.drop(columns=[colname]).join(nested)
    return df


final_df_list = []

for match_id in match_id_list:
    try:
        url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{match_id}"
        response = requests.get(url, headers=headers)
        data = response.json()

        df = pd.DataFrame([data])

        # Flatten nested JSON fields
        nested_fields = {
            "team1": "team1_",
            "team2": "team2_",
            "umpire1": "umpire1_",
            "umpire2": "umpire2_",
            "umpire3": "umpire3_",
            "referee": "referee_",
            "venueinfo": "venueinfo_",
        }

        for col, prefix in nested_fields.items():
            df = flatten_column(df, col, prefix)

        # Drop columns you don't need
        df = df.drop(columns=["appindex", "boundarytrackervalues"], errors='ignore')

        final_df_list.append(df)

    except Exception as e:
        print(f"Error processing match ID {match_id}: {e}")



final_df = pd.concat(final_df_list, ignore_index=True)

print(final_df.head())
print(final_df.columns)
