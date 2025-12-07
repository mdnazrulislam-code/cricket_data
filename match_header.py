import pandas as pd
import requests

match_id_list = [117404, 126669, 108793]   # add more match IDs here

headers = {
    "x-rapidapi-key": "d1f52dc134msh606acc9612e6217p15c440jsnb9910abbb802",
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

def flatten_column(df, colname, prefix):
    if colname in df.columns and pd.notna(df[colname].iloc[0]):
        nested = pd.json_normalize(df[colname].iloc[0])
        nested = nested.add_prefix(prefix)
        df = df.drop(columns=[colname]).join(nested)
    return df

# store ALL players from ALL matches + BOTH teams
rows_by_category = {
    "playing_xi": [],
    "bench": [],
    "support_staff": []
}

def fetch_team_squad(match_id, team_id, series_id):
    squad_url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{match_id}/team/{team_id}"
    response = requests.get(squad_url, headers=headers)
    team_data = response.json()

    for key, value in team_data.items():
        if not isinstance(value, list):
            continue

        for item in value:
            category_raw = item.get("category", "")
            cat_key = category_raw.lower().replace(" ", "_")  # "Playing XI" -> "playing_xi"

            if cat_key not in rows_by_category:
                rows_by_category[cat_key] = []

            for p in item.get("player", []):
                row = dict(p)
                row["category"] = category_raw
                row["match_id"] = match_id
                row["series_id"] = series_id
                row["team_id"] = team_id
                rows_by_category[cat_key].append(row)


match_header_dfs = []

for match_id in match_id_list:
    match_header_url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{match_id}"
    response = requests.get(match_header_url, headers=headers)
    data = response.json()

    df = pd.DataFrame([data])

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

    df = df.drop(columns=["appindex", "boundarytrackervalues"], errors='ignore')
    df["match_id"] = match_id
    match_header_dfs.append(df)

    # get scalar IDs
    row = df.iloc[0]
    team1_id = row["team1_teamid"]
    team2_id = row["team2_teamid"]
    series_id = row["seriesid"]

    # fetch squads for BOTH teams → all go into rows_by_category
    for tid in [team1_id, team2_id]:
        fetch_team_squad(match_id, tid, series_id)

# build final category-level DataFrames (both teams combined)
dfs = {}
for cat, rows in rows_by_category.items():
    if rows:
        dfs[cat] = pd.DataFrame(rows)

# convenient variables
df_playing_xi = dfs.get("playing_xi", pd.DataFrame())
df_bench = dfs.get("bench", pd.DataFrame())
df_support_staff = dfs.get("support_staff", pd.DataFrame())


# print(df_playing_xi)
# print("---------------------")
# print(df_bench)
# print("---------------------")
# print(df_support_staff)

match_header_df = pd.concat(match_header_dfs, ignore_index=True)
match_header_df.to_csv("match_header.csv", index=False)

# Export players by category
df_playing_xi.to_csv("playing_xi.csv", index=False)
df_bench.to_csv("bench.csv", index=False)
df_support_staff.to_csv("support_staff.csv", index=False)

