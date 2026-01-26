import pandas as pd
import os

RAW_PATH = "datasets/raw"
PROCESSED_PATH = "datasets/processed"

def ensure_processed_dir():
    """Create processed directory if it does not exist."""
    os.makedirs(PROCESSED_PATH, exist_ok=True)

def load_raw_data():
    """Load raw CSV files into DataFrames."""
    matches = pd.read_csv(f"{RAW_PATH}/match.csv")
    teams = pd.read_csv(f"{RAW_PATH}/team.csv")
    leagues = pd.read_csv(f"{RAW_PATH}/league.csv")
    countries = pd.read_csv(f"{RAW_PATH}/country.csv")

    return matches, teams, leagues, countries

def build_dim_team(teams: pd.DataFrame) -> pd.DataFrame:
    dim_team = teams[["id", "team_long_name"]].copy()

    dim_team.rename(
        columns = {
            "id": "team_id",
            "team_long_name": "team_name"
        },
        inplace=True
    )

    return dim_team

def build_dim_league(
    leagues: pd.DataFrame,
    countries: pd.DataFrame
) -> pd.DataFrame:

    leagues = leagues.merge(
        countries,
        left_on="country_id",
        right_on="id",
        how="left"
    )

    dim_league = leagues[[
        "id_x",
        "name_x",
        "name_y"
    ]].copy()

    dim_league.rename(
        columns={
            "id_x": "league_id",
            "name_x": "league_name",
            "name_y": "country_name"
        },
        inplace=True
    )

    return dim_league

def build_fact_matches(matches: pd.DataFrame) -> pd.DataFrame:
    fact_matches = matches[[
        "id",
        "date",
        "league_id",
        "home_team_api_id",
        "away_team_api_id",
        "home_team_goal",
        "away_team_goal"
    ]].copy()

    fact_matches.rename(
        columns={
            "id": "match_id",
            "home_team_api_id": "home_team_id",
            "away_team_api_id": "away_team_id",
            "home_team_goal": "home_goals",
            "away_team_goal": "away_goals"
        },
        inplace=True
    )

    fact_matches["date"] = pd.to_datetime(
        fact_matches["date"],
        errors="coerce"
    )

    return fact_matches

def save_processed_data(
        dim_team: pd.DataFrame,
        dim_league: pd.DataFrame,
        fact_matches: pd.DataFrame
):
    dim_team.to_csv(f"{PROCESSED_PATH}/dim_team.csv", index=False)
    dim_league.to_csv(f"{PROCESSED_PATH}/dim_league.csv", index=False)
    fact_matches.to_csv(f"{PROCESSED_PATH}/fact_matches.csv", index=False)

def main():

    ensure_processed_dir()

    matches, teams, leagues, countries = load_raw_data()

    dim_team = build_dim_team(teams)
    dim_league = build_dim_league(leagues, countries)
    fact_matches = build_fact_matches(matches)

    save_processed_data(dim_team, dim_league, fact_matches)

    print("Transformation completed successfully.")

if __name__ == "__main__":
    main()