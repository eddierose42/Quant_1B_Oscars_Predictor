import pandas as pd
import numpy as np
import warnings

pd.set_option('mode.copy_on_write', True)
year_from = pd.to_datetime("2000")

def clean_oscar_data(full_oscars):
    def update_years(value):
        if "/" in value:
            return None
        else:
            return pd.to_datetime(value)

    full_oscars["Year"] = full_oscars["Year"].apply(update_years)
    oscars = full_oscars.drop(full_oscars[full_oscars["Year"].isna()].index)
    
    oscar_films = oscars[(oscars["Category"] == "BEST PICTURE") & (oscars["Year"] >= year_from)][["Year", "Film", "Winner"]]
    oscar_actresses = oscars[(oscars["Category"] == "ACTRESS IN A LEADING ROLE") & (oscars["Year"] >= year_from)][["Year", "Name", "Film", "Winner"]]
    oscar_actors = oscars[(oscars["Category"] == "ACTOR IN A LEADING ROLE") & (oscars["Year"] >= year_from)][["Year", "Name", "Film", "Winner"]]
    
    oscar_films["Name"] = oscar_films["Film"]
    oscar_films["Type"] = "film"
    oscar_actresses["Type"] = "actress"
    oscar_actors["Type"] = "actor"
    
    oscar_dfs = [oscar_films, oscar_actresses, oscar_actors]
    all_oscars = pd.concat(oscar_dfs)
    all_oscars.rename(columns={"Winner": "Oscar-win"}, inplace=True)
    all_oscars["Name"].str.strip()
    all_oscars.loc[all_oscars["Oscar-win"] == True, "Oscar-win"] = 1

    return all_oscars

def clean_wikipedia_data(path, award_ceremonies, award_categories, unwanted_columns):
    list_df = pd.Series()
    
    for award_type in award_categories:
        for award_name in award_ceremonies:
            df = pd.read_csv("{}/{}-{}.csv".format(path, award_name.lower(), award_type))
            if df["Year"].dtype == np.int64:
                df["Year"] = pd.to_datetime(df["Year"], format="%Y")
            else:
                df["Year"] = pd.to_datetime(df["Year"].str[:4])
                        
            df.rename(columns={award_type.capitalize(): "Name"}, inplace=True)
            if award_type == "film":
                df["Film"] = df["Name"]
    
            df["Type"] = award_type
            df["Name"].str.strip()
            df["{}-nom".format(award_name)] = 1.0
            df["{}-win".format(award_name)] = np.nan
    
            for i in df.columns:
                if i in unwanted_columns:
                    df.drop(i, axis=1, inplace=True)
    
            for year in df["Year"].value_counts().index:
                row_idx = df[df["Year"] == year].index[0]
                df.loc[row_idx, "{}-win".format(award_name)] = 1
    
            list_df.loc["{}/{}".format(award_name, award_type)] = df

    return list_df

def merge_data(list_df, all_oscars):
    def merge_series(list_df):
        list_df = list_df.to_list()
        df = list_df[0]    
        for i in range(1, len(list_df)):
            df = pd.merge(df, list_df[i], how="outer")
        return df

    films_merged = merge_series(list_df[:4])
    actresses_merged = merge_series(list_df[4:-4])
    actors_merged = merge_series(list_df[-4:])
    all_categories = pd.concat([films_merged, actresses_merged, actors_merged])
    full_df = pd.merge(all_oscars, all_categories, how="left")
    
    return full_df

def load_all_data(path="data", show_films=False):
    oscars_raw = pd.read_csv("{}/oscars.csv".format(path), sep="\t")
    oscars_clean = clean_oscar_data(oscars_raw)
    
    ceremonies = ["Bafta", "Sag", "Gg-dram", "Gg-com"]
    categories = ["film", "actress", "actor"]
    unwanted_columns = ["Director(s)", "Producer(s)", "Country", "Cast members", "Role(s)", "Ref.", "Character", "Director", "Producers", "Producer"]
    wiki_dfs = clean_wikipedia_data(path, ceremonies, categories, unwanted_columns)

    full_df = merge_data(wiki_dfs, oscars_clean)
    full_df.fillna(0, inplace=True)
    column_to_move = full_df.pop("Oscar-win")
    full_df.insert(4, "Oscar-win", column_to_move)
    
    if not show_films:
        full_df.drop("Film", axis=1, inplace=True)
    
    return full_df