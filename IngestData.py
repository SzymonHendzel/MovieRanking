import os
from pathlib import Path
from typing import Iterable, List
import pandas as pd
import requests

############################################Generic helper functions
def load_csv(file: Path, columns: List[str]) -> pd.DataFrame:
    if file.exists():
        return pd.read_csv(file)
    return pd.DataFrame(columns=columns)

def save_csv(df: pd.DataFrame, file: Path) -> None:
    file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(file, index=False)

def add_ids(existing: pd.DataFrame, to_add: pd.DataFrame, id_col: str = "Id") -> pd.DataFrame:
    if existing.empty or id_col not in existing.columns:
        start = 0
    else:
        max_id = pd.to_numeric(existing[id_col], errors="coerce").max()
        start = 0 if pd.isna(max_id) else int(max_id)
    to_add = to_add.copy()
    to_add[id_col] = range(start + 1, start + 1 + len(to_add))
    return to_add

def anti_join_left_only(left: pd.DataFrame, right: pd.DataFrame, on: List[str]) -> pd.DataFrame:
    m = left.merge(right[on].drop_duplicates(), on=on, how="left", indicator=True)
    return m.loc[m["_merge"] == "left_only", left.columns]

##############################################Extract functions
def read_input_files(path: Path) -> pd.DataFrame:
    files = list(path.glob("*.csv"))
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_csv(f) for f in files), ignore_index=True)

def get_additional_movie_info(titles: Iterable[str], api_key: str) -> pd.DataFrame:
    session = requests.Session()
    url = "http://www.omdbapi.com/"
    rows = []
    for t in pd.unique(pd.Series(titles).dropna().astype(str).str.strip()):
        if not t:
            continue
        try:
            r = session.get(url, params={"t": t, "apikey": api_key}, timeout=10)
            r.raise_for_status()
            data = r.json()
            if str(data.get("Response", "")).lower() == "true":
                rows.append(data)
        except requests.RequestException:
            pass
    if not rows:
        return pd.DataFrame()
    return pd.json_normalize(rows)

###############################################Transform functions
def prepare_actors_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.loc[:, ["Title", "Actors"]].dropna().copy()
    out["Actor"] = out["Actors"].str.split(",")
    out = out.explode("Actor")
    parts = out["Actor"].astype(str).str.strip().str.extract(r"^\s*(\S+)\s*(.*)$")
    out["Name"] = parts[0].fillna("")
    out["Surname"] = parts[1].fillna("")
    return out.loc[:, ["Title", "Name", "Surname"]].drop_duplicates()

def prepare_genres_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.loc[:, ["Title", "Genre"]].dropna().copy()
    out["Genre"] = out["Genre"].astype(str).str.split(",")
    out = out.explode("Genre")
    out["Genre"] = out["Genre"].str.strip()
    return out.loc[:, ["Title", "Genre"]].drop_duplicates()

def prepare_revenues_dataframe(df_movies: pd.DataFrame,df_revenues: pd.DataFrame) -> pd.DataFrame:
    out = pd.merge(df_movies,df_revenues, left_on='Title',right_on='title',how='inner')
    return out.loc[:, ["Title", "distributor","Metascore","imdbRating","revenue","date"]].drop_duplicates().rename(columns={"revenue":"Revenue","distributor":"Distributor","imdbRating":"ImdbRating","date":"Date"})

########################################################Load functions
def upsert_movies_dim(movies: pd.DataFrame, root: Path) -> pd.DataFrame:
    file = root / "MoviesDim.csv"
    cols = ["Id", "Title", "ReleaseDate"]
    existing = load_csv(file, cols)

    to_add = (
        movies.loc[:, ["Title", "Year"]]
        .rename(columns={"Year": "ReleaseDate"})
        .dropna(subset=["Title"])
        .drop_duplicates(subset=["Title"])
    )
    new_rows = anti_join_left_only(to_add, existing, on=["Title"])
    if not new_rows.empty:
        new_rows = add_ids(existing, new_rows, "Id")
        out = pd.concat([existing, new_rows], ignore_index=True)
        save_csv(out, file)
        return out
    return existing

def upsert_actors_dim(actors: pd.DataFrame, root: Path) -> pd.DataFrame:
    file = root / "ActorsDim.csv"
    cols = ["Id", "Name", "Surname"]
    existing = load_csv(file, cols)

    to_add = actors.loc[:, ["Name", "Surname"]].dropna().drop_duplicates()
    new_rows = anti_join_left_only(to_add, existing, on=["Name", "Surname"])
    if not new_rows.empty:
        new_rows = add_ids(existing, new_rows, "Id")
        out = pd.concat([existing, new_rows], ignore_index=True)
        save_csv(out, file)
        return out
    return existing

def upsert_genres_dim(genres: pd.DataFrame, root: Path) -> pd.DataFrame:
    file = root / "GenresDim.csv"
    cols = ["Id", "Genre"]
    existing = load_csv(file, cols)

    to_add = genres.loc[:, ["Genre"]].dropna().drop_duplicates()
    new_rows = anti_join_left_only(to_add, existing, on=["Genre"])
    if not new_rows.empty:
        new_rows = add_ids(existing, new_rows, "Id")
        out = pd.concat([existing, new_rows], ignore_index=True)
        save_csv(out, file)
        return out
    return existing


def upsert_distributor_dim(revenues: pd.DataFrame, root: Path) -> pd.DataFrame:
    file = root / "DistributorsDim.csv"
    cols = ["Id", "Distributor"]
    existing = load_csv(file, cols)
    to_add = revenues.loc[:, ["Distributor"]].dropna().drop_duplicates()
    new_rows = anti_join_left_only(to_add, existing, on=["Distributor"])
    if not new_rows.empty:
        new_rows = add_ids(existing, new_rows, "Id")
        out = pd.concat([existing, new_rows], ignore_index=True)
        save_csv(out, file)
        return out
    return existing


def upsert_revenuedate_dim(revenues: pd.DataFrame, root: Path) -> pd.DataFrame:
    file = root / "RevenueDatesDim.csv"
    cols = ["Id", "Date"]
    existing = load_csv(file, cols)

    to_add = revenues.loc[:, ["Date"]].dropna().drop_duplicates()
    new_rows = anti_join_left_only(to_add, existing, on=["Date"])
    if not new_rows.empty:
        new_rows = add_ids(existing, new_rows, "Id")
        out = pd.concat([existing, new_rows], ignore_index=True)
        save_csv(out, file)
        return out
    return existing


def upsert_movie_actors_fact(actors: pd.DataFrame, root: Path) -> pd.DataFrame:
    fact_file = root / "MovieActorsFact.csv"
    fact_cols = ["MovieId", "ActorId"]
    fact = load_csv(fact_file, fact_cols)

    movies_dim = pd.read_csv(root / "MoviesDim.csv")
    actors_dim = pd.read_csv(root / "ActorsDim.csv")

    merged = (
        actors
        .merge(movies_dim[["Id", "Title"]], on="Title", how="left", suffixes=("", "_Movie"))
        .merge(actors_dim[["Id", "Name", "Surname"]], on=["Name", "Surname"], how="left", suffixes=("", "_Actor"))
    )
    pairs = merged.loc[:, ["Id", "Id_Actor"]].rename(columns={"Id": "MovieId", "Id_Actor": "ActorId"}).dropna()
    pairs = pairs.astype({"MovieId": int, "ActorId": int}).drop_duplicates()

    new_rows = anti_join_left_only(pairs, fact, on=["MovieId", "ActorId"])
    if not new_rows.empty:
        out = pd.concat([fact, new_rows], ignore_index=True)
        save_csv(out, fact_file)
        return out
    return fact

def upsert_movie_genres_fact(genres: pd.DataFrame, root: Path) -> pd.DataFrame:
    fact_file = root / "MovieGenresFact.csv"
    fact_cols = ["MovieId", "GenreId"]
    fact = load_csv(fact_file, fact_cols)

    movies_dim = pd.read_csv(root / "MoviesDim.csv")
    genres_dim = pd.read_csv(root / "GenresDim.csv")

    merged = (
        genres
        .merge(movies_dim[["Id", "Title"]], on="Title", how="left")
        .merge(genres_dim[["Id", "Genre"]], on="Genre", how="left", suffixes=("", "_Genre"))
    )
    pairs = merged.loc[:, ["Id", "Id_Genre"]].rename(columns={"Id": "MovieId", "Id_Genre": "GenreId"}).dropna()
    pairs = pairs.astype({"MovieId": int, "GenreId": int}).drop_duplicates()

    new_rows = anti_join_left_only(pairs, fact, on=["MovieId", "GenreId"])
    if not new_rows.empty:
        out = pd.concat([fact, new_rows], ignore_index=True)
        save_csv(out, fact_file)
        return out
    return fact

def upsert_movie_fact(revenues: pd.DataFrame, root: Path) -> pd.DataFrame:
    fact_file = root / "MovieFact.csv"
    fact_cols = ["MovieId", "ImdbRating","Metascore","Revenue","DateId","DistributorId"]
    fact = load_csv(fact_file, fact_cols)

    revenuedates_dim = pd.read_csv(root / "RevenueDatesDim.csv")
    distributors_dim = pd.read_csv(root / "DistributorsDim.csv")
    movie_dim = pd.read_csv(root / "MoviesDim.csv")

    merged = (
        revenues
        .merge(movie_dim[["Id", "Title"]], on='Title', how="left")
        .merge(revenuedates_dim[["Id", "Date"]], on="Date", how="left", suffixes=("", "_daterevenue"))
        .merge(distributors_dim[["Id", "Distributor"]], on="Distributor", how="left", suffixes=("", "_distributor"))
    )

    pairs = merged.loc[:, ["Id","ImdbRating",'Metascore','Revenue',"Id_daterevenue", "Id_distributor"]].rename(columns={"Id":"MovieId","Id_daterevenue": "DateId", "Id_distributor": "DistributorId"}).dropna()

    new_rows = anti_join_left_only(pairs, fact, on=["MovieId", "DateId","DistributorId"])
    if not new_rows.empty:
        out = pd.concat([fact, new_rows], ignore_index=True)
        save_csv(out, fact_file)
        return out
    return fact


def main( input_csv_dir: str, output_dir: str,omdb_api_key: str = "964219c4"):
    input_path = Path(input_csv_dir)
    out_path = Path(output_dir)


    base_df = read_input_files(input_path)
    if base_df.empty:
        print("Missing files")
        return

    titles = base_df["title"].dropna().unique().tolist()[:500] #limitation due to the free API keys limitation per day

    #prepare movie dataframe
    movies_df = get_additional_movie_info(titles, omdb_api_key)
    if movies_df.empty:
        print("Connection failed")
        return

    #prepare actor and genres dataframe
    actors_df = prepare_actors_dataframe(movies_df)
    genres_df = prepare_genres_dataframe(movies_df)
    revenue_df = prepare_revenues_dataframe(movies_df,base_df)

    #upsert dim tables
    movies_dim = upsert_movies_dim(movies_df, out_path)
    actors_dim = upsert_actors_dim(actors_df, out_path)
    genres_dim = upsert_genres_dim(genres_df, out_path)
    distributors_dim = upsert_distributor_dim(revenue_df, out_path)
    revenueDate_dim = upsert_revenuedate_dim(revenue_df, out_path)

    #upsert fact tables
    _ = upsert_movie_actors_fact(actors_df, out_path)
    _ = upsert_movie_genres_fact(genres_df, out_path)
    _ = upsert_movie_fact(revenue_df, out_path)



if __name__ == "__main__":
    main(os.path.join(os.getcwd(),'Input'),os.path.join(os.getcwd(),'Output'))
