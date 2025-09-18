import pandas as pd
import streamlit as st
import os

st.set_page_config(layout="wide")
path_to_tables = os.path.join(os.getcwd(),'Output')

# Load data from tables
actors = pd.read_csv(os.path.join(path_to_tables,'ActorsDim.csv'))
genres = pd.read_csv(os.path.join(path_to_tables,'GenresDim.csv'))
movie_actors = pd.read_csv(os.path.join(path_to_tables,'MovieActorsFact.csv'))
movie_genres = pd.read_csv(os.path.join(path_to_tables,'MovieGenresFact.csv'))
movies = pd.read_csv(os.path.join(path_to_tables,'MoviesDim.csv'))
movie_facts = pd.read_csv(os.path.join(path_to_tables,"MovieFact.csv"))
distributors = pd.read_csv(os.path.join(path_to_tables,'DistributorsDim.csv'))
revenue_dates =  pd.read_csv(os.path.join(path_to_tables,'RevenueDatesDim.csv'))

# Prepare  basic data
movie_data = movie_facts.merge(movies, left_on="MovieId", right_on="Id", how="left")
movie_data = movie_data.merge(distributors, left_on="DistributorId", right_on="Id", how="left", suffixes=("", "_Distributor"))
movie_data = movie_data.rename(columns={"Distributor": "DistributorName"})

movie_genres = movie_genres.merge(genres, left_on="GenreId", right_on="Id", how="left")
movie_genres_grouped = movie_genres.groupby("MovieId")["Genre"].apply(lambda x: ", ".join(x)).reset_index()
movie_data = movie_data.merge(movie_genres_grouped, on="MovieId", how="left")

movie_actors = movie_actors.merge(actors, left_on="ActorId", right_on="Id", how="left")
movie_actors["ActorFullName"] = movie_actors["Name"] + " " + movie_actors["Surname"]
movie_actors_grouped = movie_actors.groupby("MovieId")["ActorFullName"].apply(lambda x: ", ".join(x)).reset_index()
movie_data = movie_data.merge(movie_actors_grouped, on="MovieId", how="left")

movie_data = movie_data.merge(revenue_dates, left_on="DateId", right_on="Id", how="left")
movie_data = movie_data.rename(columns={"Date": "RevenueDate"})

#Initialize title
st.title("Movies ranking")

#Prepare values to select
selected_genre = st.selectbox("Choose Genre", ["All"] + sorted(genres["Genre"].unique().tolist()))
selected_actor = st.selectbox("Choose Actor", ["All"] + sorted(movie_actors["ActorFullName"].unique().tolist()))
selected_distributor = st.selectbox("Choose Distributor", ["All"] + sorted(distributors["Distributor"].unique().tolist()))
ranking_criterion = st.selectbox("Choose ranking criteria", ["ImdbRating", "Metascore", "Revenue"])
movie_data["RevenueYear"] = pd.DatetimeIndex(movie_data["RevenueDate"]).year

#Prepare data to show
available_dates = sorted(movie_data["RevenueYear"].dropna().unique())
selected_date = None
if ranking_criterion == "Revenue":
    selected_date = st.selectbox("Choose date criteria", ["All time"] + available_dates[::-1])
    if selected_date == "All time":
        selected_date = available_dates[-1]
    movie_data = movie_data[movie_data["RevenueYear"] == selected_date].groupby("MovieId").max()
else:
    movie_data = movie_data.drop_duplicates(subset=["MovieId"])

# Filter data depends on the selected value
filtered_data = movie_data.copy()
if selected_genre != "All":
    filtered_data = filtered_data[filtered_data["Genre"].str.contains(selected_genre, na=False)]
if selected_actor != "All":
    filtered_data = filtered_data[filtered_data["ActorFullName"].str.contains(selected_actor, na=False)]
if selected_distributor != "All":
    filtered_data = filtered_data[filtered_data["DistributorName"] == selected_distributor]

# Show ranking
st.subheader(f"Movies ranking by: {ranking_criterion}")
ranking = filtered_data.sort_values(by=ranking_criterion, ascending=False)[["Title", "ReleaseDate", "Genre", "ActorFullName", "DistributorName", ranking_criterion]]
ranking = ranking.reset_index(drop=True)
ranking.index += 1
ranking=ranking.rename(columns={"ReleaseDate":"Release Year","ActorFullName":"Main Actors","DistributorName":"Distributor Name"})
st.dataframe(ranking)


