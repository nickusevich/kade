import streamlit as st
import pandas as pd
import numpy as np
from db import Neo4jConnection



#________________________________________________________________________________
#POTENTIAL DATABASE CONNECTION

# @st.cache_resource
# def init_connection(URI, USER, PASSWORD):
#     return Neo4jConnection(URI, USER, PASSWORD)

# conn = init_connection(URI=URI, USER=USER, PASSWORD=PASSWORD)

# try:
#     status = conn.verify_connectivity()
#     st.sidebar.write(status)
# except Exception as e:
#     st.sidebar.error(f"Connection failed: {e} ")


#________________________________________________________________________________



st.sidebar.markdown(
    "<h1 style='text-align: center;'>Find your next favorite movie 😊</h1>",
    unsafe_allow_html=True
)


title = st.sidebar.selectbox('Select Film Title', ['Select a title', 'Film A', 'Film B', 'Film C'])

rating_range = st.sidebar.slider('Rating Range', 0, 10, (0, 10))

director = st.sidebar.selectbox('Select Director', ['Select a director', 'Director A', 'Director B', 'Director C'])

country = st.sidebar.selectbox('Select Country', ['Select a country', 'USA', 'UK', 'France'])

actors = st.sidebar.multiselect('Select Actors', ['Actor A', 'Actor B', 'Actor C', 'Actor D', 'Actor E'])


