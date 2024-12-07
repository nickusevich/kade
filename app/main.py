import streamlit as st
import pandas as pd
import numpy as np
from db import Neo4jConnection
from dotenv import load_dotenv
import os
from environs import Env

import streamlit as st
import pandas as pd
import numpy as np
from db import Neo4jConnection
from environs import Env
import requests

# #variables
# env = Env()
# env.read_env() 

# NEO4J_URI = env("NEO4J_URI")
# NEO4J_USER = env("NEO4J_USER")
# NEO4J_PASSWORD = env("NEO4J_PASSWORD")
# #________________________________________________________________________________


# conn = Neo4jConnection(uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)

# # Test the connection and display status
# try:
#     status = conn.verify_connectivity()
#     print(status)
# except Exception as e:
#     print(f"Error: {str(e)}")

# #________________________________________________________________________________

# # UI for now

response = requests.get("http://localhost:7200")


    


st.sidebar.markdown(
    "<h1 style='text-align: center;'>Find your next favorite movie 😊</h1>",
    unsafe_allow_html=True
)


title = st.sidebar.selectbox('Select Film Title', ['Select a title', 'Film A', 'Film B', 'Film C'])

rating_range = st.sidebar.slider('Rating Range', 0, 10, (0, 10))

director = st.sidebar.selectbox('Select Director', ['Select a director', 'Director A', 'Director B', 'Director C'])

country = st.sidebar.selectbox('Select Country', ['Select a country', 'USA', 'UK', 'France'])

actors = st.sidebar.multiselect('Select Actors', ['Actor A', 'Actor B', 'Actor C', 'Actor D', 'Actor E'])


#________________________________________________________________________________