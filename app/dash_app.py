from dash import Dash, dcc, html, State, Input, Output
import pandas as pd
from db import Neo4jConnection
import requests
# from environs import Env


# env = Env()
# env.read_env()

# NEO4J_URI = env("NEO4J_URI")
# NEO4J_USER = env("NEO4J_USER")
# NEO4J_PASSWORD = env("NEO4J_PASSWORD")

# # Establish Neo4j connection
# conn = Neo4jConnection(uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)

# print('__')
# print(conn.verify_connectivity())
# print('__')




graphdb_url = 'http://localhost:7200'


# Send the query to GraphDB
response = requests.get(graphdb_url)
print(response)


app = Dash(__name__)



app.layout = html.Div([
    
    html.Div([
        html.H2("Find Your Next Movie 😊", className="sidebar-title"),

       
        html.Div([
            html.Label("Select Film Title:"),
            dcc.Dropdown(
                id="film-title",
                options=[
                    {"label": "Film A", "value": "Film A"},
                    {"label": "Film B", "value": "Film B"},
                    {"label": "Film C", "value": "Film C"},
                    {"label": "Film 5", "value": "Film 5"},
                    {"label": "Film 3", "value": "Film 3"},
                    {"label": "Film 228", "value": "Film 228"},
                    {"label": "Film XDD", "value": "Film XDD"},
                    {"label": "Film XD", "value": "Film XD"},
                    {"label": "Film 10", "value": "Film 10"},
                    {"label": "Film X", "value": "Film X"}
                ],
                value="Select a title",
                className="dropdown",
                style={}
            )
        ], className="input-group"),

        
        html.Div([
            html.Label("Rating Range:"),
            dcc.RangeSlider(
                id="rating-range",
                min=0,
                max=10,
                step=1,
                value=[0, 10],
                marks={i: str(i) for i in range(0, 11)}
            )
        ], className="input-group"),


        html.Div([
            html.Label("Select Director:"),
            dcc.Dropdown(
                id="director",
                options=[
                    {"label": "Director A", "value": "Director A"},
                    {"label": "Director B", "value": "Director B"},
                    {"label": "Director C", "value": "Director C"}
                ],
                value="Select a director",
                className="dropdown"
            )
        ], className="input-group"),

        # Country selection
        html.Div([
            html.Label("Select Country:"),
            dcc.Dropdown(
                id="country",
                options=[
                    {"label": "USA", "value": "USA"},
                    {"label": "UK", "value": "UK"},
                    {"label": "France", "value": "France"}
                ],
                value="Select a country",
                className="dropdown"
            )
        ], className="input-group"),


        html.Div([
            html.Label("Select Actors:"),
            dcc.Dropdown(
                id="actors",
                options=[
                    {"label": "Actor A", "value": "Actor A"},
                    {"label": "Actor B", "value": "Actor B"},
                    {"label": "Actor C", "value": "Actor C"},
                    {"label": "Actor D", "value": "Actor D"},
                    {"label": "Actor E", "value": "Actor E"}
                ],
                multi=True,
                value=[]
            )
        ], className="input-group"),


        html.Div([
            html.Label("Write a Short Description of the Plot:"),
            dcc.Textarea(
                id="plot-description",
                placeholder="Enter a brief description of the plot...",
                style={"width": "100%", "height": "100px"},
                value=""
            )
        ], className="input-group"),

        html.Button("Search", id="search-btn", className="button", n_clicks=0)
    ], className="sidebar"),

    html.Div([
        html.Div(id="output", className="results-container")
    ], className="content")
])



# @app.callback(
#     Output("search-btn", "disabled"),
#     [Input("film-title", "value"), Input("rating-range", "value")]
# )
# def enable_button(film_title, rating_range):
    
#     return film_title == "Select a title" or not rating_range


@app.callback(
    Output("output", "children"),
    Input("search-btn", "n_clicks"),
    State("film-title", "value"),
    State("rating-range", "value"),
    prevent_initial_call=True
)
def display_output(n_clicks, film_title, rating_range):
    output = f"""
    You selected:
    - Film Title: {film_title}
    - Rating Range: {rating_range}
    """
    return html.Pre(output)



if __name__ == '__main__':
    app.run_server(debug=True)


