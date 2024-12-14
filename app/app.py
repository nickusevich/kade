from dash import Dash, html, dcc, Input, Output
import pandas as pd
from environs import Env
from dash import Dash, dcc, html, State, Input, Output
from querying import get_all_genres, get_all_actors, get_all_directors


# Initializing the application
app = Dash(__name__)

# Initializing graphdb endpoint
graphdb_endpoint = "http://localhost:7200/repositories/MoviesRepo"



app.layout = html.Div([
    # Sidebar styling
    html.Div([
        html.H2("Find Your Next Movie 😊", className="sidebar-title"),

        # Film title selection
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

        # Rating Range slider
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

        # Director selection
        html.Div([
            html.Label("Select Director:"),
            dcc.Dropdown(
                id="director",
                options= get_all_directors(),
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

        # Actors selection
        html.Div([
            html.Label("Select Actors:"),
            dcc.Dropdown(
                id="actors",
                options= get_all_actors(),
                multi=True,
                value=[]
            )
        ], className="input-group"),

        # New Description field
        html.Div([
            html.Label("Write a Short Description of the Plot:"),
            dcc.Textarea(
                id="plot-description",
                placeholder="Enter a brief description of the plot...",
                style={"width": "100%", "height": "100px"},
                value=""
            )
        ], className="input-group"),

        # Search button
        html.Button("Search", id="search-btn", className="button", n_clicks=0)
    ], className="sidebar"),

    # Main content for output
    html.Div([
        html.Div(id="output", className="results-container")
    ], className="content")
])



@app.callback(
    Output("search-btn", "disabled"),
    [Input("film-title", "value"), Input("rating-range", "value")]
)
def enable_button(film_title, rating_range):
    # Enable the button only when all fields have valid values
    return film_title == "Select a title" or not rating_range


@app.callback(
    Output("output", "children"),
    Input("search-btn", "n_clicks"),
    State("film-title", "value"),
    State("rating-range", "value"),
    State("actors", "value"),  # Getting the selected actors
    prevent_initial_call=True
)
def display_output(n_clicks, film_title, rating_range, actors):
    output = f"""
    You selected:
    - Film Title: {film_title}
    - Rating Range: {rating_range}
    - Actors: {', '.join(actors)}  # Displaying the selected actors
    """
    return html.Pre(output)


if __name__ == '__main__':
    app.run_server(debug=True)


