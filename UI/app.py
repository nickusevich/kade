from dash import Dash, html, dcc, Input, Output, State, callback
import requests
import dash
from dash.exceptions import PreventUpdate
from urllib.parse import urlencode, parse_qs
import os
# from RestService import MovieDatabase
import json
import asyncio
# from RestService import db_crud

# Initialize the app
app = Dash(__name__, suppress_callback_exceptions=True)

# movie_db = MovieDatabase()

# Function to fetch dropdown options from REST API
def get_options_from_api(endpoint):
    response = requests.get(endpoint)
    if response.status_code == 200:
        data = response.json()
        return [{"label": item["label"], "value": item["label"]} for item in data]
    else:
        return []

def is_running_in_docker():
    """Check if the code is running inside a Docker container."""
    path = '/proc/1/cgroup'
    if os.path.exists(path):
        with open(path, 'r') as f:
            return 'docker' in f.read()
    return False

REST_SERVICE_URI = "http://localhost:80"
if is_running_in_docker():
    REST_SERVICE_URI = "http://host.docker.internal:80"

# Fetch initial options
movies_options = get_options_from_api(f'{REST_SERVICE_URI}/movies')
director_options = get_options_from_api(f'{REST_SERVICE_URI}/directors')
actor_options = get_options_from_api(f'{REST_SERVICE_URI}/actors')
genre_options = get_options_from_api(f'{REST_SERVICE_URI}/genres')

# Define the layout for the home page
home_layout = html.Div([
    html.Div([
        html.H1("Movie Finder 🎥", className="main-title"),
        html.Div([
            html.Label("Select Film Title:", className="label"),
            dcc.Dropdown(
                id="film-title",
                options=movies_options,
                multi=True,
                placeholder="Start typing to search for a title",
                className="dropdown"
            ),
            html.Label("Select Genres:", className="label"),
            dcc.Dropdown(
                id="genres",
                options=genre_options,
                multi=True,
                placeholder="Select genres",
                className="dropdown"
            ),
            html.Label("Year Range:", className="label"),
            dcc.RangeSlider(
                id="year-range",
                min=1940,
                max=2024,
                step=1,
                value=[1940, 2024],
                marks={i: str(i) for i in range(1940, 2024, 3)}
            ),
            html.Label("Max Movies to Show:", className="label"),
            dcc.Input(
                id="number_of_results",
                type="number",
                min=1,
                max=50,
                step=1,
                value=10,
                className="input"
            ),
            html.Label("Select Director:", className="label"),
            dcc.Dropdown(
                id="director",
                options=director_options,
                placeholder="Select a director",
                className="dropdown"
            ),
            html.Label("Select Actors:", className="label"),
            dcc.Dropdown(
                id="actors",
                options=actor_options,
                multi=True,
                placeholder="Select actors",
                className="dropdown"
            ),
            html.Label("Short Description of the Plot:", className="label"),
            dcc.Textarea(
                id="plot-description",
                placeholder="Enter a brief description of the plot...",
                style={"width": "100%", "height": "100px", "border-radius": "12px"},
                value=""
            ),
            html.Button("Search", id="search-btn", className="button", n_clicks=0)
        ], className="form-container")
    ], className="filters-container"),  # Apply the filters container class here

    html.Div(id="results-display", className="results-container")
], className="main-container")

# Layout for the results page
results_layout = html.Div([
    html.H1("Results Page", className="main-title"),
    html.Div(id="results-display"),
    dcc.Link('Back to Search', href='/', className="back-link")
])

# Define the layout for the app (main entry point)
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),  # it is a component that allows you to manage the URL state in your Dash app.
    # this is important for handling navigation between pages. the pathname of this component changes based on the URL in the browser.
    dcc.Store(id='search-store', storage_type='memory'),  # it is a component used to store the search parameters in memory. This is where you will save the user's search parameters, which can then be accessed on the results page.
    html.Div(id='page-content')
])

# Callback to update the page content based on the URL
@app.callback(
    Output('page-content', 'children'),  # output is the page content itself
    [Input('url', 'pathname')]  # Input as url, so below we have function, and based on URL it returns the layout
)
def display_page(pathname):
    if pathname == '/results':
        return results_layout
    return home_layout

# Callback for the search button to store parameters and redirect
@app.callback(
    [Output('results-display', 'children'),
     Output('url', 'pathname')],
    [Input('search-btn', 'n_clicks')],
    [State('film-title', 'value'),
     State('year-range', 'value'),
     State('genres', 'value'),
     State('number_of_results', 'value'),
     State('actors', 'value'),
     State('director', 'value'),
     State('plot-description', 'value')],
    prevent_initial_call=True
)
def store_search_parameters(n_clicks, film_title, year,
                            genres, number_of_results, actors, director, plot_description):

    if n_clicks == 0:
        raise PreventUpdate

    # Check if any of the search parameters are filled in
    if not any([film_title, year, genres, number_of_results, actors, director, plot_description]):
        raise PreventUpdate  # Prevent the page from redirecting if no parameters are selected

    params = {
        'title': film_title,
        'year_range': year,
        'genres': genres,
        'number_of_results': number_of_results,
        'actors': actors,
        'director': director,
        # 'plot_description': plot_description
    }
    movies = requests.get(f'{REST_SERVICE_URI}/movies_details', params=params).json()
    if not movies:
        return html.Div("no similar movies are found.")

    movie_results = [
        html.Div([
            html.H3(f"Title: {movie.get('title', 'N/A')}"),
            html.P(f"Abstract: {movie.get('abstract', 'N/A')}"),
            html.P(f"Runtime: {movie.get('runtime', 'N/A')}"),
            html.P(f"Budget: {movie.get('budget', 'N/A')}"),
            html.P(f"Box Office: {movie.get('boxOffice', 'N/A')}"),
            html.P(f"Release Year: {movie.get('releaseYear', 'N/A')}"),
            html.P(f"Country: {movie.get('country', 'N/A')}"),
            html.P(f"Genres: {movie.get('genres', 'N/A')}"),
            html.P(f"Starring: {movie.get('starring', 'N/A')}"),
            html.P(f"Directors: {movie.get('directors', 'N/A')}"),
            html.P(f"Producers: {movie.get('producers', 'N/A')}"),
            html.P(f"Writers: {movie.get('writers', 'N/A')}"),
            html.P(f"Composers: {movie.get('composers', 'N/A')}"),
            html.P(f"Cinematographers: {movie.get('cinematographers', 'N/A')}"),
            html.P(f"Similarity Score: {movie.get('similarity_score', 'N/A')}")
        ], className="movie-card")
        for movie in movies
    ]
    return html.Div(movie_results, className="movie-results"), '/results'

# Callback to display search results
@app.callback(
    Output('results-display', 'children'),
    [Input('search-store', 'data')]
)
def update_results(stored_data):
    if not stored_data:
        raise PreventUpdate

    # Make the GET request to the FastAPI service
    url = REST_SERVICE_URI + "/movies"
    response = requests.get(url, params=stored_data)

    # Check if the request was successful
    extracted_movies = []
    if response.status_code == 200:
        # Print the response JSON
        extracted_movies = response.json()
    else:
        # Print the error message
        print(f"Failed to get movies: {response.status_code}, {response.text}")

    # JSON
    formatted_json = json.dumps(extracted_movies, indent=4)

    # Showing results in JSON (extracted films)
    return html.Div([
        html.H2("Extracted JSON"),
        html.Pre(formatted_json, className="json-output", style={
            "backgroundColor": "#000000",
            "padding": "10px",
            "borderRadius": "5px",
            "whiteSpace": "pre-wrap",
            "wordBreak": "break-word",
        })
    ])

@callback(
    Output("film-title", "options"),
    Input("film-title", "search_value"),
    State("film-title", "value")
)
def update_multi_options_film_title(search_value, value):
    if not search_value:
        return get_options_from_api(f'{REST_SERVICE_URI}/movies')
    return get_options_from_api(f'{REST_SERVICE_URI}/movies?title={search_value}')

@callback(
    Output("genres", "options"),
    Input("genres", "search_value"),
    State("genres", "value")
)
def update_multi_options_genres(search_value, value):
    if not search_value:
        return get_options_from_api(f'{REST_SERVICE_URI}/genres')
    return get_options_from_api(f'{REST_SERVICE_URI}/genres?genreName={search_value}')

@callback(
    Output("director", "options"),
    Input("director", "search_value"),
    State("director", "value")
)
def update_multi_options_directors(search_value, value):
    if not search_value:
        return get_options_from_api(f'{REST_SERVICE_URI}/directors')
    return get_options_from_api(f'{REST_SERVICE_URI}/directors?directorName={search_value}')

@callback(
    Output("actors", "options"),
    Input("actors", "search_value"),
    State("actors", "value")
)
def update_multi_options_actors(search_value, value):
    if not search_value:
        return get_options_from_api(f'{REST_SERVICE_URI}/actors')
    return get_options_from_api(f'{REST_SERVICE_URI}/actors?actorName={search_value}')

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0')
    # app.run_server(debug=True)