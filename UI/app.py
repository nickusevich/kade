from dash import Dash, html, dcc, Input, Output, State, callback, ctx
import requests
import dash
from dash.exceptions import PreventUpdate
from urllib.parse import urlencode, parse_qs, quote
import os

# Initialize the app
app = Dash(__name__, suppress_callback_exceptions=True)

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
app.layout = html.Div([
    html.Div([
        html.H1("Movie Finder 🎥", className="main-title"),
        html.Div([
            html.Label("Select Film Title:", className="label"),
            dcc.Dropdown(
                id="film-title",
                options=movies_options,
                multi=False,
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
            html.Div([
                dcc.Checklist(
                    id="enable-year-range",
                    options=[{'label': 'Enable Year Range', 'value': 'enabled'}],
                    value=['disabled'],
                    className="checkbox",
                    style={"margin-right": "10px"}
                ),
                html.Label("Year Range:", className="label", style={"margin-right": "10px"}),
                dcc.RangeSlider(
                    id="year-range",
                    min=1924,
                    max=2024,
                    step=1,
                    value=[1924, 2024],
                    marks={i: str(i) for i in range(1924, 2025, 10)},
                    className="range-slider"
                ),
            ], className="year-range-container", style={"display": "flex", "alignItems": "center"}),
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
                style={"width": "100%", "height": "50px", "border-radius": "12px"},
                value=""
            ),
            html.Label("Max Movies to Show:", className="label"),
            dcc.Input(
                id="number_of_results",
                type="number",
                min=1,
                max=100,
                step=1,
                value=10,
                className="input"
            ),
            html.Button("Search", id="search-btn", className="button", n_clicks=0)
        ], className="form-container")
    ], className="filters-container"),  # Apply the filters container class here

    html.Div([
        dcc.Loading(
            id="loading-results",
            type="default",
            children=html.Div(id="results-display", className="results-container")
        )
    ], className="results-wrapper")
], className="main-container")

# Callback to enable/disable the RangeSlider
@app.callback(
    Output('year-range', 'disabled'),
    [Input('enable-year-range', 'value')]
)
def toggle_year_range(enable_year_range):
    return 'enabled' not in enable_year_range

# Combined callback for storing search parameters and displaying results
@app.callback(
    Output('results-display', 'children'),
    [Input('search-btn', 'n_clicks')],
    [State('film-title', 'value'),
     State('enable-year-range', 'value'),
     State('year-range', 'value'),
     State('genres', 'value'),
     State('number_of_results', 'value'),
     State('actors', 'value'),
     State('director', 'value'),
     State('plot-description', 'value')],
    prevent_initial_call=True
)
def handle_search_and_display(n_clicks, film_title, enable_year_range, year,
                              genres, number_of_results, actors, director, plot_description):
    if n_clicks == 0:
        raise PreventUpdate

    # Check if any of the search parameters are filled in
    if not any([film_title, genres, number_of_results, actors, director, plot_description]) and not ('enabled' in enable_year_range and year):
        raise PreventUpdate  # Prevent the page from redirecting if no parameters are selected



    params = {
        'movieLabel': [quote(film_title)] if film_title else None,
        'startYear': year[0] if 'enabled' in enable_year_range else None,
        'endYear': year[1] if 'enabled' in enable_year_range else None,
        'genres': [quote(genre) for genre in genres] if genres else None,
        'number_of_results': number_of_results,
        'actors': [quote(actor) for actor in actors] if actors else None,
        'director': quote(director) if director else None,
        'description': quote(plot_description) if plot_description else None,
        'getSimilarMovies': True if film_title else False
    }
    movies = requests.get(f'{REST_SERVICE_URI}/movies_details', params=params).json()
    if not movies or ('detail' in movies):
        if len(movies) == 0 or ('detail' in movies and 'not found' in movies['detail'].lower()):
            return html.Div("No movies were found.")
        else:
            return html.Div("An error occurred while fetching the movies. Please try again later.")
    

    movie_results = [
        html.Div([
            html.Div(f"{index + 1}", className="movie-index"),
            html.Div([
                html.Span("Similarity Score: ", className="attribute-label"),
                html.Span(movie.get('similarity_score', 'N/A'), className="attribute-value")
            ], className="similarity-score") if 'similarity_score' in movie and movie['similarity_score'] not in [None, ''] else None,
            html.H3(f"{movie.get('title', 'N/A')}"),
            html.P([html.Span("Runtime: ", className="attribute-label"), html.Span(movie.get('runtime', 'N/A'), className="attribute-value")]) if 'runtime' in movie and movie['runtime'] not in [None, ''] else None,
            html.P([html.Span("Release Year: ", className="attribute-label"), html.Span(movie.get('releaseYear', 'N/A'), className="attribute-value")]) if 'releaseYear' in movie and movie['releaseYear'] not in [None, ''] else None,
            html.P([html.Span("Country: ", className="attribute-label"), html.Span(movie.get('country', 'N/A'), className="attribute-value")]) if 'country' in movie and movie['country'] not in [None, ''] else None,
            html.P([html.Span("Genres: ", className="attribute-label"), html.Span(movie.get('genres', 'N/A'), className="attribute-value")], className="genres") if 'genres' in movie and movie['genres'] not in [None, ''] else None,
            html.P([html.Span("Starring: ", className="attribute-label"), html.Span(movie.get('starring', 'N/A'), className="attribute-value")], className="starring") if 'starring' in movie and movie['starring'] not in [None, ''] else None,
            html.P([html.Span("Directors: ", className="attribute-label"), html.Span(movie.get('directors', 'N/A'), className="attribute-value")], className="directors") if 'directors' in movie and movie['directors'] not in [None, ''] else None,
            html.P([html.Span("Abstract: ", className="attribute-label"), html.Span(movie.get('abstract', 'N/A'), className="attribute-value")], className="abstract") if 'abstract' in movie and movie['abstract'] not in [None, ''] else None
        ], className="movie-card")
        for index, movie in enumerate(movies)
    ]
    return html.Div(movie_results, className="movie-results")

# @callback(
#     Output("film-title", "options"),
#     [Input("film-title", "search_value"),
#      State("film-title", "value")]
# )
# def update_options_film_title(search_value, current_value):
#     encoded_search_value = quote(search_value) if search_value else ""
#     movies = get_options_from_api(f'{REST_SERVICE_URI}/movies?movieLabel={encoded_search_value}&numberOfResults=500')
    
#     # Ensure the current value is included in the options
#     if current_value and current_value not in [movie['value'] for movie in movies]:
#         movies.append({"label": current_value, "value": current_value})
    
#     return movies

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
    encoded_search_value = quote(search_value) 
    actors = get_options_from_api(f'{REST_SERVICE_URI}/actors?actorName={encoded_search_value}')
    return actors

if __name__ == '__main__':
    if is_running_in_docker():
        app.run_server(debug=True, host='0.0.0.0') 
    else:
        app.run_server(debug=True)