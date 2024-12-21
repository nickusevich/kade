from dash import Dash, html, dcc, Input, Output, State, callback
import requests
import dash
from dash.exceptions import PreventUpdate
from urllib.parse import urlencode, parse_qs

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

# Fetch initial options
movies_options = get_options_from_api('http://localhost:80/movies')
director_options = get_options_from_api('http://localhost:80/directors')
actor_options = get_options_from_api('http://localhost:80/actors')
genre_options = get_options_from_api('http://localhost:80/genres')

# Define the layout for the first page (Home Page)
home_layout = html.Div([
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
            min=1990,
            max=2023,
            step=1,
            value=[1990, 2023],
            marks={i: str(i) for i in range(1990, 2024, 3)}
        ),
        html.Label("Rating Range:", className="label"),
        dcc.RangeSlider(
            id="rating-range",
            min=0,
            max=10,
            step=1,
            value=[0, 10],
            marks={i: str(i) for i in range(0, 11)}
        ),
        html.Label("Number of Similar Movies:", className="label"),
        dcc.Input(
            id="similar-movies",
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
            style={"width": "100%", "height": "100px"},
            value=""
        ),
        html.Button("Search", id="search-btn", className="button", n_clicks=0)
    ], className="form-container")
])

# Define the layout for the results page
results_layout = html.Div([
    html.H1("Results Page", className="main-title"),
    html.Div(id="results-display"),
    dcc.Link('Back to Search', href='/', className="back-link")
])

# Define the layout for the app (main entry point)
app.layout = html.Div([
    dcc.Location(id='url', refresh=False), # it is a component that allows you to manage the URL state in your Dash app. 
    # this is important for handling navigation between pages. the pathname of this component changes based on the URL in the browser.
    dcc.Store(id='search-store', storage_type='memory'), # it is a component used to store the search parameters in memory. This is where you will save the user's search parameters, which can then be accessed on the results page.
    html.Div(id='page-content')
])





#________________
# callback to update the page content based on the URL
@app.callback(
    Output('page-content', 'children'), #output is the page content itself
    [Input('url', 'pathname')] # input as url, so below we have function, and based on URL it returns the layout
)
def display_page(pathname):
    if pathname == '/results':
        return results_layout
    return home_layout 

# callback for the search button to store parameters and redirect
@app.callback(
    [Output('search-store', 'data'),
     Output('url', 'pathname')],
    [Input('search-btn', 'n_clicks')],
    [State('film-title', 'value'),
     State('rating-range', 'value'),
     State('year-range', 'value'),
     State('genres', 'value'),
     State('similar-movies', 'value'),
     State('actors', 'value'),
     State('director', 'value'),
     State('plot-description', 'value')],
    prevent_initial_call=True
)
def store_search_parameters(n_clicks, film_title, rating_range, year_range, 
                          genres, similar_movies, actors, director, plot_description):
    if n_clicks == 0:
        raise PreventUpdate

    # Check if any of the search parameters are filled in
    if not any([film_title, rating_range, year_range, genres, similar_movies, actors, director, plot_description]):
        raise PreventUpdate  # Prevent the page from redirecting if no parameters are selected

    search_params = {
        'film_title': film_title,
        'rating_range': rating_range,
        'year_range': year_range,
        'genres': genres,
        'similar_movies': similar_movies,
        'actors': actors,
        'director': director,
        'plot_description': plot_description
    }
    
    return search_params, '/results'


# Callback to display search results
@app.callback(
    Output('results-display', 'children'),
    [Input('search-store', 'data')]
)
def update_results(stored_data):
    if not stored_data:
        raise PreventUpdate
    
    # Here you would typically make an API call with the stored parameters
    # For now, just display the search parameters
    results_content = []
    
    for key, value in stored_data.items():
        if value is not None:  # Only display non-None values
            formatted_key = key.replace('_', ' ').title()
            formatted_value = str(value)
            results_content.append(
                html.Div([
                    html.Strong(f"{formatted_key}: "),
                    html.Span(formatted_value)
                ], className="result-item")
            )
    
    return html.Div([
        html.H2("Search Parameters"),
        html.Div(results_content, className="results-container")
    ])

if __name__ == '__main__':
    app.run_server(debug=True)
