from dash import Dash, html, dcc, Input, Output, State, exceptions
import requests
import os

def is_running_in_docker():
    """Check if the code is running inside a Docker container."""
    path = '/proc/1/cgroup'
    if os.path.exists(path):
        with open(path, 'r') as f:
            return 'docker' in f.read()
    return False

REST_SERVICE_URL = "http://localhost"
if is_running_in_docker():
    REST_SERVICE_URL = "http://host.docker.internal"
REST_SERVICE_URL += ":80/"

# Initializing the application
app = Dash(__name__)

# Function to get options from REST API
def get_options_from_api(endpoint):
    response = requests.get(endpoint)
    if response.status_code == 200:
        data = response.json()
        return [{"label": item["label"], "value": item["label"]} for item in data]
    else:
        return []

app_data = {}

# Function to initialize the app    
def init_app():
    global app_data
    # Fetch initial options for dropdowns
    app_data["movies_options"] = get_options_from_api(f'{REST_SERVICE_URL}movies')
    app_data["director_options"] = get_options_from_api(f'{REST_SERVICE_URL}directors')
    app_data["actor_options"] = get_options_from_api(f'{REST_SERVICE_URL}actors')
    app_data["countries_options"] = get_options_from_api(f'{REST_SERVICE_URL}countries')

# Call init_app to initialize the app data
init_app()

# Define the layout of the app
app.layout = html.Div([
    # Sidebar styling
    html.Div([
        html.H2("Find Your Next Movie 😊", className="sidebar-title"),

        # Film title selection
        html.Div([
            html.Label("Select Film Title:"),
            dcc.Dropdown(
                id="film-title",
                options=app_data.get("movies_options", []),
                multi=True,
                placeholder="Select a title",
                className="dropdown"
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
                options=app_data.get("director_options", []),
                value="Select a director",
                className="dropdown"
            )
        ], className="input-group"),

        # Country selection
        html.Div([
            html.Label("Select Country:"),
            dcc.Dropdown(
                id="country",
                options=app_data.get("countries_options", []),
                value="Select a country",
                className="dropdown"
            )
        ], className="input-group"),

        # Actors selection
        html.Div([
            html.Label("Select Actors:"),
            dcc.Dropdown(
                id="actors",
                options=app_data.get("actor_options", []),
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
    Output("film-title", "options"),
    Input("film-title", "search_value"),
    prevent_initial_call=True
)
def update_movie_options(search_value):
    if not search_value:
        raise exceptions.PreventUpdate
    endpoint = f'{REST_SERVICE_URL}movies?movieLabel={search_value}'
    return get_options_from_api(endpoint)

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
    app.run_server(host='0.0.0.0', port=8050, debug=True)