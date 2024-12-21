from dash import Dash, html, dcc, Input, Output, State, exceptions
import requests

# Initialize the app
app = Dash(__name__)

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
print(genre_options)

app.layout = html.Div([
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
            )
        ], className="form-container"),

        html.Button("Search", id="search-btn", className="button", n_clicks=0)
    ], className="center-container"),

    html.Div(id="output", className="results-container")
])


@app.callback(
    Output("output", "children"),
    Input("search-btn", "n_clicks"),
    State("film-title", "value"),
    State("rating-range", "value"),
    State("year-range", "value"),
    State("genres", "value"),
    State("similar-movies", "value"),
    State("actors", "value"),
    State("director", "value"),
    State("plot-description", "value"),
    prevent_initial_call=True
)
def search_movies(n_clicks, film_title, rating_range, year_range, genres, similar_movies, actors, director, plot_description):
    # Prepare the search parameters
    params = {
        "film_title": film_title,
        "rating_range": rating_range,
        "year_range": year_range,
        "genres": genres,
        "similar_movies": similar_movies,
        "actors": actors,
        "director": director,
        "plot_description": plot_description
    }
    # Make a call to your REST API
    response = requests.post("http://localhost:80/search", json=params)
    if response.status_code == 200:
        return html.Pre(str(response.json()))
    else:
        return html.Pre("Error fetching data. Please try again.")

if __name__ == '__main__':
    app.run_server(debug=True)
