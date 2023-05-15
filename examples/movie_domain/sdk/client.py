from . import api


class MovieClient:
    def __init__(self):
        self.movies = api.MovieAPI()
        self.directors = api.DirectorAPI()
        self.nominations = api.NominationAPI()
