from . import api


class NominationAPI:
    def __init__(self):
        self.best_directors = api.BestDirectorAPI()
        self.best_leading_actor = api.BestLeadingActorAPI()
        self.best_leading_actress = api.BestLeadingActressAPI()


class RolesAPI:
    def __init__(self):
        self.actors = api.ActorsAPI()
        self.directors = api.DirectorAPI()


class MovieClient:
    def __init__(self):
        self.movies = api.MovieAPI()
        self.nominations = NominationAPI()
        self.persons = api.PersonsAPI()
        self.ratings = api.RatingsAPI()
        self.roles = RolesAPI()
