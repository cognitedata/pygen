from . import api, data_classes, list_data_classes


class NominationAPI:
    def __init__(self, data):
        self.best_directors = api.BestDirectorAPI(
            data_classes.BestDirector, list_data_classes.BestDirectorList, data.best_directors
        )
        self.best_leading_actor = api.BestLeadingActorAPI(
            data_classes.BestLeadingActor, list_data_classes.BestLeadingActorList, data.best_actors
        )
        self.best_leading_actress = api.BestLeadingActressAPI(
            data_classes.BestLeadingActress, list_data_classes.BestLeadingActressList, data.best_actress
        )


class RolesAPI:
    def __init__(self, data):
        self.actors = api.ActorsAPI(data_classes.Actor, list_data_classes.ActorList, data.actors)
        self.directors = api.DirectorAPI(data_classes.Director, list_data_classes.DirectorList, data.directors)


class MovieClient:
    def __init__(self, data):
        self.movies = api.MovieAPI(data_classes.Movie, list_data_classes.MovieList, data.movies)
        self.nominations = NominationAPI(data)
        self.persons = api.PersonsAPI(data_classes.Person, list_data_classes.PersonList, data.persons)
        self.ratings = api.RatingsAPI(data_classes.Rating, list_data_classes.RatingList)
        self.roles = RolesAPI(data)
