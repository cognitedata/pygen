from . import api, data_classes, list_data_classes


class MovieClient:
    def __init__(self):
        self.movies = api.MovieAPI(
            data_classes.Movie,
            list_data_classes.MovieList,
        )
        self.persons = api.PersonsAPI(data_classes.Person, list_data_classes.PersonList)
        self.ratings = api.RatingsAPI(data_classes.Rating, list_data_classes.RatingList)
        self.actors = api.ActorsAPI(data_classes.Actor, list_data_classes.ActorList)
        self.directors = api.DirectorAPI(data_classes.Director, list_data_classes.DirectorList)
        self.best_directors = api.BestDirectorAPI(data_classes.BestDirector, list_data_classes.BestDirectorList)
        self.best_leading_actor = api.BestLeadingActorAPI(
            data_classes.BestLeadingActor, list_data_classes.BestLeadingActorList
        )
        self.best_leading_actress = api.BestLeadingActressAPI(
            data_classes.BestLeadingActress, list_data_classes.BestLeadingActressList
        )
