from django.core.management.base import BaseCommand, CommandError
from taxi.models import Recommendation
import pickle
from django.contrib.gis.geos import Polygon


class Command(BaseCommand):
    help = 'Unserializes and persists the recommendations from "result.pickle" file'

    def handle(self, *args, **options):
        with open('result.pickle', 'rb') as f:
            result = pickle.load(f)
            for weekday, hour, c_score, vertices in result:
                vertices = [list(v) for v in vertices]
                vertices += [vertices[0]]  # Last vert == first vert
                Recommendation.objects.create(
                    hour=hour, weekday=weekday, score=c_score,
                    poly=Polygon(vertices)
                )
