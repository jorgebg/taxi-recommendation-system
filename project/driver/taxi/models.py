# from dateutil.rrule import weekdays
from django.contrib.gis.db import models
import calendar
# calendar.day_name

CENTER = {'lat':40.730610, 'lng':-73.935242}

class Recommendation(models.Model):
    HOUR_CHOICES = tuple(zip(range(0, 24), map(str, range(0,24))))
    WEEKDAY_CHOICES = tuple(zip(range(0, 7), map(str, calendar.day_name)))

    hour = models.IntegerField(choices=HOUR_CHOICES)
    weekday = models.IntegerField(choices=WEEKDAY_CHOICES)
    score = models.FloatField()
    poly = models.PolygonField()

    def __str__(self):
        return 'Recommendation(hour=%s, day=%s, score=%s)' % (self.hour, self.weekday, self.score)
