from django.contrib.gis import admin
from .models import Recommendation, CENTER


class RecommendationAdmin(admin.GeoModelAdmin):
    list_display = ['weekday', 'hour', 'score', 'poly']
    default_lon = CENTER['lng']
    default_lat = CENTER['lat']
    default_zoom = 10

admin.site.register(Recommendation, RecommendationAdmin)
