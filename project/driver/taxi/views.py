from django.views.generic.base import TemplateView
from django.contrib.gis import measure
from django.contrib.gis.geos import Point
from django.contrib.gis.db.models.functions import Distance

from taxi.models import Recommendation, CENTER
from taxi.forms import RecommendationForm
from datetime import datetime
from django.db.models.expressions import Value, Func, F

class RecommendationView(TemplateView):

    template_name = "taxi/recommendation.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        params = self.request.GET.copy()
        now = datetime.now()
        params.setdefault('hour', now.hour)
        params.setdefault('weekday', now.weekday())
        params.setdefault('lng', -73.989774)
        params.setdefault('lat', 40.752336)

        hour = params.get('hour')
        weekday = params.get('weekday')
        lat = float(params.get('lat'))
        lng = float(params.get('lng'))

        point = Point(lng, lat, srid=4326)
        recommendations = (
            Recommendation.objects
            .filter(hour=hour, weekday=weekday)
            .annotate(distance=Distance('poly', point))
            .annotate(point=Func(
                F('poly'),
                Func(Value(str(point)), function='ST_PointFromText'),
                function='ST_ClosestPoint')
            )
            .filter(distance__lte=measure.Distance(mi=5).m)
        ).order_by('-score')[:10]
        context['recommendations'] = [list(r.point) for r in recommendations]

        context['form'] = RecommendationForm(data=params)
        return context
