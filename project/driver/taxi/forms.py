from django import forms
from taxi.models import Recommendation


class RecommendationForm(forms.ModelForm):
    lat = forms.FloatField()
    lng = forms.FloatField()

    class Meta:
        model = Recommendation
        fields = ['hour', 'weekday', 'lat', 'lng']
