
from django.db import models
from django.contrib.auth.models import User

class ScraperKey(models.Model):
    PLATFORM_CHOICES = [
        ('ig', 'Instagram'),
        ('tk', 'TikTok'),
        ('x', 'X/Twitter'),
        ('yt', 'YouTube'),
        ('fb', 'Facebook'),
    ]
    platform = models.CharField(max_length=2, choices=PLATFORM_CHOICES)
    key_value = models.TextField()
    purpose = models.CharField(max_length=50, default='general')
    is_active = models.BooleanField(default=True)
    last_used = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'ScraperKey'

class ScrapeResult(models.Model):
    platform = models.CharField(max_length=10)
    username = models.CharField(max_length=255)
    followers = models.BigIntegerField(default=0)
    post_date = models.DateTimeField(null=True)
    likes = models.IntegerField(default=0)
    comments = models.IntegerField(default=0)
    views = models.IntegerField(default=0)
    description = models.TextField(blank=True)
    raw_data = models.JSONField(null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    is_loto = models.BooleanField(default=False)
    sentimiento_global = models.CharField(max_length=50, default='N/A')
    alegria = models.FloatField(default=0.0)
    confianza = models.FloatField(default=0.0)
    miedo = models.FloatField(default=0.0)
    sorpresa = models.FloatField(default=0.0)
    tristeza = models.FloatField(default=0.0)
    aversion = models.FloatField(default=0.0)
    ira = models.FloatField(default=0.0)
    anticipacion = models.FloatField(default=0.0)

    class Meta:
        db_table = 'ScrapeResult'
        ordering = ['-created_at']
