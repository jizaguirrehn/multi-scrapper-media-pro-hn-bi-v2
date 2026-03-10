
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
    key_value = models.TextField()  # Encrypt this in a real prod app
    purpose = models.CharField(max_length=50, default='general')
    is_active = models.BooleanField(default=True)
    last_used = models.DateTimeField(auto_now=True)

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

    class Meta:
        ordering = ['-created_at']
