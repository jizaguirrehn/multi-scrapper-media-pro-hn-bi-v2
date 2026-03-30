
import os
import logging
from cryptography.fernet import Fernet, InvalidToken
from django.db import models
from django.contrib.auth.models import User

logger = logging.getLogger(__name__)


def _get_keys_fernet():
    key = os.getenv('SCRAPER_KEYS_MASTER_KEY', '')
    if not key:
        raise ValueError('SCRAPER_KEYS_MASTER_KEY is required to encrypt/decrypt scraper keys')
    return Fernet(key.encode())

class ScraperKey(models.Model):
    ENCRYPTED_PREFIX = 'enc::'

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

    @classmethod
    def _is_encrypted(cls, value):
        return isinstance(value, str) and value.startswith(cls.ENCRYPTED_PREFIX)

    @classmethod
    def _encrypt_value(cls, raw_value):
        if not raw_value:
            return raw_value
        if cls._is_encrypted(raw_value):
            return raw_value
        token = _get_keys_fernet().encrypt(str(raw_value).encode()).decode()
        return f'{cls.ENCRYPTED_PREFIX}{token}'

    @classmethod
    def _decrypt_value(cls, stored_value):
        if not stored_value:
            return stored_value
        if not cls._is_encrypted(stored_value):
            # Backward compatibility: valores antiguos en texto plano.
            return stored_value

        token = stored_value[len(cls.ENCRYPTED_PREFIX):]
        try:
            return _get_keys_fernet().decrypt(token.encode()).decode()
        except InvalidToken as e:
            raise ValueError('Unable to decrypt scraper key: invalid token or master key') from e

    def get_decrypted_key(self):
        return self._decrypt_value(self.key_value)

    @classmethod
    def get_active_keys(cls, platform, purpose=None):
        query = cls.objects.filter(platform=platform, is_active=True)
        if purpose is not None:
            query = query.filter(purpose=purpose)

        keys = []
        for row in query:
            try:
                keys.append(row.get_decrypted_key())
            except Exception as e:
                logger.error('Error decrypting key id=%s platform=%s: %s', row.id, row.platform, e)
        return [k for k in keys if k]

    def save(self, *args, **kwargs):
        if self.key_value:
            self.key_value = self._encrypt_value(self.key_value)
        super().save(*args, **kwargs)

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


class ExtractionRequestLog(models.Model):
    STATUS_CHOICES = [
        ('PENDING', 'PENDING'),
        ('STARTED', 'STARTED'),
        ('DENIED', 'DENIED'),
        ('INVALID', 'INVALID'),
        ('ERROR', 'ERROR'),
    ]

    user = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    platform = models.CharField(max_length=20, blank=True, default='')
    targets = models.JSONField(default=list)
    ip_address = models.CharField(max_length=64, blank=True, default='')
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
    detail = models.CharField(max_length=255, blank=True, default='')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'ExtractionRequestLog'
        ordering = ['-created_at']
