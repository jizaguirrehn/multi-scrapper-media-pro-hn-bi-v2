
from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone

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


class DimUsuario(models.Model):
    platform = models.CharField(max_length=10)
    username = models.CharField(max_length=255)
    followers = models.BigIntegerField(default=0)
    followers_updated_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'DimUsuario'
        constraints = [
            models.UniqueConstraint(
                fields=['platform', 'username'],
                name='unique_dim_usuario_platform_username',
            )
        ]
        indexes = [
            models.Index(fields=['platform', 'username']),
        ]

    def actualizar_seguidores(self, seguidores):
        if seguidores is None or int(seguidores) <= 0:
            return False

        seguidores = int(seguidores)
        if self.followers != seguidores:
            self.followers = seguidores
            self.followers_updated_at = timezone.now()
            self.save(update_fields=['followers', 'followers_updated_at'])
            return True
        return False


def obtener_o_actualizar_usuario(platform, username, seguidores=0):
    usuario, _ = DimUsuario.objects.get_or_create(
        platform=platform,
        username=username,
        defaults={
            'followers': int(seguidores or 0),
            'followers_updated_at': timezone.now() if seguidores and int(seguidores) > 0 else None,
        },
    )
    usuario.actualizar_seguidores(seguidores)
    return usuario

class ScrapeResult(models.Model):
    platform = models.CharField(max_length=10)
    username = models.CharField(max_length=255)
    dim_usuario = models.ForeignKey(
        DimUsuario,
        on_delete=models.PROTECT,
        related_name='scrape_results',
        null=True,
        blank=True,
    )
    followers = models.BigIntegerField(default=0)
    post_date = models.DateTimeField(null=True)
    likes = models.IntegerField(default=0)
    comments = models.IntegerField(default=0)
    views = models.IntegerField(default=0)
    description = models.TextField(blank=True)
    hashtags = models.TextField(blank=True, null=True)
    raw_data = models.TextField(null=True, blank=True)
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


class PostComment(models.Model):
    post = models.ForeignKey(ScrapeResult, on_delete=models.CASCADE, related_name='comentarios_guardados')
    texto = models.TextField()
    platform = models.CharField(max_length=10, default='')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'PostComment'
        ordering = ['-created_at']

    def __str__(self):
        return f"Comentario en {self.post.username} - {self.platform}"
