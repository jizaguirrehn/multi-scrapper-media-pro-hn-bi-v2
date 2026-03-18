from django.apps import AppConfig

class DjangoBackendConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'django_backend'

    def ready(self):
        import django_backend.signals