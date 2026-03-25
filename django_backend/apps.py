from django.apps import AppConfig
from django.db.models.signals import post_migrate

def create_default_groups(sender, **kwargs):
    from django.contrib.auth.models import Group
    grupos = ['Admin_Scraper', 'Colaborador', 'Usuario', 'Gerente', 'Director']
    for nombre in grupos:
        Group.objects.get_or_create(name=nombre)
    print(f"--- Grupos de {sender.name} verificados/creados ---")

class DjangoBackendConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'django_backend'

    def ready(self):
        import django_backend.signals
        post_migrate.connect(create_default_groups, sender=self)