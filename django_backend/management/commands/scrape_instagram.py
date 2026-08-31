from django.core.management.base import BaseCommand
from django_backend.scripts.script_ig import iniciar
import os


class Command(BaseCommand):
    help = 'Ejecuta el scraper de Instagram para extraer posts y comentarios'

    def add_arguments(self, parser):
        parser.add_argument(
            '--perfiles',
            type=str,
            default='aleborjas91',
            help='Lista de perfiles separados por coma: python manage.py scrape_instagram --perfiles perfil1,perfil2'
        )

    def handle(self, *args, **options):
        # Obtener la API key desde variables de entorno
        mis_apis_keys = [os.getenv("RAPIDAPI_KEY", "TU_API_KEY_AQUI")]
        
        # Parsear perfiles
        perfiles_str = options['perfiles']
        lista_perfiles = [p.strip() for p in perfiles_str.split(',')]
        
        self.stdout.write(self.style.SUCCESS(f'Iniciando scraper para: {", ".join(lista_perfiles)}'))
        
        try:
            iniciar(mis_apis_keys, lista_perfiles)
            self.stdout.write(self.style.SUCCESS('✅ Proceso completado exitosamente'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Error: {e}'))
