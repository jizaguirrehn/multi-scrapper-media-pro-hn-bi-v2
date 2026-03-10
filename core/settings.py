import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
SECRET_KEY = 'django-insecure-tu-clave-secreta'

# CAMBIAR A FALSE EN AZURE
DEBUG = False 
ALLOWED_HOSTS = ['*']

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles', # Necesario para archivos estáticos
    'rest_framework',
    'corsheaders',
    'django_backend',
    'whitenoise.runserver_nostatic', # Agregado para WhiteNoise
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware', # Debe ir después de Security
    'django.contrib.sessions.middleware.SessionMiddleware', # <--- AGREGAR ESTA LÍNEA
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware', # <--- AHORA SÍ FUNCIONARÁ
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

CORS_ALLOW_ALL_ORIGINS = True 
ROOT_URLCONF = 'core.urls'

# CONFIGURACIÓN DE TEMPLATES PARA REACT
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [os.path.join(BASE_DIR, 'static')], # Donde está tu index.html
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

IF_AZURE = os.path.exists('/home/site')

if IF_AZURE:
    if not os.path.exists('/home/data'):
        os.makedirs('/home/data', exist_ok=True)
    
    DB_PATH = '/home/data/db.sqlite3'
else:
    # Desarrollo local
    DB_PATH = BASE_DIR / 'db.sqlite3'


DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': DB_PATH,
    }
}

# ARCHIVOS ESTÁTICOS (CRUCIAL PARA EL DESPLIEGUE)
STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'staticfiles') # Azure usará esto

# Donde Django buscará los archivos generados por npm run build
STATICFILES_DIRS = [
    os.path.join(BASE_DIR, 'static'),
]

# Optimización de WhiteNoise para producción
STATICFILES_STORAGE = 'whitenoise.storage.CompressedManifestStaticFilesStorage'

USE_TZ = True
