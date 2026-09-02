# Multi-Scraper Media Pro — Documentación Técnica

API REST construida con **Django + Django REST Framework** para la extracción, almacenamiento y análisis de publicaciones de redes sociales (Instagram, TikTok, X/Twitter, YouTube y Facebook). Incluye análisis de sentimientos mediante un modelo servido desde **Azure Databricks**.

---

## Tabla de Contenidos
1. [Arquitectura General](#arquitectura-general)
2. [Stack Tecnológico](#stack-tecnológico)
3. [Estructura del Proyecto](#estructura-del-proyecto)
4. [Modelos de Datos](#modelos-de-datos)
5. [Autenticación y Roles](#autenticación-y-roles)
6. [API — Endpoints](#api--endpoints)
7. [Servicios de Extracción](#servicios-de-extracción)
8. [Análisis de Sentimientos](#análisis-de-sentimientos)
9. [Variables de Entorno](#variables-de-entorno)
10. [Instalación y Ejecución Local](#instalación-y-ejecución-local)
11. [Despliegue en Azure](#despliegue-en-azure)

## Arquitectura General

```
        │
        │  JWT Bearer Token
        ▼
└───────────┬───────────────────┘
            │
   ┌─────────┴──────────┐
   ▼                    ▼
SQLite DB         Hilos de extracción (threading)
(ScrapeResult,          │
                └───────────────────────────────┘
                            │
                            ▼
                  Azure Databricks Model
                  (Análisis de sentimientos)
```

El backend expone una API JSON. La extracción por plataforma se ejecuta en **hilos daemon** para no bloquear la respuesta HTTP. Los resultados se persisten en SQLite (o en `/home/data/db.sqlite3` en Azure App Service).


| Componente | Versión |
|---|---|
| Python | 3.11+ |
| Django | ≥ 5.0, < 6.0 |
| Django REST Framework | ≥ 3.15 |
| djangorestframework-simplejwt | ≥ 5.3 |
**Individual:** `script_ind_ig.py` procesa un único post usando `post_url`.
| python-jose[cryptography] | ≥ 3.3 |
| requests | ≥ 2.31 |
| pandas | ≥ 2.2 |
**Individual:** `script_ind_tk.py` procesa un único video usando `videoId`.
| google-api-python-client | ≥ 2.130 |
| python-dotenv | ≥ 1.0 |

**Individual:** `script_ind_x.py` procesa un tweet usando `tweet_id`.

## Estructura del Proyecto

**Individual:** `script_ind_yb.py` procesa un video usando `video_id`, pagina comentarios y los guarda en `PostComment`.
├── manage.py                          # Punto de entrada Django CLI
├── requirements.txt                   # Dependencias Python
├── databricks.yml                     # Configuración del job en Databricks
**Individual:** `script_ind_fb.py` procesa un post usando `post_url`, obtiene comentarios desde `/post/comments` y evita duplicados por `post_id`.
│
├── core/                              # Configuración del proyecto Django
│   ├── settings.py                    # Configuración global (DB, JWT, CORS, Email)
│   ├── urls.py                        # Enrutamiento raíz
│   └── wsgi.py                        # Punto de entrada WSGI
│
├── django_backend/                    # Aplicación principal
│   ├── models.py                      # ScraperKey, DimUsuario, ScrapeResult, PostComment
│   ├── serializers.py                 # Serializadores DRF
│   ├── views.py                       # ViewSet y autenticación
│   ├── signals.py                     # Grupos de usuarios
│   ├── migrations/                    # Migraciones de base de datos
│   └── scripts/                       # Scrapers y análisis
│       ├── script_ig.py               # Instagram masivo
│       ├── script_ind_ig.py           # Instagram por post
│       ├── script_tk.py               # TikTok masivo
│       ├── script_ind_tk.py           # TikTok por videoId
│       ├── script_x.py                # X/Twitter masivo
│       ├── script_ind_x.py            # X por tweet_id
│       ├── script_yb.py               # YouTube masivo
│       ├── script_ind_yb.py           # YouTube por video_id
│       ├── script_fb.py               # Facebook masivo
│       ├── script_ind_fb.py           # Facebook por post_url
│       └── sentiments/analizador.py   # Cliente del modelo Databricks
│
└── results/                           # Archivos CSV exportados por fecha
    └── datos_<plataforma>_<fecha>.csv
```

---

## Modelos de Datos

### `ScraperKey`

Almacena las claves de API de RapidAPI y YouTube por plataforma y propósito.

| Campo | Tipo | Descripción |
|---|---|---|
| `platform` | CharField | `ig`, `tk`, `x`, `yt`, `fb` |
| `key_value` | TextField | Valor de la clave API |
| `purpose` | CharField | `general`, `search`, `posts` |
| `is_active` | BooleanField | Activa/inactiva |
| `last_used` | DateTimeField | Última modificación (auto) |

Los scrapers consultan las claves con `platform`, `purpose` e `is_active`.

> No publiques claves reales. Las claves expuestas deben regenerarse en el proveedor correspondiente.

### `DimUsuario`

Dimensión de usuarios de redes sociales, única por la combinación `(platform, username)`.

| Campo | Tipo | Descripción |
|---|---|---|
| `platform` | CharField | `ig`, `tk`, `x`, `yt` o `fb` |
| `username` | CharField | Usuario, página o canal |
| `followers` | BigIntegerField | Seguidores o suscriptores actuales |
| `followers_updated_at` | DateTimeField | Última actualización positiva |

Cada scraper actualiza los seguidores si recibe un valor positivo diferente. `ScrapeResult.followers` conserva el snapshot histórico.

---

### `ScrapeResult`

Registro de cada publicación extraída de las redes sociales.

| Campo | Tipo | Descripción |
|---|---|---|
| `platform` | CharField | Plataforma de origen |
| `username` | CharField | Handle del influencer |
| `dim_usuario` | ForeignKey | Usuario relacionado en `DimUsuario` |
| `followers` | BigIntegerField | Cantidad de seguidores |
| `post_date` | DateTimeField | Fecha de publicación original |
| `likes` | IntegerField | Me gusta |
| `comments` | IntegerField | Comentarios |
| `views` | IntegerField | Vistas (aplica a video) |
| `description` | TextField | Texto del post/descripción |
| `raw_data` | TextField | Respuesta JSON completa de la API |
| `created_at` | DateTimeField | Fecha de extracción (auto) |
| `is_loto` | BooleanField | Marcado como contenido Loto |
| `sentimiento_global` | CharField | Sentimiento predominante del modelo |
| `alegria` | FloatField | Peso de la emoción |
| `confianza` | FloatField | Peso de la emoción |
| `miedo` | FloatField | Peso de la emoción |
| `sorpresa` | FloatField | Peso de la emoción |
| `tristeza` | FloatField | Peso de la emoción |
| `aversion` | FloatField | Peso de la emoción |
| `ira` | FloatField | Peso de la emoción |
| `anticipacion` | FloatField | Peso de la emoción |

---

### `PostComment`

Guarda los comentarios individuales relacionados con una publicación.

| Campo | Tipo | Descripción |
|---|---|---|
| `post` | ForeignKey | Publicación relacionada |
| `texto` | TextField | Texto del comentario |
| `platform` | CharField | Plataforma de origen |
| `created_at` | DateTimeField | Fecha de inserción |

La migración `0006_dimusuario_scraperesult_dim_usuario.py` crea la dimensión y enlaza los resultados históricos existentes.

---

## Autenticación y Roles

### Flujos de autenticación

| Endpoint | Descripción |
|---|---|
| `POST /api/auth/register/` | Registro de nuevo usuario con contraseña |
| `POST /api/auth/login/` | Login con `username` o `email` + contraseña |
| `POST /api/auth/azure-login/` | Login federado con token Azure AD (OIDC) |
| `POST /api/token/` | Obtener par de tokens JWT (SimpleJWT estándar) |
| `POST /api/token/refresh/` | Renovar `access_token` usando `refresh_token` |

Todos los endpoints protegidos requieren el header:
```
Authorization: Bearer <access_token>
```

Los tokens de acceso tienen vigencia de **60 minutos**. El refresh tiene vigencia de **1 día** y rota en cada uso.

### Grupos de usuarios

| Grupo | Permisos |
|---|---|
| `Admin_Scraper` | Acceso total: extracciones, llaves, roles, cambio de contraseña, métricas |
| `Director` | Extracciones, métricas, historial de usuarios |
| `Gerente` | Extracciones, métricas, historial de usuarios |
| `Usuario` | Extracciones, historial de usuarios, perfil de influencer |
| `Colaborador` | Asignado automáticamente al registrarse (sin acceso a extracción) |

> **Signal:** Al crear un nuevo usuario, `signals.py` asigna automáticamente el grupo `Colaborador`.

### Login con Azure AD

El endpoint `POST /api/auth/azure-login/` recibe un `access_token` emitido por Microsoft Identity Platform. El token se valida obteniendo las llaves públicas del JWKS del tenant de Azure (`login.microsoftonline.com/common`), verificando la firma RSA. Si el token es válido, se crea o recupera el usuario por email y se emite un par de tokens JWT propios.

---

## API — Endpoints

Base URL: `/api/scraper/`

### `POST /api/scraper/trigger_extraction/`

Inicia la extracción asíncrona de publicaciones para una plataforma y lista de targets.

**Permisos:** `Admin_Scraper`, `Usuario`, `Gerente`, `Director`

**Body:**
```json
{
  "platform": "ig",
  "targets": ["usuario1", "usuario2"]
}
```

**Plataformas soportadas:** `ig`, `tk`, `x`, `yt`, `fb`

La extracción se ejecuta en un **hilo daemon** independiente. La respuesta es inmediata con código `202 Accepted`.

**Respuesta exitosa:**
```json
{
  "status": "Extracción iniciada",
  "platform": "ig",
  "started_at": "2026-04-14T10:00:00Z"
}
```

---

### `POST /api/scraper/bulk_update/`

Reemplaza masivamente las llaves de API por plataforma y propósito.

**Permisos:** `Admin_Scraper`

**Body:**
```json
{
  "ig": {
    "general": ["RAPIDAPI_KEY_1", "RAPIDAPI_KEY_2"]
  },
  "tk": {
    "search": ["KEY_A"],
    "posts":  ["KEY_B"]
  }
}
```

Las llaves anteriores de esa combinación `(platform, purpose)` se marcan como inactivas (`is_active=False`) y se crea la nueva activa.

---

### `GET /api/scraper/latest_results/`

Devuelve las extracciones más recientes.

**Permisos:** Autenticado

**Query params:**

| Parámetro | Tipo | Descripción |
|---|---|---|
| `platform` | string | Filtrar por plataforma |
| `since` | datetime | Filtrar registros posteriores a esta fecha |
| `limit` | int | Máximo de resultados (default: 1000) |

Cada resultado incluye el objeto `usuario` relacionado con `DimUsuario`. El campo `followers` del resultado es histórico; `usuario.followers` es el valor actual más reciente.

### Endpoints de posts individuales

Todos requieren `Authorization: Bearer <access_token>` y consultan la llave activa correspondiente en `ScraperKey`.

#### `GET /api/scraper/instagram-post/`

Procesa un post de Instagram usando su URL.

```text
/api/scraper/instagram-post/?post_url=https://www.instagram.com/usuario/p/CODIGO/
```

#### `GET /api/scraper/tiktok-post/`

Procesa un video de TikTok usando `videoId`.

```text
/api/scraper/tiktok-post/?videoId=7306132438047116586
```

#### `GET /api/scraper/x-post/`

Procesa un tweet usando `tweet_id`.

```text
/api/scraper/x-post/?tweet_id=1671370010743263233
```

#### `GET /api/scraper/facebook-post/`

Procesa un post de Facebook usando `post_url`. En PowerShell, codifica el URL porque puede contener `&`.

```text
/api/scraper/facebook-post/?post_url=https%3A%2F%2Fwww.facebook.com%2Fphoto%3Ffbid%3D1709187494547460
```

#### `GET /api/scraper/youtube-post/`

Procesa un video de YouTube usando `video_id`.

```text
/api/scraper/youtube-post/?video_id=fX-g6XNrkiA
```

Respuesta común:

```json
{
  "status": "created",
  "post_id": 123,
  "username": "usuario",
  "platform": "tk",
  "comments_saved": 10,
  "post": {}
}
```

`status` puede ser `created` o `already_exists`.

### Endpoints de comentarios

#### `GET /api/scraper/post-comments/`

Obtiene los comentarios de un resultado específico.

```text
/api/scraper/post-comments/?post_id=123
```

#### `GET /api/scraper/user-comments/`

Obtiene los comentarios de todos los posts de un usuario. Para TikTok, por ejemplo:

```text
/api/scraper/user-comments/?username=will&platform=tk&limit=1000
```

La respuesta incluye `total_posts`, `total_comments` y la lista de comentarios relacionados.

---

### `GET /api/scraper/user_history/`

Busca el historial de publicaciones por nombre de usuario (soporta expresiones regulares).

**Permisos:** `Admin_Scraper`, `Usuario`, `Gerente`, `Director`

**Query params:**

| Parámetro | Descripción |
|---|---|
| `query` | Username exacto o patrón regex. Usar `*` para todos los registros |

**Respuesta:** Lista de `ScrapeResult` ordenados por fecha descendente (máx. 500).

---

### `GET /api/scraper/get_metrics/`

Devuelve el dashboard de métricas agregadas.

**Permisos:** `Admin_Scraper`, `Director`, `Gerente`

**Respuesta:**
```json
{
  "total_extracted": 15000,
  "total_profiles": 320,
  "avg_engagement": 4.72,
  "platform_distribution": {
    "ig": 5000,
    "tk": 4500,
    "x": 3000,
    "yt": 1500,
    "fb": 1000
  },
  "weekly_volume": {
    "Mon": 120, "Tue": 200, "Wed": 180, "Thu": 160,
    "Fri": 220, "Sat": 80, "Sun": 40
  },
  "users_api_calls": {
    "admin": 45,
    "analista1": 12
  }
}
```

El engagement se calcula como: `(likes + comments) / followers * 100`. Se reporta la mediana del conjunto de todos los registros.

---

### Consulta por usuario

Usa `/api/scraper/user_history/?query=usuario` para consultar los posts de un influencer.

**Permisos:** `Admin_Scraper`, `Usuario`, `Gerente`, `Director`

**Query params:**

| Parámetro | Descripción |
|---|---|
| `username` | Handle exacto del influencer (case-insensitive) |

**Respuesta:**
```json
{
  "influencer": {
    "username": "ejemplo_hn",
    "total_posts": 45,
    "latest_platform": "ig",
    "latest_followers": 85000,
    "latest_post_date": "2026-04-10T12:00:00Z",
    "last_updated": "2026-04-14T08:00:00Z",
    "sentimiento_global": "Positivo",
    "is_loto": false,
    "alegria": 12.5,
    "confianza": 8.3,
    "miedo": 1.1,
    "sorpresa": 3.2,
    "tristeza": 0.8,
    "aversion": 0.5,
    "ira": 0.3,
    "anticipacion": 4.7,
    "posts": [ /* lista de ScrapeResult */ ]
  },
  "influencers_list": ["usuario1", "usuario2"]
}
```

Si no se envía `username`, se retorna `400` con la lista de todos los influencers disponibles.

---

---

### `POST /api/scraper/assign_role/`

Asigna un grupo (rol) a un usuario existente.

**Permisos:** `Admin_Scraper` o superusuario Django

**Body:**
```json
{
  "username": "analista1",
  "group_name": "Gerente",
  "clear_existing": true
}
```

Si `clear_existing` es `true`, se eliminan todos los grupos actuales antes de asignar el nuevo. Los grupos `Admin_Scraper` y `Director` también activan `is_staff = True` en el usuario.

---

### `GET /api/scraper/list_users/`

Lista todos los usuarios registrados.

**Permisos:** `is_staff` o miembro de `Admin_Scraper`

**Respuesta:** `[{ "id", "username", "first_name", "email" }, ...]`

---

### `GET /api/scraper/public_status/`

Endpoint público de salud sin autenticación.

**Respuesta:**
```json
{ "status": "Servidor Vivo", "version": "1.6.1" }
```

---

## Servicios de Extracción

Cada servicio vive en `django_backend/scripts/` y los scrapers masivos se invocan desde un hilo daemon mediante `trigger_extraction`.

### Instagram (`script_ig.py`)

- **API:** `instagram-looter2.p.rapidapi.com`
- **Llaves necesarias:** `platform='ig'`, `purpose='general'`
- **Flujo:**
  1. Búsqueda del perfil y obtención de IDs de posts.
  2. Para cada post: solicitud al endpoint `/post` para obtener la caption.
  3. Análisis de sentimientos de las captions acumuladas.
  4. Persistencia en `ScrapeResult` y exportación a CSV en `results/`.
- **Rotación de llaves:** Si una clave devuelve `429 Too Many Requests`, avanza al siguiente índice.
- **Individual:** `script_ind_ig.py` procesa un único post mediante `post_url`.

### TikTok (`script_tk.py`)

- **API:** `tiktok-scraper7.p.rapidapi.com`
- **Llaves necesarias:** `purpose='search'` (buscar perfil) y `purpose='posts'` (obtener posts y comentarios)
- **Flujo:**
  1. Búsqueda del usuario con llaves `search`.
  2. Obtención de hasta 25 comentarios por video con llaves `posts` vía `/comment/list`.
  3. Análisis de sentimientos sobre los textos de comentarios.
  4. Persistencia en `ScrapeResult` y CSV.
- **Cache de IDs:** Los IDs de usuario ya consultados se almacenan en `usuarios_tiktok_registrados.json` para evitar re-consultas.
- **Individual:** `script_ind_tk.py` procesa un único video mediante `videoId`.

### X / Twitter (`script_x.py`)

- **API:** `twitter-api45.p.rapidapi.com`
- **Llaves necesarias:** `purpose='search'` (perfil/screenname) y `purpose='posts'` (timeline)
- **Flujo:**
  1. Resolución de `screen_name` → `rest_id` (con cache en `usuarios_X_registrados.json`).
  2. Obtención del timeline del usuario.
  3. Persistencia de cada tweet en `ScrapeResult`. El campo `comments` almacena `replies`.
- **Conversión de fechas:** Formato `"Tue Feb 17 01:01:13 +0000 2026"` parseado con `strptime`.
- **Individual:** `script_ind_x.py` procesa un tweet mediante `tweet_id`.

### YouTube (`script_yb.py`)

- **API:** Google YouTube Data API v3
- **Llave necesaria:** Variable de entorno `KEY` con la API Key de Google.
- **Flujo:**
  1. Búsqueda del canal por nombre, obtención de `channel_id`.
  2. Obtención de estadísticas del canal (`subscriberCount`).
  3. Recuperación de los últimos 25 videos.
  4. Para cada video: obtención de hasta 50 comentarios de nivel superior.
  5. Análisis de sentimientos sobre los comentarios.
  6. Persistencia en `ScrapeResult` con `platform='yt'`.
- **Individual:** `script_ind_yb.py` procesa un video mediante `video_id`, pagina comentarios y los guarda en `PostComment`.
- Usa `cache_discovery=False` para evitar el caché incompatible de `googleapiclient`.

### Facebook (`script_fb.py`)

- **API:** `facebook-scraper3.p.rapidapi.com`
- **Llaves necesarias:** `platform='fb'`, `purpose='general'`
- **Flujo:**
  1. Obtención de los posts del perfil de página.
  2. Para posts cuya URL sigue el patrón `/posts/<post_id>`: solicitud a `/post/comments`.
  3. Extracción de texto de comentarios (soporta múltiples estructuras de respuesta JSON).
  4. Análisis de sentimientos y persistencia en `ScrapeResult` y CSV.
- **Individual:** `script_ind_fb.py` procesa un post mediante `post_url` y obtiene comentarios desde `/post/comments`.

---

## Análisis de Sentimientos

**Módulo:** `django_backend/scripts/sentiments/analizador.py`

La clase `get_data` actúa como cliente HTTP del modelo de análisis de sentimientos desplegado en Azure Databricks (MLflow Model Serving).

### Configuración

| Variable de entorno | Descripción |
|---|---|
| `URL_DATABRICKS` | URL del endpoint de serving del modelo |
| `DATABRICKS_TOKEN` | Token personal de acceso a Databricks |

### Uso

```python
from django_backend.scripts.sentiments.analizador import get_data

ai_service = get_data()
df = pd.DataFrame({"text": ["Texto del comentario"]})
resultado = ai_service.main(df)
```

### Estructura de la respuesta del modelo

```json
{
  "predictions": [
    {
      "sentimiento_global": "Positivo",
      "detalles_petalos": [
        {
          "Alegría": 0.85,
          "Confianza": 0.60,
          "Miedo": 0.05,
          "Sorpresa": 0.10,
          "Tristeza": 0.02,
          "Aversión": 0.01,
          "Ira": 0.01,
          "Anticipación": 0.30
        }
      ]
    }
  ]
}
```

Los pesos por emoción se **acumulan** (suma) a través de todos los textos analizados de un mismo post/perfil y se almacenan en los campos correspondientes de `ScrapeResult`.

---

## Variables de Entorno

Crear un archivo `.env` en la raíz del proyecto con las siguientes variables:

```env
# Django
SECRET_KEY=<clave-secreta-django-min-50-chars>
JWT_SIGNING_KEY=<clave-para-firmar-jwt>          # Opcional, usa SECRET_KEY si no se define
ALLOWED_HOSTS=localhost,127.0.0.1,tu-dominio.com
DEBUG=False

# Base de datos (solo en Azure App Service se usa /home/data/)
# En local, se usa BASE_DIR/db.sqlite3 automáticamente

# Análisis de sentimientos (Databricks)
URL_DATABRICKS=https://<workspace>.azuredatabricks.net/serving-endpoints/<endpoint>/invocations
DATABRICKS_TOKEN=<personal-access-token>

# YouTube Data API v3
KEY=<google-api-key>

# Email (opcional)
DEFAULT_FROM_EMAIL=no-reply@scraper.local
EMAIL_BACKEND=django.core.mail.backends.console.EmailBackend
EMAIL_HOST=smtp.gmail.com
EMAIL_PORT=587
EMAIL_HOST_USER=
EMAIL_HOST_PASSWORD=
EMAIL_USE_TLS=True
```

---

## Instalación y Ejecución Local

### 1. Clonar y crear entorno virtual

```powershell
git clone <repo>
cd multi-scrapper-media-pro-hn-bi-v2

python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 2. Instalar dependencias

```powershell
pip install -r requirements.txt
```

### 3. Configurar variables de entorno

Crear el archivo `.env` con los valores descritos en la sección anterior.

### 4. Aplicar migraciones

```powershell
& ".venv\Scripts\python.exe" manage.py migrate
```

### Registrar una key en `ScraperKey`

Desde PowerShell, registra temporalmente la key y elimínala de la sesión al terminar:

```powershell
$env:RAPIDAPI_KEY = "TU_KEY"
& ".venv\Scripts\python.exe" manage.py shell -c "import os; from django_backend.models import ScraperKey; ScraperKey.objects.filter(platform='ig').update(is_active=False); ScraperKey.objects.create(platform='ig', purpose='general', key_value=os.environ['RAPIDAPI_KEY'], is_active=True)"
Remove-Item Env:RAPIDAPI_KEY
```

Repite el proceso cambiando `platform` y `purpose` para `tk`, `x` o `fb`. YouTube usa la variable `KEY` o una key `platform='yt'`.

### 5. Crear superusuario

```powershell
& ".venv\Scripts\python.exe" manage.py createsuperuser
```

### 6. Crear grupos de roles (post_migrate)

Los grupos se crean mediante la señal `post_migrate`. Para crearlos manualmente desde el shell:

```powershell
& ".venv\Scripts\python.exe" manage.py shell
```

```python
from django.contrib.auth.models import Group
for name in ['Admin_Scraper', 'Director', 'Gerente', 'Usuario', 'Colaborador']:
    Group.objects.get_or_create(name=name)
```

### 7. Iniciar el servidor de desarrollo

```powershell
& ".venv\Scripts\python.exe" manage.py runserver
```

La API queda disponible en `http://127.0.0.1:8000/`.

### Probar desde PowerShell

```powershell
$token = "TU_ACCESS_TOKEN"
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/scraper/latest_results/?limit=10" `
  -Headers @{ Authorization = "Bearer $token" } | ConvertTo-Json -Depth 10
```

Para URLs con parámetros, usa `[uri]::EscapeDataString(...)` antes de construir la URL, especialmente en Facebook porque sus URLs pueden contener `&`.

Ejemplos de posts individuales:

```text
/api/scraper/instagram-post/?post_url=URL_DEL_POST
/api/scraper/tiktok-post/?videoId=VIDEO_ID
/api/scraper/x-post/?tweet_id=TWEET_ID
/api/scraper/facebook-post/?post_url=URL_DEL_POST
/api/scraper/youtube-post/?video_id=VIDEO_ID
```

---

## Despliegue en Azure

El proyecto detecta automáticamente si corre en Azure App Service verificando la existencia de `/home/site`:

```python
IF_AZURE = os.path.exists('/home/site')
```

En ese caso, la base de datos SQLite se persiste en `/home/data/db.sqlite3` (almacenamiento persistente de App Service).

### Configuración recomendada en App Service

- **Runtime:** Python 3.11
- **Startup command:** `gunicorn core.wsgi:application --bind 0.0.0.0:8000`
- **Variables de aplicación:** Configurar todas las variables del `.env` en **Configuration > Application Settings**.

### Archivos relevantes

- `databricks.yml` — Configuración del job en Azure Databricks para el modelo de sentimientos.
- `core/wsgi.py` — Punto de entrada WSGI para el servidor de producción.

---

## Herramientas CLI incluidas

### Consulta histórica

```powershell
& ".venv\Scripts\python.exe" -c "
import django, os
os.environ['DJANGO_SETTINGS_MODULE'] = 'core.settings'
django.setup()
from django_backend.scripts.script_historico import mostrar_historico
mostrar_historico('nombre_usuario')
"
```

### Dashboard de métricas en consola

```powershell
& ".venv\Scripts\python.exe" django_backend/scripts/script_metricas.py
```

---

## Consideraciones de Seguridad

- Las claves de API se cifran con **Fernet (AES-128-CBC + HMAC)** antes de almacenarse. La clave maestra nunca se guarda en base de datos.
- Los tokens JWT se firman con `SIGNING_KEY` configurable (HS256). Los refresh tokens rotan y se invalidan tras cada uso.
- El login Azure AD valida la firma del token contra las llaves públicas JWKS del tenant antes de crear sesión.
- `SerializerKey` nunca expone `key_value` en las respuestas API (el serializador solo incluye `platform`, `purpose`, `is_active`, `last_used`).
- `CORS_ALLOW_ALL_ORIGINS = True` está habilitado; restringir en producción configurando `CORS_ALLOWED_ORIGINS`.
- `DEBUG = False` hardcodeado en `settings.py`.