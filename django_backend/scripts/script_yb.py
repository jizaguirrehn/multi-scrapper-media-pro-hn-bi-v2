import os
import pandas as pd
import logging
import re
from googleapiclient.discovery import build
from .sentiments.analizador import get_data 
from dotenv import load_dotenv

import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'django_backend.settings')
django.setup()
from django_backend.models import ScrapeResult

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_KEY = os.environ.get("KEY")
youtube = build('youtube', 'v3', developerKey=API_KEY)

def extraer_hashtags(texto):
    """
    Extrae todos los hashtags de un texto.
    Retorna una lista de hashtags sin el símbolo #
    """
    if not texto:
        return []

    hashtags = re.findall(r'#\w+', texto)
    return [tag.lstrip('#') for tag in hashtags]

def obtener_datos_completos(nombre_canal):
    ai_service = get_data()

    search_canal = youtube.search().list(q=nombre_canal, type="channel", part="snippet", maxResults=1).execute()
    if not search_canal['items']: 
        logger.error("Canal no encontrado.")
        return
    
    channel_id = search_canal['items'][0]['id']['channelId']
    channel_title = search_canal['items'][0]['snippet']['title']
    logger.info(f"Analizando canal: {channel_title}\n")

    stats_canal = youtube.channels().list(part="statistics", id=channel_id).execute()
    total_subs = int(stats_canal['items'][0]['statistics'].get('subscriberCount', 0))

    search_videos = youtube.search().list(channelId=channel_id, part="id,snippet", order="date", type="video", maxResults=25).execute()
    ids_videos = [item['id']['videoId'] for item in search_videos['items']]

    stats_videos = youtube.videos().list(part="statistics,snippet", id=",".join(ids_videos)).execute()

    for v_item in stats_videos['items']:
        v_id = v_item['id']
        titulo = v_item['snippet']['title']
        vistas = int(v_item['statistics'].get('viewCount', 0))
        likes = int(v_item['statistics'].get('likeCount', 0))
        cantidad_comentarios = int(v_item['statistics'].get('commentCount', 0))
        descripcion = v_item['snippet'].get('description', 'Sin descripción')
        fecha_pub = v_item['snippet'].get('publishedAt')

        hashtags = extraer_hashtags(descripcion)
        hashtags_str = ",".join(hashtags)

        logger.info(f"Hashtags encontrados: {hashtags}")

        logger.info(f"Procesando Video: {titulo}")

        pesos = {
            'alegria': 0.0, 'confianza': 0.0, 'miedo': 0.0, 'sorpresa': 0.0,
            'tristeza': 0.0, 'aversion': 0.0, 'ira': 0.0, 'anticipacion': 0.0
        }
        sentimiento_global = "N/A"

        try:
            comentarios_req = youtube.commentThreads().list(
                part="snippet", videoId=v_id, maxResults=50, textFormat="plainText"
            ).execute()

            comentarios_raw = comentarios_req.get('items', [])
            
            if comentarios_raw:
                textos = [c['snippet']['topLevelComment']['snippet']['textDisplay'] for c in comentarios_raw]
                df_comentarios = pd.DataFrame(textos, columns=['text'])

                resultado = ai_service.main(df_comentarios)

                if resultado and 'predictions' in resultado:
                    lista_emociones = [p['detalles_petalos'][0] for p in resultado['predictions']]
                    df_preds = pd.DataFrame(lista_emociones)
                    df_numeric = df_preds.apply(pd.to_numeric, errors='coerce')
                    
                    sumas = df_numeric.sum()
                    
                    pesos['alegria'] = sumas.get('Alegría', 0.0)
                    pesos['confianza'] = sumas.get('Confianza', 0.0)
                    pesos['miedo'] = sumas.get('Miedo', 0.0)
                    pesos['sorpresa'] = sumas.get('Sorpresa', 0.0)
                    pesos['tristeza'] = sumas.get('Tristeza', 0.0)
                    pesos['aversion'] = sumas.get('Aversión', 0.0)
                    pesos['ira'] = sumas.get('Ira', 0.0)
                    pesos['anticipacion'] = sumas.get('Anticipación', 0.0)
                    
                    sentimiento_global = resultado['predictions'][0].get('sentimiento_global', 'N/A')

            ScrapeResult.objects.create(
                platform    = "yt",
                username    = channel_title,
                followers   = total_subs,
                post_date   = fecha_pub,
                likes       = likes,
                comments    = cantidad_comentarios,
                views       = vistas,
                description = descripcion[:5000],
                hashtags=hashtags_str,
                
                sentimiento_global = sentimiento_global,
                alegria     = pesos['alegria'],
                confianza   = pesos['confianza'],
                miedo       = pesos['miedo'],
                sorpresa    = pesos['sorpresa'],
                tristeza    = pesos['tristeza'],
                aversion    = pesos['aversion'],
                ira         = pesos['ira'],
                anticipacion = pesos['anticipacion']
            )
            logger.info(f"DB ✔ Guardado exitoso: {titulo[:30]}...")

        except Exception as e:
            logger.error(f"Error procesando video {v_id}: {e}")
        
        print("-" * 60)

def iniciar_yt(target):
    obtener_datos_completos(target)