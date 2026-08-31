import requests
import csv
import json
import os
import re
from datetime import datetime
# Importación del modelo de Django
from django_backend.models import ScrapeResult
from django.utils.timezone import make_aware 

ARCHIVO_IDS = "usuarios_tiktok_registrados.json"
HOY = datetime.now().strftime("%Y_%m_%d")

def extraer_hashtags(texto):
    """
    Extrae todos los hashtags de un texto.
    Retorna una lista de hashtags sin el símbolo #
    """
    if not texto:
        return []
    # Busca todas las palabras que comienzan con #
    hashtags = re.findall(r'#\w+', texto)
    # Remueve el # y devuelve la lista
    return [tag.lstrip('#') for tag in hashtags]

def guardar_en_db(target, seguidores, fecha_str, likes, comentarios, vistas, desc, hashtags=None):
    try:
        if hashtags is None:
            hashtags = []
        
        hashtags_str = ",".join(hashtags) if hashtags else ""
        
        fecha_dt = None
        if fecha_str and fecha_str != "N/A":
            try:
                naive_datetime = datetime.strptime(fecha_str, '%d/%m/%Y %H:%M:%S')
                fecha_dt = make_aware(naive_datetime)
            except Exception as e_fecha:
                print(f"Error parseando fecha {fecha_str}: {e_fecha}")

        ScrapeResult.objects.create(
            platform='tk',
            username=target,
            followers=seguidores if isinstance(seguidores, int) else 0,
            post_date=fecha_dt,
            likes=likes,
            comments=comentarios,
            views=vistas,
            description=desc,
            hashtags=hashtags_str
        )
    except Exception as e:
        print(f"Error crítico al guardar en DB (TikTok): {e}")


def cargar_cache_ids():
    if os.path.exists(ARCHIVO_IDS):
        with open(ARCHIVO_IDS, 'r') as f:
            return json.load(f)
    return {}

def guardar_cache_ids(cache):
    with open(ARCHIVO_IDS, 'w') as f:
        json.dump(cache, f, indent=4)

def analizar_tiktok_optimizado(keys_search, keys_posts, lista_targets):
    cache_uids = cargar_cache_ids()
    nombre_csv = f"results/datos_tk_{HOY}.csv"
    idx_s, idx_p = 0, 0

    if not os.path.exists('results'): os.makedirs('results')
    
    if not os.path.exists(nombre_csv):
        with open(nombre_csv, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(['USUARIO', 'SEGUIDORES', 'CORAZONES_TOTALES', 'FECHA_POST', 'LIKES_VIDEO', 'VISTAS', 'DESCRIPCION', 'HASHTAGS'])

    for target in lista_targets:
        info_perfil = cache_uids.get(target)
        
        # 1. Obtener ID del usuario si no está en caché
        if not info_perfil:
            while idx_s < len(keys_search):
                headers = {
                    "x-rapidapi-key": keys_search[idx_s], 
                    "x-rapidapi-host": "tiktok-api23.p.rapidapi.com"
                }
                try:
                    res = requests.get("https://tiktok-api23.p.rapidapi.com/api/user/info", 
                                       headers=headers, params={"uniqueId": target}, timeout=10)
                    res_json = res.json()
                    if res.status_code == 200:
                        user_info = res_json.get('userInfo', {})
                        if user_info:
                            stats = user_info.get('stats', {})
                            info_perfil = {
                                "secUid": user_info.get('user', {}).get('id'),
                                "followers": stats.get('followerCount', 0),
                                "hearts": stats.get('heart', 0)
                            }
                            cache_uids[target] = info_perfil
                            guardar_cache_ids(cache_uids)
                            break
                    idx_s += 1
                except:
                    idx_s += 1

        if info_perfil:
            sec_uid = info_perfil.get("secUid")
            seguidores = info_perfil.get("followers")
            corazones = info_perfil.get("hearts")

            exito_posts = False
            while idx_p < len(keys_posts) and not exito_posts:
                headers_p = {
                    "x-rapidapi-key": keys_posts[idx_p], 
                    "x-rapidapi-host": "tiktok-scraper7.p.rapidapi.com"
                }
                try:
                    res_p = requests.get("https://tiktok-scraper7.p.rapidapi.com/user/posts", 
                                         headers=headers_p, params={"user_id": sec_uid, "count": "35"}, timeout=15)
                    if res_p.status_code == 200:
                        items = res_p.json().get('data', {}).get('videos', [])
                        if items:
                            with open(nombre_csv, mode='a', newline='', encoding='utf-8-sig') as f:
                                writer = csv.writer(f)
                                for item in items:
                                    likes = item.get('digg_count', 0)
                                    comentarios = item.get('comment_count', 0)
                                    vistas = item.get('play_count', 0)
                                    ts = item.get('create_time')
                                    fecha_txt = datetime.fromtimestamp(int(ts)).strftime('%d/%m/%Y %H:%M:%S') if ts else "N/A"
                                    descripcion = item.get('title', '').replace('\n', ' ')
                                    
                                    # Extraer hashtags
                                    hashtags = extraer_hashtags(descripcion)
                                    hashtags_csv = ",".join(hashtags)
                                    
                                    # --- GUARDADO DOBLE ---
                                    # CSV
                                    writer.writerow([target, seguidores, corazones, fecha_txt, likes, vistas, descripcion, hashtags_csv])
                                    # Base de Datos Django
                                    print(f"Datos {target}, {seguidores} {fecha_txt}, {likes}, {vistas}, {descripcion} | Hashtags: {hashtags}")
                                    guardar_en_db(target, seguidores, fecha_txt, likes, comentarios, vistas, descripcion, hashtags=hashtags)
                            
                            print(f"  @{target} procesado y sincronizado con Django.")
                        exito_posts = True
                    else:
                        idx_p += 1
                except:
                    idx_p += 1

def iniciar(keys_de_100, keys_de_300, lista_perfiles):
    print(f"\n--- INICIANDO MÓDULO TIKTOK (DB CONNECTED) ---")
    analizar_tiktok_optimizado(keys_de_100, keys_de_300, lista_perfiles)