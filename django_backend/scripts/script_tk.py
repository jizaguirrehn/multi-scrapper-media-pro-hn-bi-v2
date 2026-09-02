import requests
import csv
import json
import os
import re
from datetime import datetime
import pandas as pd
# Importación del modelo de Django
from django_backend.models import ScrapeResult, PostComment, obtener_o_actualizar_usuario
from django.utils.timezone import make_aware 
from .sentiments.analizador import get_data

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


def _obtener_comentarios_video(target, video_id, headers):
    """Obtiene comentarios de un video de TikTok usando video_id."""
    if not video_id:
        return []

    url = "https://tiktok-scraper7.p.rapidapi.com/comment/list"
    comentarios = []

    try:
        video_url = f"https://www.tiktok.com/@{target}/video/{video_id}"
        response = requests.get(
            url,
            headers=headers,
            params={"url": video_url, "count": "10", "cursor": "0"},
            timeout=15,
        )

        if response.status_code != 200:
            print(f"  [TikTok] Error en comentarios para video_id={video_id}: {response.status_code}")
            return comentarios

        data = response.json()
        items = data.get("comments") or data.get("data", {}).get("comments") or []

        if isinstance(items, dict):
            items = items.get("comments") or []

        for item in items:
            if not isinstance(item, dict):
                continue
            texto = (
                item.get("text")
                or item.get("comment")
                or item.get("content")
                or item.get("message")
                or ""
            )
            if texto and str(texto).strip():
                comentarios.append(str(texto).strip())

        return comentarios
    except Exception as e:
        print(f"  [TikTok] Error obteniendo comentarios para video_id={video_id}: {e}")
        return comentarios


def analizar_sentimiento(comentarios):
    """
    Procesa la lista de comentarios para determinar métricas de sentimiento.
    Usa el modelo de Databricks igual que en YouTube.
    """
    pesos = {
        "alegria": 0.0, "confianza": 0.0, "miedo": 0.0, "sorpresa": 0.0,
        "tristeza": 0.0, "aversion": 0.0, "ira": 0.0, "anticipacion": 0.0,
    }
    sentimiento_global = "N/A"

    if not comentarios:
        return pesos, sentimiento_global

    try:
        ai_service = get_data()
        df_comentarios = pd.DataFrame({'text': [str(comentario) for comentario in comentarios]})
        resultado = ai_service.main(df_comentarios)

        if resultado and 'predictions' in resultado:
            lista_emociones = [p['detalles_petalos'][0] for p in resultado['predictions']]
            if lista_emociones:
                df_preds = pd.DataFrame(lista_emociones)
                df_numeric = df_preds.apply(pd.to_numeric, errors='coerce')
                sumas = df_numeric.sum()

                pesos['alegria'] = float(sumas.get('Alegría', 0.0))
                pesos['confianza'] = float(sumas.get('Confianza', 0.0))
                pesos['miedo'] = float(sumas.get('Miedo', 0.0))
                pesos['sorpresa'] = float(sumas.get('Sorpresa', 0.0))
                pesos['tristeza'] = float(sumas.get('Tristeza', 0.0))
                pesos['aversion'] = float(sumas.get('Aversión', 0.0))
                pesos['ira'] = float(sumas.get('Ira', 0.0))
                pesos['anticipacion'] = float(sumas.get('Anticipación', 0.0))
                sentimiento_global = resultado['predictions'][0].get('sentimiento_global', 'N/A')
                return pesos, sentimiento_global
    except Exception as e:
        print(f"Error usando Databricks para sentimiento TikTok: {e}")

    pesos["alegria"] = round(len(comentarios) * 0.5, 2)
    sentimiento_global = "Positivo" if len(comentarios) > 0 else "Neutral"
    return pesos, sentimiento_global


def guardar_en_db(target, seguidores, fecha_str, likes, comentarios, vistas, desc, hashtags=None, pesos=None, sentimiento_global='N/A'):
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

        dim_usuario = obtener_o_actualizar_usuario('tk', target, seguidores)
        scrape_result = ScrapeResult.objects.create(
            platform='tk',
            username=target,
            dim_usuario=dim_usuario,
            followers=seguidores if isinstance(seguidores, int) else 0,
            post_date=fecha_dt,
            likes=likes,
            comments=comentarios,
            views=vistas,
            description=desc,
            hashtags=hashtags_str,
            sentimiento_global=sentimiento_global,
            alegria=float(pesos.get('alegria', 0.0)) if pesos else 0.0,
            confianza=float(pesos.get('confianza', 0.0)) if pesos else 0.0,
            miedo=float(pesos.get('miedo', 0.0)) if pesos else 0.0,
            sorpresa=float(pesos.get('sorpresa', 0.0)) if pesos else 0.0,
            tristeza=float(pesos.get('tristeza', 0.0)) if pesos else 0.0,
            aversion=float(pesos.get('aversion', 0.0)) if pesos else 0.0,
            ira=float(pesos.get('ira', 0.0)) if pesos else 0.0,
            anticipacion=float(pesos.get('anticipacion', 0.0)) if pesos else 0.0,
        )
        return scrape_result
    except Exception as e:
        print(f"Error crítico al guardar en DB (TikTok): {e}")
        return None


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
                                    # Filtrar posts anclados
                                    is_top = item.get('is_top', 0)
                                    if is_top == 1:
                                        print(f"   [ANCLADO] Video saltado (anclado al perfil)")
                                        continue

                                    likes = item.get('digg_count', 0)
                                    comentarios_count = item.get('comment_count', 0)
                                    vistas = item.get('play_count', 0)
                                    ts = item.get('create_time')
                                    fecha_txt = datetime.fromtimestamp(int(ts)).strftime('%d/%m/%Y %H:%M:%S') if ts else "N/A"
                                    descripcion = item.get('title', '').replace('\n', ' ')
                                    video_id = item.get('video_id') or item.get('id')
                                    
                                    # Extraer hashtags
                                    hashtags = extraer_hashtags(descripcion)
                                    hashtags_csv = ",".join(hashtags)

                                    # Analizar comentarios reales del video cuando estén disponibles
                                    comentarios_post = _obtener_comentarios_video(target, video_id, headers_p)
                                    texto_analisis = comentarios_post if comentarios_post else ([descripcion] if descripcion else [])
                                    pesos, sentimiento_global = analizar_sentimiento(texto_analisis)

                                    # --- GUARDADO DOBLE ---
                                    # CSV
                                    writer.writerow([target, seguidores, corazones, fecha_txt, likes, vistas, descripcion, hashtags_csv])
                                    # Base de Datos Django
                                    print(f"Datos {target}, {seguidores} {fecha_txt}, {likes}, {vistas}, {descripcion} | Sentimiento: {sentimiento_global} | Hashtags: {hashtags}")
                                    scrape_result = guardar_en_db(
                                        target,
                                        seguidores,
                                        fecha_txt,
                                        likes,
                                        comentarios_count,
                                        vistas,
                                        descripcion,
                                        hashtags=hashtags,
                                        pesos=pesos,
                                        sentimiento_global=sentimiento_global,
                                    )

                                    if scrape_result and comentarios_post:
                                        for comentario_texto in comentarios_post:
                                            try:
                                                PostComment.objects.create(
                                                    post=scrape_result,
                                                    texto=comentario_texto,
                                                    platform='tk'
                                                )
                                            except Exception as e:
                                                print(f"Error guardando comentario en DB (TikTok): {e}")
                                        print(f"  [DB Django] {len(comentarios_post)} comentarios guardados para el video {video_id}")
                            
                            print(f"  @{target} procesado y sincronizado con Django.")
                        exito_posts = True
                    else:
                        idx_p += 1
                except:
                    idx_p += 1

def iniciar(keys_de_100, keys_de_300, lista_perfiles):
    print(f"\n--- INICIANDO MÓDULO TIKTOK (DB CONNECTED) ---")
    analizar_tiktok_optimizado(keys_de_100, keys_de_300, lista_perfiles)