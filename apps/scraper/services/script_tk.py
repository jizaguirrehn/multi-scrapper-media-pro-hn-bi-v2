import csv
import json
import logging
import os
import pandas as pd
import requests
from datetime import datetime

# Importación del modelo de Django y utilidades
from django.utils.timezone import make_aware
from apps.scraper.models import ScrapeResult
from apps.scraper.services.sentiments.analizador import get_data

# Instancia del logger vinculada al contexto/módulo de Django
logger = logging.getLogger(__name__)

ARCHIVO_IDS = "usuarios_tiktok_registrados.json"
HOY = datetime.now().strftime("%Y_%m_%d")


def _obtener_comentarios_post(video_id, keys_posts, key_index):
    """Llama a /comment/list?url=<video_id> y retorna lista de textos de comentarios."""
    url = "https://tiktok-scraper7.p.rapidapi.com/comment/list"
    host = "tiktok-scraper7.p.rapidapi.com"
    idx = key_index

    while idx < len(keys_posts):
        headers = {
            "x-rapidapi-key": keys_posts[idx],
            "x-rapidapi-host": host,
            "Content-Type": "application/json"
        }
        try:
            response = requests.get(
                url,
                headers=headers,
                params={"url": str(video_id), "count": "25", "cursor": "0"},
                timeout=30
            )
            if response.status_code == 429:
                logger.warning(
                    "Rate limit (429) alcanzado al obtener comentarios de TikTok. Rotando API key.",
                    extra={"video_id": video_id, "key_index_fallida": idx}
                )
                idx += 1
                continue

            if response.status_code == 200:
                comentarios = response.json().get('data', {}).get('comments', []) or []
                textos = [
                    c.get('text', '').strip()
                    for c in comentarios
                    if c.get('text', '').strip()
                ]
                logger.info(
                    "Comentarios de TikTok obtenidos exitosamente",
                    extra={"video_id": video_id, "total_comentarios": len(textos)}
                )
                return textos

            logger.error(
                "Error HTTP al solicitar comentarios de TikTok",
                extra={"video_id": video_id, "status_code": response.status_code, "response": response.text}
            )
            break

        except Exception:
            logger.exception("Excepción obteniendo comentarios TK", extra={"video_id": video_id})
            idx += 1

    return []


def _analizar_sentimiento(textos):
    """Analiza lista de textos con el modelo Databricks. Retorna pesos y sentimiento global."""
    pesos = {
        'alegria': 0.0, 'confianza': 0.0, 'miedo': 0.0, 'sorpresa': 0.0,
        'tristeza': 0.0, 'aversion': 0.0, 'ira': 0.0, 'anticipacion': 0.0
    }
    sentimiento_global = 'N/A'

    if not textos:
        logger.debug("No hay comentarios para analizar sentimiento (TikTok).")
        return pesos, sentimiento_global

    try:
        ai_service = get_data()
        df_textos = pd.DataFrame(textos, columns=['text'])
        resultado = ai_service.main(df_textos)

        if resultado and 'predictions' in resultado:
            lista_emociones = [p['detalles_petalos'][0] for p in resultado['predictions']]
            df_preds = pd.DataFrame(lista_emociones)
            df_numeric = df_preds.apply(pd.to_numeric, errors='coerce')
            sumas = df_numeric.sum()

            pesos['alegria']      = float(sumas.get('Alegría', 0.0))
            pesos['confianza']    = float(sumas.get('Confianza', 0.0))
            pesos['miedo']        = float(sumas.get('Miedo', 0.0))
            pesos['sorpresa']     = float(sumas.get('Sorpresa', 0.0))
            pesos['tristeza']     = float(sumas.get('Tristeza', 0.0))
            pesos['aversion']     = float(sumas.get('Aversión', 0.0))
            pesos['ira']          = float(sumas.get('Ira', 0.0))
            pesos['anticipacion'] = float(sumas.get('Anticipación', 0.0))

            sentimiento_global = resultado['predictions'][0].get('sentimiento_global', 'N/A')
            logger.info("Análisis de sentimiento (TikTok) finalizado", extra={"sentimiento_global": sentimiento_global})

    except Exception:
        logger.exception("Error en análisis de sentimiento (TikTok)")

    return pesos, sentimiento_global


def guardar_en_db(target, seguidores, fecha_str, likes, comentarios, vistas, desc, pesos=None, sentimiento_global='N/A'):
    """Recibe los datos del scrapeo de TikTok y los almacena en la base de datos Django."""
    try:
        fecha_dt = None
        if fecha_str and fecha_str != "N/A":
            try:
                naive_datetime = datetime.strptime(fecha_str, '%d/%m/%Y %H:%M:%S')
                fecha_dt = make_aware(naive_datetime)
            except Exception:
                logger.exception("Error parseando fecha recibida de TikTok", extra={"fecha_str": fecha_str, "target": target})

        p = pesos or {}
        obj = ScrapeResult.objects.create(
            platform='tk',
            username=target,
            followers=seguidores if isinstance(seguidores, int) else 0,
            post_date=fecha_dt,
            likes=likes,
            comments=comentarios,
            views=vistas,
            description=desc,
            sentimiento_global=sentimiento_global,
            alegria=p.get('alegria', 0.0),
            confianza=p.get('confianza', 0.0),
            miedo=p.get('miedo', 0.0),
            sorpresa=p.get('sorpresa', 0.0),
            tristeza=p.get('tristeza', 0.0),
            aversion=p.get('aversion', 0.0),
            ira=p.get('ira', 0.0),
            anticipacion=p.get('anticipacion', 0.0),
        )
        logger.info(
            "Registro insertado en DB exitosamente (TikTok)",
            extra={"target": target, "scrape_result_id": obj.id, "sentimiento": sentimiento_global}
        )
    except Exception:
        logger.exception("Error crítico al guardar en DB (TikTok)", extra={"target": target})


def cargar_cache_ids():
    """Carga los IDs guardados localmente."""
    if os.path.exists(ARCHIVO_IDS):
        try:
            with open(ARCHIVO_IDS, 'r') as f:
                return json.load(f)
        except Exception:
            logger.exception("Error leyendo archivo de cache de IDs")
    return {}


def guardar_cache_ids(cache):
    """Persiste en disco los datos de caché de usuarios."""
    try:
        with open(ARCHIVO_IDS, 'w') as f:
            json.dump(cache, f, indent=4)
        logger.debug("Cache de IDs de TikTok actualizado correctamente.")
    except Exception:
        logger.exception("Error guardando cache de IDs de TikTok")


def _obtener_info_perfil(target, keys_search, idx_s, cache_uids):
    """Consulta la API de usuario TikTok para obtener secUid, followers y hearts. Retorna (info_perfil, nuevo_idx_s)."""
    while idx_s < len(keys_search):
        headers = {
            "x-rapidapi-key": keys_search[idx_s],
            "x-rapidapi-host": "tiktok-api23.p.rapidapi.com"
        }
        try:
            res = requests.get(
                "https://tiktok-api23.p.rapidapi.com/api/user/info",
                headers=headers, params={"uniqueId": target}, timeout=40
            )
            if res.status_code == 200:
                user_info = res.json().get('userInfo', {})
                if user_info:
                    stats = user_info.get('stats', {})
                    info = {
                        "secUid": user_info.get('user', {}).get('id'),
                        "followers": stats.get('followerCount', 0),
                        "hearts": stats.get('heart', 0)
                    }
                    cache_uids[target] = info
                    guardar_cache_ids(cache_uids)
                    logger.info("Información de perfil obtenida y cacheada", extra={"target": target, "followers": info["followers"]})
                    return info, idx_s

            logger.warning(
                "Respuesta no exitosa al consultar perfil TK",
                extra={"target": target, "status_code": res.status_code, "key_index": idx_s}
            )
            idx_s += 1
        except Exception:
            logger.exception("Error obteniendo perfil TK", extra={"target": target, "key_index": idx_s})
            idx_s += 1

    return None, idx_s


def _procesar_video(item, target, seguidores, corazones, keys_posts, idx_p, writer):
    """Obtiene comentarios, analiza sentimiento y guarda un video en CSV y DB."""
    likes = item.get('digg_count', 0)
    comentarios = item.get('comment_count', 0)
    vistas = item.get('play_count', 0)
    ts = item.get('create_time')
    fecha_txt = datetime.fromtimestamp(int(ts)).strftime('%d/%m/%Y %H:%M:%S') if ts else "N/A"
    descripcion = item.get('title', '').replace('\n', ' ')

    raw_id = item.get('video_id') or ''
    video_id = str(raw_id) if str(raw_id).isdigit() else None

    if not video_id:
        logger.warning("ID de video no numérico. Se omiten comentarios.", extra={"raw_id": raw_id, "target": target})

    textos = _obtener_comentarios_post(video_id, keys_posts, idx_p) if video_id else []
    pesos, sentimiento_global = _analizar_sentimiento(textos)

    logger.info(
        "Procesando métricas de Video TikTok",
        extra={"video_id": video_id, "sentimiento": sentimiento_global, "target": target}
    )

    writer.writerow([target, seguidores, corazones, fecha_txt, likes, vistas, descripcion])
    guardar_en_db(
        target, seguidores, fecha_txt, likes, comentarios, vistas, descripcion,
        pesos=pesos, sentimiento_global=sentimiento_global
    )


def _obtener_y_procesar_posts(target, sec_uid, seguidores, corazones, keys_posts, idx_p, nombre_csv):
    """Descarga los posts del usuario y procesa cada video. Retorna (exito, nuevo_idx_p)."""
    while idx_p < len(keys_posts):
        headers_p = {
            "x-rapidapi-key": keys_posts[idx_p],
            "x-rapidapi-host": "tiktok-scraper7.p.rapidapi.com"
        }
        try:
            res_p = requests.get(
                "https://tiktok-scraper7.p.rapidapi.com/user/posts",
                headers=headers_p, params={"user_id": sec_uid, "count": "35"}, timeout=30
            )
            if res_p.status_code == 200:
                items = res_p.json().get('data', {}).get('videos', [])
                if items:
                    with open(nombre_csv, mode='a', newline='', encoding='utf-8-sig') as f:
                        writer = csv.writer(f)
                        for item in items:
                            _procesar_video(item, target, seguidores, corazones, keys_posts, idx_p, writer)
                    logger.info("Posts de perfil procesados y sincronizados", extra={"target": target, "total_videos": len(items)})
                else:
                    logger.warning("El perfil no contiene videos o la respuesta retornó lista vacía", extra={"target": target})
                return True, idx_p

            logger.warning(
                "Status HTTP no exitoso obteniendo posts de TikTok",
                extra={"target": target, "status_code": res_p.status_code, "key_index": idx_p}
            )
            idx_p += 1
        except Exception:
            logger.exception("Error obteniendo posts TK", extra={"target": target, "key_index": idx_p})
            idx_p += 1

    return False, idx_p


def analizar_tiktok_optimizado(keys_search, keys_posts, lista_targets):
    """Coordina la búsqueda, rotación de keys y extracción de datos."""
    cache_uids = cargar_cache_ids()
    nombre_csv = f"results/datos_tk_{HOY}.csv"
    idx_s, idx_p = 0, 0

    if not os.path.exists('results'):
        os.makedirs('results')

    if not os.path.exists(nombre_csv):
        with open(nombre_csv, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(['USUARIO', 'SEGUIDORES', 'CORAZONES_TOTALES', 'FECHA_POST', 'LIKES_VIDEO', 'VISTAS', 'DESCRIPCION'])

    for target in lista_targets:
        logger.info("Procesando objetivo TikTok", extra={"target": target})
        info_perfil = cache_uids.get(target)

        if not info_perfil:
            info_perfil, idx_s = _obtener_info_perfil(target, keys_search, idx_s, cache_uids)

        if info_perfil:
            sec_uid = info_perfil.get("secUid")
            seguidores = info_perfil.get("followers")
            corazones = info_perfil.get("hearts")
            exito, idx_p = _obtener_y_procesar_posts(
                target, sec_uid, seguidores, corazones, keys_posts, idx_p, nombre_csv
            )
            if not exito:
                logger.error("No se pudieron procesar los posts del objetivo", extra={"target": target})
        else:
            logger.error("No fue posible resolver la información del perfil del objetivo", extra={"target": target})

        if idx_s >= len(keys_search) or idx_p >= len(keys_posts):
            logger.critical("Se agotaron las llaves API de búsqueda o contenido de TikTok. Deteniendo proceso.")
            break


def iniciar(keys_de_100, keys_de_300, lista_perfiles):
    """Punto de entrada de ejecución del módulo de TikTok."""
    logger.info(
        "Iniciando módulo de extracción TikTok",
        extra={"total_search_keys": len(keys_de_100), "total_posts_keys": len(keys_de_300), "total_perfiles": len(lista_perfiles)}
    )
    analizar_tiktok_optimizado(keys_de_100, keys_de_300, lista_perfiles)
    logger.info("Módulo TikTok finalizado.")