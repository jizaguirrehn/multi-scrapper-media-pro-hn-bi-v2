import requests
import csv
import json
import os
import re
from datetime import datetime
import pandas as pd

from django_backend.models import ScrapeResult, PostComment
from django.utils.timezone import make_aware
from .sentiments.analizador import get_data

ARCHIVO_IDS = "usuarios_fb_registrados.json"
HOY = datetime.now().strftime("%Y_%m_%d")


def guardar_en_db(target, seguidores, fecha_str, likes, comentarios, vistas, desc, hashtags=None, pesos=None, sentimiento_global='N/A'):
    try:
        if hashtags is None:
            hashtags = []

        hashtags_str = ",".join(hashtags) if hashtags else ""

        fecha_dt = None
        if fecha_str and fecha_str != "N/A":
            try:
                if isinstance(fecha_str, datetime):
                    fecha_dt = fecha_str if fecha_str.tzinfo else make_aware(fecha_str)
                else:
                    naive_datetime = datetime.strptime(fecha_str, '%d/%m/%Y %H:%M:%S')
                    fecha_dt = make_aware(naive_datetime)
            except Exception as e_fecha:
                print(f"Error parseando fecha {fecha_str}: {e_fecha}")

        scrape_result = ScrapeResult.objects.create(
            platform='fb',
            username=target,
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
        print(f"Error crítico al guardar en DB (Facebook): {e}")
        return None


def cargar_cache_ids():
    if os.path.exists(ARCHIVO_IDS):
        with open(ARCHIVO_IDS, 'r') as f:
            return json.load(f)
    return {}

def guardar_cache_ids(cache):
    with open(ARCHIVO_IDS, 'w') as f:
        json.dump(cache, f, indent=4)


def _obtener_comentarios_post(post_id, headers):
    """Obtiene comentarios del post de Facebook usando el post_id."""
    if not post_id:
        return [], headers

    url = "https://facebook-scraper3.p.rapidapi.com/post/comments"
    comentarios = []

    try:
        response = requests.get(url, headers=headers, params={"post_id": str(post_id)}, timeout=15)
        if response.status_code != 200:
            print(f"  [Facebook] Error en comentarios para post_id={post_id}: {response.status_code}")
            return comentarios, headers

        data = response.json()
        items = data.get("results") or data.get("comments") or data.get("data") or []

        if isinstance(items, dict):
            items = items.get("comments") or items.get("results") or []

        for item in items:
            if not isinstance(item, dict):
                continue
            texto = (
                item.get("text")
                or item.get("message")
                or item.get("comment")
                or item.get("content")
                or item.get("body")
                or ""
            )
            if texto and str(texto).strip():
                comentarios.append(str(texto).strip())

        return comentarios, headers
    except Exception as e:
        print(f"  [Facebook] Error obteniendo comentarios para post_id={post_id}: {e}")
        return comentarios, headers


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
        df_comentarios = pd.DataFrame(comentarios, columns=['text'])
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
        print(f"Error usando Databricks para sentimiento Facebook: {e}")

    # Fallback local
    pesos["alegria"] = round(len(comentarios) * 0.5, 2)
    sentimiento_global = "Positivo" if len(comentarios) > 0 else "Neutral"
    return pesos, sentimiento_global


def analizar_facebook_optimizado(api_key, lista_targets):
    cache_uids = cargar_cache_ids()
    nombre_csv = f"results/datos_fb_{HOY}.csv"

    if not os.path.exists('results'): os.makedirs('results')
    
    if not os.path.exists(nombre_csv):
        with open(nombre_csv, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow([
                'USUARIO',
                'SEGUIDORES',
                'FECHA_POST',
                'LIKES',
                'COMENTARIOS',
                'VISTAS',
                'DESCRIPCION',
                'SENTIMIENTO',
                'HASHTAGS'
            ])

    # Headers globales ya que solo usamos una key
    headers = {
        "x-rapidapi-key": api_key, 
        "x-rapidapi-host": "facebook-scraper3.p.rapidapi.com"
    }

    for target in lista_targets:
        info_perfil = cache_uids.get(target)
        
        # 1. Obtener ID del usuario si no está en caché
        if not info_perfil:
            print(f"Buscando ID para @{target}...")
            querystring = {"url": f"https://www.facebook.com/{target}"}
            try:
                res = requests.get("https://facebook-scraper3.p.rapidapi.com/page/details", 
                                   headers=headers, params=querystring, timeout=10)
                if res.status_code == 200:
                    user_info = res.json()
                    user_info = user_info.get("results", {})
                    info_perfil = {
                        "pageId": user_info.get('page_id'),
                        "followers": user_info.get('followers', 0)
                    }
                    cache_uids[target] = info_perfil
                    guardar_cache_ids(cache_uids)
                else:
                    print(f"Error obteniendo perfil: {res.status_code}")
                    continue
            except Exception as e:
                print(f"Error de conexión perfil: {e}")
                continue

        # 2. Obtener Posts con Cursor (7 iteraciones)
        if info_perfil:
            page_id = info_perfil.get("pageId")
            seguidores = info_perfil.get("followers")
            cursor = None
            
            
            for iteration in range(1, 8):
                params = {"page_id": page_id}
                if cursor: params["cursor"] = cursor

                try:
                    res_p = requests.get("https://facebook-scraper3.p.rapidapi.com/page/posts", 
                                        headers=headers, params=params, timeout=15)
                    
                    if res_p.status_code == 200:
                        data = res_p.json()
                        items = data.get('results', [])
                        cursor = data.get('cursor')
                        
                        if items:
                            with open(nombre_csv, mode='a', newline='', encoding='utf-8-sig') as f:
                                writer = csv.writer(f)
                                for item in items:
                                    likes = item.get('reactions_count', 0)
                                    coments = item.get('comments_count', 0)
                                    vistas = item.get('reshare_count', 0)
                                    ts = item.get('timestamp')
                                    fecha_txt = datetime.fromtimestamp(int(ts)).strftime('%d/%m/%Y %H:%M:%S') if ts else "N/A"
                                    desc = (item.get('message_rich') or item.get('message') or item.get('description') or '').replace('\n', ' ')
                                    post_id = item.get('post_id') or item.get('id')

                                    # Extraer hashtags
                                    hashtags = extraer_hashtags(desc)
                                    hashtags_csv = ",".join(hashtags)

                                    comentarios_post, _ = _obtener_comentarios_post(post_id, headers)
                                    pesos, sentimiento_global = analizar_sentimiento(comentarios_post)

                                    writer.writerow([
                                        target,
                                        seguidores,
                                        fecha_txt,
                                        likes,
                                        coments,
                                        vistas,
                                        desc,
                                        sentimiento_global,
                                        hashtags_csv
                                    ])

                                    print(
                                        f"Datos {target}, {seguidores}, {fecha_txt}, "
                                        f"{likes}, {coments}, {vistas}, {desc} | "
                                        f"Sentimiento: {sentimiento_global} | Hashtags: {hashtags}"
                                    )

                                    scrape_result = guardar_en_db(
                                        target,
                                        seguidores,
                                        fecha_txt,
                                        likes,
                                        coments,
                                        vistas,
                                        desc,
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
                                                    platform='fb'
                                                )
                                            except Exception as e:
                                                print(f"Error guardando comentario en DB (Facebook): {e}")
                                        print(f"  [DB Django] {len(comentarios_post)} comentarios guardados para el post {post_id}")
                        
                        
                        if not cursor:
                            print("  No hay más posts disponibles.")
                            break
                    else:
                        print(f"  Error en nivel {iteration}: {res_p.status_code}")
                        break # Si la key falla (429), detenemos el bucle
                except Exception as e:
                    print(f"  Error de conexión en nivel {iteration}: {e}")
                    break

def extraer_hashtags(texto):
    """
    Extrae todos los hashtags de un texto.
    Retorna una lista de hashtags sin el símbolo #
    """
    if not texto:
        return []

    hashtags = re.findall(r'#\w+', texto)
    return [tag.lstrip('#') for tag in hashtags]

def iniciar_fb(key, lista_perfiles):
    print(f"\n--- INICIANDO MÓDULO FACEBOOK (SINGLE KEY) ---")
    analizar_facebook_optimizado(key, lista_perfiles)