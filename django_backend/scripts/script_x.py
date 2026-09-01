import requests
import csv
import json
import os
import re
import pandas as pd

from datetime import datetime
from django_backend.models import ScrapeResult, PostComment
from django.utils.timezone import make_aware
from .sentiments.analizador import get_data

def extraer_hashtags(texto):
    """
    Extrae todos los hashtags de un texto.
    Retorna una lista de hashtags sin el símbolo #
    """
    if not texto:
        return []

    hashtags = re.findall(r'#\w+', texto)
    print(f"Hashtags encontrados: {hashtags}")

    return [tag.lstrip('#') for tag in hashtags]

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
        print(f"Error usando Databricks para sentimiento X/Twitter: {e}")

    pesos["alegria"] = round(len(comentarios) * 0.5, 2)
    sentimiento_global = "Positivo" if len(comentarios) > 0 else "Neutral"
    return pesos, sentimiento_global


def guardar_en_db(
    target,
    seguidores,
    fecha_obj,
    likes,
    replies,
    retweets,
    vistas,
    desc,
    hashtags=None,
    pesos=None,
    sentimiento_global='N/A'
):
    """
    Inserta los resultados en la base de datos de Django.
    Recibe fecha_obj ya como un objeto datetime.
    """
    try:
        if hashtags is None:
            hashtags = []

        hashtags_str = ",".join(hashtags) if hashtags else ""

        fecha_dt = None
        if fecha_obj:
            if fecha_obj.tzinfo is None:
                fecha_dt = make_aware(fecha_obj)
            else:
                fecha_dt = fecha_obj

        scrape_result = ScrapeResult.objects.create(
            platform='x',
            username=target,
            followers=seguidores if isinstance(seguidores, int) else 0,
            post_date=fecha_dt,
            likes=likes,
            comments=replies,
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
        print(f"Error al guardar en DB (X/Twitter): {e}")
        return None

def formatear_fecha_x(fecha_str):
    """Convierte 'Tue Feb 17 01:01:13 +0000 2026' a un objeto datetime."""
    try:
        if not fecha_str:
            return None
        formato_entrada = "%a %b %d %H:%M:%S %z %Y"
        return datetime.strptime(fecha_str, formato_entrada)
    except Exception as e:
        print(f"Error procesando fecha de X: {e}")
        return None
    
ARCHIVO_IDS = "usuarios_X_registrados.json"
HOY = datetime.now().strftime("%Y_%m_%d")

def cargar_cache_ids():
    if os.path.exists(ARCHIVO_IDS):
        with open(ARCHIVO_IDS, 'r') as f:
            return json.load(f)
    return {}

def guardar_cache_ids(cache):
    with open(ARCHIVO_IDS, 'w') as f:
        json.dump(cache, f, indent=4)

def analizar_X_optimizado(keys_user, keys_timeline, lista_targets):
    cache_ids = cargar_cache_ids()
    nombre_csv = f"results/datos_X_{HOY}.csv"
    idx_u, idx_t = 0, 0

    if not os.path.exists('results'): os.makedirs('results')
    
    if not os.path.exists(nombre_csv):
        with open(nombre_csv, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow([
                'USUARIO',
                'CANTIDAD_SEGUIDORES',
                'FECHA_POST',
                'LIKES',
                'REPLIES',
                'RETWEETS',
                'VISTAS',
                'DESCRIPCION',
                'HASHTAGS'
            ])

    for target in lista_targets:
        user_info = cache_ids.get(target)
        
        # 1. Obtener rest_id si no está en caché
        if not user_info:
            while idx_u < len(keys_user):
                headers_u = {"x-rapidapi-key": keys_user[idx_u], "x-rapidapi-host": "twitter241.p.rapidapi.com"}
                try:
                    res_u = requests.get("https://twitter241.p.rapidapi.com/user", 
                                       headers=headers_u, params={"username": target}, timeout=10)
                    if res_u.status_code == 200:
                        data = res_u.json()
                        info_profunda = data.get('result', {}).get('data', {}).get('user', {}).get('result', {})
                        if info_profunda:
                            legacy = info_profunda.get('legacy', {})
                            user_info = {
                                "rest_id": info_profunda.get('rest_id'),
                                "followers": legacy.get('followers_count', 0)
                            }
                            cache_ids[target] = user_info
                            guardar_cache_ids(cache_ids)
                            break
                    idx_u += 1
                except:
                    idx_u += 1

        # 2. Extraer Timeline
        if user_info:
            exito_timeline = False
            while idx_t < len(keys_timeline) and not exito_timeline:
                headers_t = {"x-rapidapi-key": keys_timeline[idx_t], "x-rapidapi-host": "twitter-api45.p.rapidapi.com"}
                try:
                    res_t = requests.get("https://twitter-api45.p.rapidapi.com/timeline.php", 
                                       headers=headers_t, params={"screenname": target}, timeout=15)
                    
                    if res_t.status_code == 200:
                        tweets = res_t.json().get('timeline', [])
                        with open(nombre_csv, mode='a', newline='', encoding='utf-8-sig') as f:
                            writer = csv.writer(f)
                            for tweet in tweets:
                                fecha_dt = formatear_fecha_x(tweet.get('created_at'))
                                fecha_str = fecha_dt.strftime("%d/%m/%Y %H:%M:%S") if fecha_dt else "N/A"
                                desc = tweet.get('text', '').replace('\n', ' ')

                                # Extraer hashtags
                                hashtags = extraer_hashtags(desc)
                                hashtags_csv = ",".join(hashtags)

                                print(f"Hashtags encontrados: {hashtags}")
                                print(f"Hashtags string: {hashtags_csv}")
                                
                                print(
                                    f"Datos {target}, {user_info['followers']}, "
                                    f"{fecha_str}, {desc} | Hashtags: {hashtags}"
                                )

                                writer.writerow([
                                    target,
                                    user_info['followers'],
                                    fecha_str,
                                    tweet.get('favorites', 0),
                                    tweet.get('replies', 0),
                                    tweet.get('retweets', 0),
                                    tweet.get('views', 0),
                                    desc,
                                    hashtags_csv
                                ])

                                sentiment_text = [desc] if desc else []
                                pesos, sentimiento_global = analizar_sentimiento(sentiment_text)

                                scrape_result = guardar_en_db(
                                    target,
                                    user_info['followers'],
                                    fecha_dt,
                                    tweet.get('favorites', 0),
                                    tweet.get('replies', 0),
                                    tweet.get('retweets', 0),
                                    tweet.get('views', 0),
                                    desc,
                                    hashtags=hashtags,
                                    pesos=pesos,
                                    sentimiento_global=sentimiento_global,
                                )
                                
                                # Nota: En X/Twitter, los replies son tweets separados
                                # por eso se analiza el texto del tweet disponible como contenido del post

                        print(f" ✅ @{target} sincronizado con la base de datos.")
                        exito_timeline = True
                    else:
                        idx_t += 1
                except:
                    idx_t += 1

def iniciar(keys_busqueda, keys_timeline, lista_perfiles):
    print(f"\n--- INICIANDO MÓDULO X (DB CONNECTED) ---")
    if not lista_perfiles: return
    analizar_X_optimizado(keys_busqueda, keys_timeline, lista_perfiles)