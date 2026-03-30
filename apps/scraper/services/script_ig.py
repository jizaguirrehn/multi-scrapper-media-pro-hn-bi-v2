import requests
import csv
import os
import pandas as pd
from datetime import datetime

from apps.scraper.models import ScrapeResult
from django.utils.timezone import make_aware
from apps.scraper.services.sentiments.analizador import get_data

HOY = datetime.now().strftime("%Y_%m_%d")


def _obtener_texto_post(post_id, lista_keys, key_index):
    """Llama a /post?id=<post_id> y retorna el texto de caption del post, o cadena vacía."""
    url = "https://instagram-looter2.p.rapidapi.com/post"
    host = "instagram-looter2.p.rapidapi.com"
    idx = key_index

    while idx < len(lista_keys):
        headers = {
            "x-rapidapi-key": lista_keys[idx],
            "x-rapidapi-host": host,
            "Content-Type": "application/json"
        }
        try:
            response = requests.get(url, headers=headers, params={"id": post_id}, timeout=15)
            if response.status_code == 429:
                idx += 1
                continue
            if response.status_code == 200:
                data = response.json()
                edges = data.get('edge_media_to_caption', {}).get('edges', [])
                texto = edges[0].get('node', {}).get('text', '').strip() if edges else ''
                return texto
            break
        except Exception as e:
            print(f"Error obteniendo post IG (id={post_id}): {e}")
            idx += 1

    return ''


def _analizar_sentimiento(textos):
    """Analiza una lista de textos con el modelo Databricks y retorna pesos + sentimiento global."""
    pesos = {
        'alegria': 0.0, 'confianza': 0.0, 'miedo': 0.0, 'sorpresa': 0.0,
        'tristeza': 0.0, 'aversion': 0.0, 'ira': 0.0, 'anticipacion': 0.0
    }
    sentimiento_global = 'N/A'

    if not textos:
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

    except Exception as e:
        print(f"Error en análisis de sentimiento (Instagram): {e}")

    return pesos, sentimiento_global


def guardar_en_db(target, seguidores, fecha_obj, likes, comms, desc, pesos=None, sentimiento_global='N/A'):
    """
    Recibe fecha_obj directamente como un objeto datetime
    """
    try:
        fecha_dt = None
        if fecha_obj:
            # Si el objeto no tiene zona horaria, se la añadimos para Django
            if fecha_obj.tzinfo is None:
                fecha_dt = make_aware(fecha_obj)
            else:
                fecha_dt = fecha_obj

        p = pesos or {}
        ScrapeResult.objects.create(
            platform='ig',
            username=target,
            followers=seguidores,
            post_date=fecha_dt,
            likes=likes,
            comments=comms,
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
    except Exception as e:
        print(f"Error al guardar en DB (Instagram): {e}")

def _guardar_posts_en_csv(nombre_archivo, target, seguidores, edges, lista_keys, key_index):
    """Por cada post: obtiene texto via /post?id=, analiza sentimiento y guarda en CSV y DB."""
    with open(nombre_archivo, mode='a', newline='', encoding='utf-8-sig') as file_append:
        writer = csv.writer(file_append)
        for edge in edges:
            node = edge.get('node', {})
            post_id = node.get('id')
            timestamp = node.get('taken_at_timestamp')
            fecha_dt_obj = datetime.fromtimestamp(timestamp) if timestamp else None
            fecha_csv = fecha_dt_obj.strftime('%d/%m/%Y') if fecha_dt_obj else "N/A"

            likes = node.get('edge_liked_by', {}).get('count', 0)
            comms = node.get('edge_media_to_comment', {}).get('count', 0)

            # 1. Obtener texto del post via /post?id=
            texto = _obtener_texto_post(post_id, lista_keys, key_index) if post_id else ''
            desc = texto.replace('\n', ' ')

            # 2. Analizar sentimiento solo de este post
            pesos, sentimiento_global = _analizar_sentimiento([texto] if texto else [])

            print(f"  Post {post_id}: sentimiento={sentimiento_global}")

            # 3. Guardar
            writer.writerow([target, seguidores, fecha_csv, "Post", likes, comms, desc])
            guardar_en_db(
                target, seguidores, fecha_dt_obj, likes, comms, desc,
                pesos=pesos, sentimiento_global=sentimiento_global
            )


def _procesar_target(target, lista_keys, key_actual_index, url, host, nombre_archivo):
    """Llama al API de web-profile y por cada post obtiene texto, analiza sentimiento y guarda.
    Retorna (exito, nuevo_key_index)."""
    headers = {
        "x-rapidapi-key": lista_keys[key_actual_index],
        "x-rapidapi-host": host
    }
    try:
        response = requests.get(url, headers=headers, params={"username": target}, timeout=15)
        if response.status_code == 429:
            return False, key_actual_index + 1

        if response.status_code == 200:
            res_data = response.json()
            user = res_data.get('data', {}).get('user', {}) if 'data' in res_data else res_data.get('user', {})
            if user:
                seguidores = user.get('edge_followed_by', {}).get('count', 0)
                edges = user.get('edge_owner_to_timeline_media', {}).get('edges', [])
                _guardar_posts_en_csv(nombre_archivo, target, seguidores, edges, lista_keys, key_actual_index)
                print(f" @{target} procesado y guardado en DB.")
            return True, key_actual_index

        return False, key_actual_index + 1

    except Exception as e:
        print(f"Error procesando @{target}: {e}")
        return False, key_actual_index + 1


def analizar_con_rotacion(lista_keys, lista_targets):
    host = "instagram-looter2.p.rapidapi.com"
    url = "https://instagram-looter2.p.rapidapi.com/web-profile"

    if not os.path.exists('results'):
        os.makedirs('results')

    nombre_archivo = f"results/datos_ig_{HOY}.csv"
    key_actual_index = 0

    if not os.path.exists(nombre_archivo):
        with open(nombre_archivo, mode='w', newline='', encoding='utf-8-sig') as file:
            writer = csv.writer(file)
            writer.writerow(['USUARIO', 'SEGUIDORES', 'FECHA', 'TIPO', 'LIKES', 'COMMS', 'DESCRIPCION'])

    for target in lista_targets:
        exito = False
        while not exito and key_actual_index < len(lista_keys):
            exito, key_actual_index = _procesar_target(
                target, lista_keys, key_actual_index, url, host, nombre_archivo
            )


def iniciar(mis_apis_keys, lista_perfiles):
    analizar_con_rotacion(mis_apis_keys, lista_perfiles)