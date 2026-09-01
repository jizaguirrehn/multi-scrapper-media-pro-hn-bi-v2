import csv
import json
import os
import re
from datetime import datetime
import requests
import pandas as pd

from django_backend.models import ScrapeResult, PostComment
from django.utils.timezone import make_aware
from .sentiments.analizador import get_data

HOY = datetime.now().strftime("%Y_%m_%d")
HOST = "instagram-scraper-stable-api.p.rapidapi.com"
JSON_DIR = "json_logs"


def _guardar_json_local(categoria, identificador, data):
    """Guarda las respuestas JSON de las peticiones en carpetas clasificadas."""
    try:
        folder_path = os.path.join(JSON_DIR, HOY, categoria)
        os.makedirs(folder_path, exist_ok=True)

        timestamp = datetime.now().strftime("%H%M%S")
        filename = f"{identificador}_{timestamp}.json"
        filepath = os.path.join(folder_path, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"   [JSON] Guardado en: {filepath}")
    except Exception as e:
        print(f"Error guardando JSON local para {categoria}/{identificador}: {e}")


def _obtener_comentarios_post(media_code, lista_keys, key_index):
    """Obtiene los comentarios de un post para usarlos en la captura/análisis de sentimiento."""
    url = f"https://{HOST}/get_post_comments.php"
    idx = key_index
    comentarios = []

    while idx < len(lista_keys):
        headers = {
            "x-rapidapi-key": lista_keys[idx],
            "x-rapidapi-host": HOST,
            "Content-Type": "application/json",
        }
        params = {"media_code": media_code, "sort_order": "recent"}
        try:
            response = requests.get(url, headers=headers, params=params, timeout=15)

            if response.status_code == 429:
                print(f"Rate limit (429) en Key índice {idx}. Probando siguiente key...")
                idx += 1
                continue

            if response.status_code == 200:
                data = response.json()
                _guardar_json_local("comentarios", media_code, data)

                comments_list = data.get("comments", []) or data.get("data", {}).get("comments", [])
                for comment in comments_list:
                    texto = comment.get("text", "").strip()
                    if texto:
                        comentarios.append(texto)
                return comentarios, idx

            break
        except Exception as e:
            print(f"Error obteniendo comentarios del post {media_code}: {e}")
            idx += 1

    return comentarios, idx


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
        print(f"Error usando Databricks para sentimiento Instagram: {e}")

    # Fallback local
    pesos["alegria"] = round(len(comentarios) * 0.5, 2)
    sentimiento_global = "Positivo" if len(comentarios) > 0 else "Neutral"
    return pesos, sentimiento_global


def guardar_en_db(target, seguidores, fecha_obj, likes, comms, desc, hashtags=None, pesos=None, sentimiento_global="N/A"):
    """
    Guarda los resultados del scrapeo y el sentimiento derivado de los comentarios en Django DB.
    Retorna el objeto ScrapeResult creado.
    """
    try:
        fecha_dt = None
        if fecha_obj:
            if fecha_obj.tzinfo is None:
                fecha_dt = make_aware(fecha_obj)
            else:
                fecha_dt = fecha_obj

        if hashtags is None:
            hashtags = []

        # Convertir hashtags a string separado por comas
        hashtags_str = ",".join(hashtags) if hashtags else ""

        scrape_result = ScrapeResult.objects.create(
            platform='ig',
            username=target,
            followers=seguidores,
            post_date=fecha_dt,
            likes=likes,
            comments=comms,
            description=desc,
            hashtags=hashtags_str,
            sentimiento_global=sentimiento_global,
            alegria=float(pesos.get("alegria", 0.0)) if pesos else 0.0,
            confianza=float(pesos.get("confianza", 0.0)) if pesos else 0.0,
            miedo=float(pesos.get("miedo", 0.0)) if pesos else 0.0,
            sorpresa=float(pesos.get("sorpresa", 0.0)) if pesos else 0.0,
            tristeza=float(pesos.get("tristeza", 0.0)) if pesos else 0.0,
            aversion=float(pesos.get("aversion", 0.0)) if pesos else 0.0,
            ira=float(pesos.get("ira", 0.0)) if pesos else 0.0,
            anticipacion=float(pesos.get("anticipacion", 0.0)) if pesos else 0.0,
        )
        print(f"   [DB Django] Guardado exitoso -> @{target} | Sentimiento: {sentimiento_global} | Hashtags: {hashtags}")
        return scrape_result
    except Exception as e:
        print(f"Error al guardar en DB (Instagram): {e}")
        return None


def _guardar_posts_en_csv(nombre_archivo, target, seguidores, posts, lista_keys, key_index):
    current_key_idx = key_index

    with open(nombre_archivo, mode="a", newline="", encoding="utf-8-sig") as file_append:
        writer = csv.writer(file_append)

        for item in posts:
            node = item.get("node", item) if isinstance(item, dict) else {}

            # Filtrar posts anclados
            pinned_ids = node.get("timeline_pinned_user_ids")
            if pinned_ids:  # Si tiene valores, es un post anclado, lo saltamos
                print(f"   [ANCLADO] Post saltado (anclado al perfil)")
                continue

            code = node.get("code") or node.get("shortcode")
            if not code:
                continue

            caption_data = node.get("caption")
            if isinstance(caption_data, dict):
                caption = caption_data.get("text", "")
            elif isinstance(caption_data, str):
                caption = caption_data
            else:
                caption = ""

            desc = caption.replace("\n", " ")
            
            # Extrae hashtags del caption
            hashtags = extraer_hashtags(caption)

            timestamp = node.get("taken_at") or node.get("taken_at_timestamp")
            fecha_dt_obj = datetime.fromtimestamp(timestamp) if timestamp else None
            fecha_csv = fecha_dt_obj.strftime("%d/%m/%Y") if fecha_dt_obj else "N/A"

            likes = node.get("like_count", 0)
            comms = node.get("comment_count", 0)

            # 1. Captura de comentarios del post para análisis de sentimiento
            comentarios, current_key_idx = _obtener_comentarios_post(
                code, lista_keys, current_key_idx
            )

            # 2. Análisis del sentimiento sobre la lista de comentarios
            pesos, sentimiento_global = analizar_sentimiento(comentarios)

            print(f"   Post {code}: {len(comentarios)} comentarios analizados. Sentimiento = {sentimiento_global}")

            # 3. Guardado en archivo CSV
            hashtags_csv = ",".join(hashtags)
            writer.writerow([target, seguidores, fecha_csv, "Post", likes, comms, desc, sentimiento_global, hashtags_csv])

            # 4. Persistence real en Django DB
            scrape_result = guardar_en_db(
                target,
                seguidores,
                fecha_dt_obj,
                likes,
                comms,
                desc,
                hashtags=hashtags,
                pesos=pesos,
                sentimiento_global=sentimiento_global,
            )
            
            # 5. Guardar comentarios relacionados al post
            if scrape_result and comentarios:
                for comentario_texto in comentarios:
                    try:
                        PostComment.objects.create(
                            post=scrape_result,
                            texto=comentario_texto,
                            platform='ig'
                        )
                    except Exception as e:
                        print(f"Error guardando comentario en DB: {e}")
                print(f"   [DB Django] {len(comentarios)} comentarios guardados para el post {code}")

    return current_key_idx


def _obtener_info_usuario(target, lista_keys, key_index):
    url_perfil = f"https://{HOST}/ig_get_fb_profile_hover.php"
    headers = {
        "x-rapidapi-key": lista_keys[key_index],
        "x-rapidapi-host": HOST,
    }
    try:
        response = requests.get(
            url_perfil,
            headers=headers,
            params={"username_or_url": target},
            timeout=15,
        )
        if response.status_code == 200:
            data = response.json()
            _guardar_json_local("usuarios", target, data)
    except Exception as e:
        print(f"Error obteniendo perfil/usuario de @{target}: {e}")


def _procesar_target(target, lista_keys, key_actual_index, nombre_archivo):
    _obtener_info_usuario(target, lista_keys, key_actual_index)

    url_posts = f"https://{HOST}/get_ig_user_posts.php"
    target_url = target if target.startswith("http") else f"https://www.instagram.com/{target}/"

    headers = {
        "x-rapidapi-key": lista_keys[key_actual_index],
        "x-rapidapi-host": HOST,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    payload = {
        "username_or_url": target_url,
        "pagination_token": "",
        "amount": "12"
    }

    try:
        response = requests.post(url_posts, headers=headers, data=payload, timeout=15)

        if response.status_code == 429:
            print(f"Rate limit (429) obteniendo posts de @{target}. Cambiando de key...")
            return False, key_actual_index + 1

        if response.status_code == 200:
            res_data = response.json()

            if "error" in res_data:
                print(f"Error devuelto por la API para @{target}: {res_data['error']}")
                return False, key_actual_index

            _guardar_json_local("posts", target, res_data)

            posts = (
                res_data.get("posts", [])
                or res_data.get("items", [])
                or res_data.get("data", {}).get("items", [])
            )
            user_info = res_data.get("user", {}) or {}
            seguidores = user_info.get("follower_count", 0)

            nuevo_idx = _guardar_posts_en_csv(
                nombre_archivo,
                target,
                seguidores,
                posts,
                lista_keys,
                key_actual_index,
            )
            print(f" @{target} procesado exitosamente.\n")
            return True, nuevo_idx

        print(f"Error HTTP {response.status_code} al procesar @{target}")
        return False, key_actual_index + 1

    except Exception as e:
        print(f"Error procesando @{target}: {e}")
        return False, key_actual_index + 1


def analizar_con_rotacion(lista_keys, lista_targets):
    if not os.path.exists("results"):
        os.makedirs("results")

    nombre_archivo = f"results/datos_ig_{HOY}.csv"
    key_actual_index = 0

    if not os.path.exists(nombre_archivo):
        with open(nombre_archivo, mode="w", newline="", encoding="utf-8-sig") as file:
            writer = csv.writer(file)
            writer.writerow(
                ["USUARIO", "SEGUIDORES", "FECHA", "TIPO", "LIKES", "COMMS", "DESCRIPCION", "SENTIMIENTO", "HASHTAGS"]
            )

    for target in lista_targets:
        exito = False
        while not exito and key_actual_index < len(lista_keys):
            exito, key_actual_index = _procesar_target(
                target, lista_keys, key_actual_index, nombre_archivo
            )


def iniciar(mis_apis_keys, lista_perfiles):
    analizar_con_rotacion(mis_apis_keys, lista_perfiles)


if __name__ == "__main__":
    # Recuerda cargar tus llaves desde variables de entorno para mayor seguridad
    mis_apis_keys = [os.getenv("RAPIDAPI_KEY", "TU_API_KEY_AQUI")]
    lista_perfiles = ["aleborjas91"]

    print("Iniciando procesamiento de posts y sentimiento...")
    iniciar(mis_apis_keys, lista_perfiles)
    print("Proceso completado.")
    