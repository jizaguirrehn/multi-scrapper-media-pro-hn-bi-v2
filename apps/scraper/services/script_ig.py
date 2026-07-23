import csv
import json
import logging
import os
from datetime import datetime

import pandas as pd
import requests
from django.utils.timezone import make_aware

from apps.scraper.models import ScrapeResult
from apps.scraper.services.sentiments.analizador import get_data

# Instancia del logger vinculada al contexto/módulo de Django
logger = logging.getLogger(__name__)

HOY = datetime.now().strftime("%Y_%m_%d")
HOST = "instagram-scraper-stable-api.p.rapidapi.com"
JSON_DIR = "json_logs"


def _guardar_json_local(categoria: str, identificador: str, data: dict) -> None:
    """Guarda las respuestas JSON de las peticiones en carpetas clasificadas."""
    try:
        folder_path = os.path.join(JSON_DIR, HOY, categoria)
        os.makedirs(folder_path, exist_ok=True)

        timestamp = datetime.now().strftime("%H%M%S")
        filename = f"{identificador}_{timestamp}.json"
        filepath = os.path.join(folder_path, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logger.debug(
            "[JSON] Guardado exitosamente localmente",
            extra={"categoria": categoria, "identificador": identificador, "filepath": filepath}
        )
    except Exception:
        logger.exception(
            "Error al guardar JSON local",
            extra={"categoria": categoria, "identificador": identificador}
        )


def _obtener_comentarios_post(media_code: str, lista_keys: list, key_index: int) -> tuple[list, int]:
    """Obtiene los comentarios de un post para el análisis de sentimiento."""
    url = f"https://{HOST}/get_post_comments.php"
    idx = key_index
    comentarios = []

    while idx < len(lista_keys):
        headers = {
            "x-rapidapi-key": lista_keys[idx],
            "x-rapidapi-host": HOST,
            "Content-Type": "application/json",
        }
        params = {"media_code": media_code, "sort_order": "popular"}
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=15)

            if response.status_code == 429:
                logger.warning(
                    "Rate limit (429) alcanzado al obtener comentarios. Rotando API key.",
                    extra={"media_code": media_code, "key_index_fallida": idx}
                )
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
                
                logger.info(
                    "Comentarios obtenidos con éxito",
                    extra={"media_code": media_code, "total_comentarios": len(comentarios)}
                )
                return comentarios, idx

            logger.error(
                "Error HTTP al solicitar comentarios",
                extra={"media_code": media_code, "status_code": response.status_code, "response": response.text}
            )
            break

        except Exception:
            logger.exception("Excepción durante la llamada a API de comentarios", extra={"media_code": media_code})
            idx += 1

    return comentarios, idx


def _analizar_sentimiento(textos: list) -> tuple[dict, str]:
    """Analiza una lista de textos (comentarios) con el modelo Databricks."""
    pesos = {
        'alegria': 0.0, 'confianza': 0.0, 'miedo': 0.0, 'sorpresa': 0.0,
        'tristeza': 0.0, 'aversion': 0.0, 'ira': 0.0, 'anticipacion': 0.0
    }
    sentimiento_global = 'N/A'

    if not textos:
        logger.debug("No hay comentarios para analizar sentimiento.")
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
            
            logger.info("Análisis de sentimiento finalizado", extra={"sentimiento_global": sentimiento_global})

    except Exception:
        logger.exception("Error durante el análisis de sentimiento ML/Databricks")

    return pesos, sentimiento_global


def guardar_en_db(target, seguidores, fecha_obj, likes, comms, desc, pesos=None, sentimiento_global='N/A'):
    """Recibe los datos del scrapeo y los almacena directamente en la base de datos Django."""
    try:
        fecha_dt = None
        if fecha_obj:
            fecha_dt = make_aware(fecha_obj) if fecha_obj.tzinfo is None else fecha_obj

        p = pesos or {}
        obj = ScrapeResult.objects.create(
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
        logger.info(
            "Registro insertado en DB exitosamente",
            extra={"target": target, "scrape_result_id": obj.id, "sentimiento": sentimiento_global}
        )
    except Exception:
        logger.exception("Fallo en la persistencia con la Base de Datos Django", extra={"target": target})


def _guardar_posts_en_csv(nombre_archivo, target, seguidores, posts, lista_keys, key_index):
    """Por cada post: obtiene sus comentarios, analiza el sentimiento, y guarda en CSV y DB."""
    current_key_idx = key_index

    with open(nombre_archivo, mode="a", newline="", encoding="utf-8-sig") as file_append:
        writer = csv.writer(file_append)

        for item in posts:
            node = item.get("node", item) if isinstance(item, dict) else {}

            code = node.get("code") or node.get("shortcode")
            if not code:
                logger.warning("Nodo de post omitido por no poseer 'code' o 'shortcode'.", extra={"target": target})
                continue

            caption_data = node.get("caption")
            if isinstance(caption_data, dict):
                caption = caption_data.get("text", "")
            elif isinstance(caption_data, str):
                caption = caption_data
            else:
                caption = ""

            desc = caption.replace("\n", " ")

            timestamp = node.get("taken_at") or node.get("taken_at_timestamp")
            fecha_dt_obj = datetime.fromtimestamp(timestamp) if timestamp else None
            fecha_csv = fecha_dt_obj.strftime("%d/%m/%Y") if fecha_dt_obj else "N/A"

            likes = node.get("like_count", 0)
            comms = node.get("comment_count", 0)

            comentarios, current_key_idx = _obtener_comentarios_post(code, lista_keys, current_key_idx)
            pesos, sentimiento_global = _analizar_sentimiento(comentarios)

            logger.info(
                "Procesando métricas de Post",
                extra={"code": code, "total_comentarios": len(comentarios), "sentimiento": sentimiento_global}
            )

            writer.writerow([target, seguidores, fecha_csv, "Post", likes, comms, desc, sentimiento_global])

            guardar_en_db(
                target, seguidores, fecha_dt_obj, likes, comms, desc,
                pesos=pesos, sentimiento_global=sentimiento_global,
            )

    return current_key_idx


def _obtener_info_usuario(target, lista_keys, key_index):
    """Llama al perfil para guardar log local."""
    url_perfil = f"https://{HOST}/ig_get_fb_profile_hover.php"
    headers = {
        "x-rapidapi-key": lista_keys[key_index],
        "x-rapidapi-host": HOST,
    }

    seguidores = 0

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

            user_data = data.get("user_data", {})
            seguidores = user_data.get("follower_count", 0)
            logger.info("Información de usuario obtenida", extra={"target": target, "seguidores": seguidores})
        else:
            logger.warning(
                "No se pudo obtener información del perfil",
                extra={"target": target, "status_code": response.status_code}
            )

    except Exception:
        logger.exception("Error solicitando información de usuario", extra={"target": target})

    return seguidores


def _procesar_target(target, lista_keys, key_actual_index, nombre_archivo):
    """Realiza la petición de posts del perfil, extrae la lista y delega el guardado/análisis."""
    logger.info("Iniciando procesamiento de perfil", extra={"target": target})
    seguidores_perfil = _obtener_info_usuario(target, lista_keys, key_actual_index)
    
    url_posts = f"https://{HOST}/get_ig_user_posts.php"
    target_url = target if target.startswith("http") else f"https://www.instagram.com/{target}/"

    headers = {
        "x-rapidapi-key": lista_keys[key_actual_index],
        "x-rapidapi-host": HOST,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    payload = {
        "username_or_url": target_url,
        "pagination_token": "",
        "amount": "12",
    }

    try:
        response = requests.post(url_posts, headers=headers, data=payload, timeout=15)

        if response.status_code == 429:
            logger.warning(
                "Rate limit (429) al obtener posts del target. Rotando key...",
                extra={"target": target, "key_index": key_actual_index}
            )
            return False, key_actual_index + 1

        if response.status_code == 200:
            res_data = response.json()

            if "error" in res_data:
                logger.error(
                    "Error retornado directamente en payload JSON de la API",
                    extra={"target": target, "api_error": res_data['error']}
                )
                return False, key_actual_index

            _guardar_json_local("posts", target, res_data)

            posts = (
                res_data.get("posts", [])
                or res_data.get("items", [])
                or res_data.get("data", {}).get("items", [])
            )
            user_info = res_data.get("user", {}) or {}
            seguidores = user_info.get("follower_count") or seguidores_perfil

            nuevo_idx = _guardar_posts_en_csv(
                nombre_archivo, target, seguidores, posts, lista_keys, key_actual_index,
            )
            logger.info("Perfil procesado exitosamente", extra={"target": target})
            return True, nuevo_idx

        logger.error(
            "Error HTTP en petición de posts del target",
            extra={"target": target, "status_code": response.status_code}
        )
        return False, key_actual_index + 1

    except Exception:
        logger.exception("Excepción crítica al procesar target", extra={"target": target})
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
                ["USUARIO", "SEGUIDORES", "FECHA", "TIPO", "LIKES", "COMMS", "DESCRIPCION", "SENTIMIENTO"]
            )

    for target in lista_targets:
        exito = False
        while not exito and key_actual_index < len(lista_keys):
            exito, key_actual_index = _procesar_target(
                target, lista_keys, key_actual_index, nombre_archivo
            )
            
        if key_actual_index >= len(lista_keys):
            logger.critical("Se agotaron todas las API Keys disponibles. Deteniendo proceso.")
            break


def iniciar(mis_apis_keys, lista_perfiles):
    logger.info(
        "Iniciando flujo de recolección y análisis",
        extra={"total_keys": len(mis_apis_keys), "total_perfiles": len(lista_perfiles)}
    )
    analizar_con_rotacion(mis_apis_keys, lista_perfiles)
    logger.info("Flujo de recolección y análisis finalizado exitosamente.")