import json
import os
from datetime import datetime

import requests
from django.utils.timezone import make_aware

from django_backend.models import DimUsuario, PostComment, ScrapeResult
from .script_tk import (
    _obtener_comentarios_video,
    analizar_sentimiento,
    extraer_hashtags,
    guardar_en_db,
)

HOST = "tiktok-api23.p.rapidapi.com"
JSON_DIR = "json_logs"


def _guardar_json_local(categoria, identificador, data):
    hoy = datetime.now().strftime("%Y_%m_%d")
    folder_path = os.path.join(JSON_DIR, hoy, categoria)
    os.makedirs(folder_path, exist_ok=True)
    filename = f"{identificador}_{datetime.now().strftime('%H%M%S')}.json"
    with open(os.path.join(folder_path, filename), "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


def _obtener_item_post(data):
    """Normaliza las respuestas conocidas de TikTok post/detail."""
    candidates = [
        data.get("itemInfo", {}).get("itemStruct"),
        data.get("data", {}).get("itemInfo", {}).get("itemStruct"),
        data.get("data", {}).get("itemStruct"),
        data.get("itemStruct"),
        data.get("data"),
    ]
    for candidate in candidates:
        if isinstance(candidate, dict) and (
            candidate.get("id")
            or candidate.get("videoId")
            or candidate.get("author")
            or candidate.get("desc")
        ):
            return candidate
    return data


def _obtener_info_usuario(username, api_key):
    response = requests.get(
        f"https://{HOST}/api/user/info",
        headers={
            "x-rapidapi-key": api_key,
            "x-rapidapi-host": HOST,
            "Content-Type": "application/json",
        },
        params={"uniqueId": username},
        timeout=15,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Error HTTP {response.status_code} obteniendo el usuario @{username}"
        )

    data = response.json()
    _guardar_json_local("usuarios_tiktok_individuales", username, data)
    return data


def _extraer_autor(item):
    author = item.get("author") or item.get("authorInfo") or {}
    if isinstance(author, str):
        return author, {}
    username = (
        author.get("uniqueId")
        or author.get("unique_id")
        or author.get("username")
        or item.get("uniqueId")
        or ""
    )
    return username.strip(), author


def _extraer_estadisticas(item):
    stats = item.get("stats") or item.get("statsV2") or item.get("statistics") or {}
    return {
        "likes": stats.get("diggCount", stats.get("digg_count", item.get("diggCount", 0))) or 0,
        "comments": stats.get("commentCount", stats.get("comment_count", item.get("commentCount", 0))) or 0,
        "views": stats.get("playCount", stats.get("play_count", item.get("playCount", 0))) or 0,
    }


def procesar_post_individual_tiktok(video_id, api_key):
    """Obtiene, analiza y guarda un único video de TikTok por videoId."""
    if not video_id:
        raise ValueError("El parámetro videoId es requerido")

    response = requests.get(
        f"https://{HOST}/api/post/detail",
        headers={
            "x-rapidapi-key": api_key,
            "x-rapidapi-host": HOST,
            "Content-Type": "application/json",
        },
        params={"videoId": str(video_id)},
        timeout=20,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Error HTTP {response.status_code} obteniendo el video")

    post_data = response.json()
    if post_data.get("error"):
        raise RuntimeError(str(post_data["error"]))
    if post_data.get("statusCode") not in (None, 0, "0"):
        raise RuntimeError(post_data.get("statusMsg") or "TikTok no pudo obtener el video")

    item = _obtener_item_post(post_data)
    resolved_video_id = str(item.get("id") or item.get("videoId") or video_id)
    _guardar_json_local("posts_tiktok_individuales", resolved_video_id, post_data)

    username, author = _extraer_autor(item)
    if not username:
        raise ValueError("La respuesta del post no contiene author.uniqueId")

    existing_post = ScrapeResult.objects.filter(
        platform="tk",
        username=username,
        raw_data__contains=resolved_video_id,
    ).first()
    if existing_post:
        return existing_post, 0, username, True

    dim_usuario = DimUsuario.objects.filter(
        platform="tk",
        username=username,
        followers__gt=0,
    ).first()
    if dim_usuario:
        seguidores = dim_usuario.followers
    else:
        author_stats = item.get("authorStats") or item.get("author_stats") or {}
        seguidores = (
            author_stats.get("followerCount")
            or author_stats.get("follower_count")
            or author.get("followerCount", 0)
            or 0
        )
        if not seguidores:
            profile_data = _obtener_info_usuario(username, api_key)
            user_info = profile_data.get("userInfo", {})
            profile_stats = user_info.get("stats", {})
            seguidores = profile_stats.get("followerCount", 0) or 0

    stats = _extraer_estadisticas(item)
    description = (
        item.get("desc")
        or item.get("title")
        or item.get("description")
        or ""
    ).replace("\n", " ")
    timestamp = item.get("createTime") or item.get("create_time")
    fecha_str = "N/A"
    if timestamp:
        fecha_str = datetime.fromtimestamp(int(timestamp)).strftime("%d/%m/%Y %H:%M:%S")

    comments_headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "tiktok-scraper7.p.rapidapi.com",
        "Content-Type": "application/json",
    }
    comments = _obtener_comentarios_video(username, resolved_video_id, comments_headers)
    text_for_sentiment = comments if comments else ([description] if description else [])
    pesos, sentimiento_global = analizar_sentimiento(text_for_sentiment)

    scrape_result = guardar_en_db(
        username,
        int(seguidores or 0),
        fecha_str,
        stats["likes"],
        stats["comments"],
        stats["views"],
        description,
        hashtags=extraer_hashtags(description),
        pesos=pesos,
        sentimiento_global=sentimiento_global,
    )
    if not scrape_result:
        raise RuntimeError("No se pudo guardar el video en la base de datos")

    scrape_result.raw_data = json.dumps(post_data, ensure_ascii=False)
    scrape_result.save(update_fields=["raw_data"])

    for comment_text in comments:
        PostComment.objects.create(
            post=scrape_result,
            texto=comment_text,
            platform="tk",
        )

    return scrape_result, len(comments), username, False
