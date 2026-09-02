import json
import os
import re
from datetime import datetime
from urllib.parse import urlparse

import requests
from django_backend.models import DimUsuario, PostComment, ScrapeResult
from .script_fb import (
    _obtener_comentarios_post,
    analizar_sentimiento,
    extraer_hashtags,
    guardar_en_db,
)

HOST = "facebook-scraper3.p.rapidapi.com"
JSON_DIR = "json_logs"


def _guardar_json_local(categoria, identificador, data):
    hoy = datetime.now().strftime("%Y_%m_%d")
    folder_path = os.path.join(JSON_DIR, hoy, categoria)
    os.makedirs(folder_path, exist_ok=True)
    filename = f"{identificador}_{datetime.now().strftime('%H%M%S')}.json"
    with open(os.path.join(folder_path, filename), "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


def _extraer_username(author, post_url):
    author_url = author.get("url", "") if isinstance(author, dict) else ""
    candidate_url = author_url or post_url
    path_parts = [part for part in urlparse(candidate_url).path.split("/") if part]
    if not path_parts:
        return ""

    username = path_parts[0]
    if username in {"photo", "posts", "permalink.php", "watch"} and len(path_parts) > 1:
        username = path_parts[1]
    return re.sub(r"[^A-Za-z0-9._-]", "", username).strip()


def _obtener_info_usuario(username, api_key, author_url=None):
    response = requests.get(
        f"https://{HOST}/page/details",
        headers={
            "x-rapidapi-key": api_key,
            "x-rapidapi-host": HOST,
            "Content-Type": "application/json",
        },
        params={"url": author_url or f"https://www.facebook.com/{username}"},
        timeout=15,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Error HTTP {response.status_code} obteniendo el usuario @{username}"
        )

    data = response.json()
    _guardar_json_local("usuarios_facebook_individuales", username, data)
    profile_data = data.get("results", data)
    return profile_data.get("followers", 0) or 0


def procesar_post_individual_facebook(post_url, api_key):
    """Obtiene, analiza y guarda un único post de Facebook por URL."""
    response = requests.get(
        f"https://{HOST}/post",
        headers={
            "x-rapidapi-key": api_key,
            "x-rapidapi-host": HOST,
            "Content-Type": "application/json",
        },
        params={"post_url": post_url},
        timeout=20,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Error HTTP {response.status_code} obteniendo el post")

    response_data = response.json()
    if response_data.get("error"):
        raise RuntimeError(str(response_data["error"]))

    post_data = response_data.get("results", response_data)
    if not isinstance(post_data, dict):
        raise ValueError("La respuesta de Facebook no contiene results")
    if not post_data.get("post_id"):
        raise ValueError("La respuesta de Facebook no contiene post_id")

    post_id = str(post_data["post_id"])
    _guardar_json_local("posts_facebook_individuales", post_id, response_data)

    author = post_data.get("author") or {}
    username = _extraer_username(author, post_data.get("url") or post_url)
    if not username:
        raise ValueError("No se pudo obtener el username del autor de Facebook")

    comments_headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": HOST,
        "Content-Type": "application/json",
    }
    comments, _ = _obtener_comentarios_post(post_id, comments_headers)

    existing_post = ScrapeResult.objects.filter(
        platform="fb",
        username=username,
        raw_data__contains=post_id,
    ).first()
    if existing_post:
        if comments:
            existing_post.comentarios_guardados.all().delete()
            for comment_text in comments:
                PostComment.objects.create(
                    post=existing_post,
                    texto=comment_text,
                    platform="fb",
                )
            existing_post.comments = post_data.get("comments_count", len(comments)) or len(comments)
            existing_post.save(update_fields=["comments"])
        return existing_post, len(comments), username, True

    dim_usuario = DimUsuario.objects.filter(
        platform="fb",
        username=username,
        followers__gt=0,
    ).first()
    if dim_usuario:
        seguidores = dim_usuario.followers
    else:
        seguidores = _obtener_info_usuario(username, api_key, author.get("url"))

    timestamp = post_data.get("timestamp")
    fecha_str = "N/A"
    if timestamp:
        fecha_str = datetime.fromtimestamp(int(timestamp)).strftime("%d/%m/%Y %H:%M:%S")

    description = (
        post_data.get("message")
        or post_data.get("description")
        or ""
    ).replace("\n", " ")
    comments_count = post_data.get("comments_count", 0) or 0
    likes = post_data.get("reactions_count", 0) or 0
    views = post_data.get("reshare_count", 0) or 0

    text_for_sentiment = comments if comments else ([description] if description else [])
    pesos, sentimiento_global = analizar_sentimiento(text_for_sentiment)

    scrape_result = guardar_en_db(
        username,
        int(seguidores or 0),
        fecha_str,
        likes,
        comments_count,
        views,
        description,
        hashtags=extraer_hashtags(description),
        pesos=pesos,
        sentimiento_global=sentimiento_global,
    )
    if not scrape_result:
        raise RuntimeError("No se pudo guardar el post en la base de datos")

    scrape_result.raw_data = json.dumps(response_data, ensure_ascii=False)
    scrape_result.save(update_fields=["raw_data"])

    for comment_text in comments:
        PostComment.objects.create(
            post=scrape_result,
            texto=comment_text,
            platform="fb",
        )

    return scrape_result, len(comments), username, False
