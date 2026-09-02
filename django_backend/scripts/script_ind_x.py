import json
import os
from datetime import datetime

import requests

from django_backend.models import DimUsuario, ScrapeResult
from .script_x import analizar_sentimiento, extraer_hashtags, formatear_fecha_x, guardar_en_db

HOST = "twitter-api45.p.rapidapi.com"
JSON_DIR = "json_logs"


def _guardar_json_local(categoria, identificador, data):
    hoy = datetime.now().strftime("%Y_%m_%d")
    folder_path = os.path.join(JSON_DIR, hoy, categoria)
    os.makedirs(folder_path, exist_ok=True)
    filename = f"{identificador}_{datetime.now().strftime('%H%M%S')}.json"
    with open(os.path.join(folder_path, filename), "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


def _numero(valor):
    try:
        return int(valor or 0)
    except (TypeError, ValueError):
        return 0


def procesar_post_individual_x(tweet_id, api_key):
    """Obtiene, analiza y guarda un único tweet por su id."""
    tweet_id = str(tweet_id).strip()
    if not tweet_id:
        raise ValueError("El parámetro tweet_id es requerido")

    response = requests.get(
        f"https://{HOST}/tweet.php",
        headers={
            "x-rapidapi-key": api_key,
            "x-rapidapi-host": HOST,
            "Content-Type": "application/json",
        },
        params={"id": tweet_id},
        timeout=20,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Error HTTP {response.status_code} obteniendo el tweet")

    post_data = response.json()
    if post_data.get("error"):
        raise RuntimeError(str(post_data["error"]))
    if post_data.get("status") not in (None, "active"):
        raise RuntimeError(f"El tweet no está activo: {post_data.get('status')}")

    resolved_tweet_id = str(post_data.get("id") or tweet_id)
    _guardar_json_local("posts_x_individuales", resolved_tweet_id, post_data)

    author = post_data.get("author") or {}
    username = (author.get("screen_name") or "").strip()
    if not username:
        raise ValueError("La respuesta de X no contiene author.screen_name")

    existing_post = ScrapeResult.objects.filter(
        platform="x",
        username=username,
        raw_data__contains=resolved_tweet_id,
    ).first()
    if existing_post:
        return existing_post, 0, username, True

    seguidores = _numero(author.get("sub_count"))
    dim_usuario = DimUsuario.objects.filter(platform="x", username=username).first()
    if dim_usuario and seguidores <= 0:
        seguidores = dim_usuario.followers

    text = (post_data.get("display_text") or post_data.get("text") or "").replace("\n", " ")
    fecha_obj = formatear_fecha_x(post_data.get("created_at"))
    likes = _numero(post_data.get("likes"))
    replies = _numero(post_data.get("replies"))
    retweets = _numero(post_data.get("retweets"))
    views = _numero(post_data.get("views"))
    hashtags = extraer_hashtags(text)

    pesos, sentimiento_global = analizar_sentimiento([text] if text else [])
    scrape_result = guardar_en_db(
        username,
        seguidores,
        fecha_obj,
        likes,
        replies,
        retweets,
        views,
        text,
        hashtags=hashtags,
        pesos=pesos,
        sentimiento_global=sentimiento_global,
    )
    if not scrape_result:
        raise RuntimeError("No se pudo guardar el tweet en la base de datos")

    scrape_result.raw_data = json.dumps(post_data, ensure_ascii=False)
    scrape_result.save(update_fields=["raw_data"])
    return scrape_result, 0, username, False
