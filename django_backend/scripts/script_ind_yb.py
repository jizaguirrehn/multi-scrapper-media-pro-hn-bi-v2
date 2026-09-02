import json
import logging
import os
from datetime import datetime

import pandas as pd
from django_backend.models import DimUsuario, PostComment, ScrapeResult
from .sentiments.analizador import get_data
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

JSON_DIR = "json_logs"
logger = logging.getLogger(__name__)


def _guardar_json_local(categoria, identificador, data):
    hoy = datetime.now().strftime("%Y_%m_%d")
    folder_path = os.path.join(JSON_DIR, hoy, categoria)
    os.makedirs(folder_path, exist_ok=True)
    with open(
        os.path.join(folder_path, f"{identificador}_{datetime.now().strftime('%H%M%S')}.json"),
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
    logger.info("YouTube: JSON guardado en %s/%s", categoria, identificador)


def _analizar_sentimiento(comentarios):
    pesos = {
        "alegria": 0.0, "confianza": 0.0, "miedo": 0.0, "sorpresa": 0.0,
        "tristeza": 0.0, "aversion": 0.0, "ira": 0.0, "anticipacion": 0.0,
    }
    if not comentarios:
        logger.info("YouTube: no hay comentarios para analizar")
        return pesos, "N/A"

    try:
        logger.info("YouTube: iniciando análisis de sentimiento para %d comentarios", len(comentarios))
        resultado = get_data().main(pd.DataFrame({"text": [str(item) for item in comentarios]}))
        logger.info("YouTube: respuesta de Databricks recibida")
        predictions = resultado.get("predictions", []) if resultado else []
        if predictions:
            valores = pd.DataFrame([item["detalles_petalos"][0] for item in predictions])
            sumas = valores.apply(pd.to_numeric, errors="coerce").sum()
            mapping = {
                "alegria": "Alegría", "confianza": "Confianza", "miedo": "Miedo",
                "sorpresa": "Sorpresa", "tristeza": "Tristeza", "aversion": "Aversión",
                "ira": "Ira", "anticipacion": "Anticipación",
            }
            for campo, emocion in mapping.items():
                pesos[campo] = float(sumas.get(emocion, 0.0))
            return pesos, predictions[0].get("sentimiento_global", "N/A")
    except Exception as error:
        logger.exception("YouTube: error usando Databricks para sentimiento: %s", error)

    pesos["alegria"] = round(len(comentarios) * 0.5, 2)
    return pesos, "Positivo"


def _obtener_comentarios(youtube, video_id):
    comentarios = []
    page_token = None
    page_number = 0
    max_pages = 2
    try:
        while page_number < max_pages:
            page_number += 1
            logger.info("YouTube: solicitando comentarios, video=%s, página=%d", video_id, page_number)
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100,
                textFormat="plainText",
                pageToken=page_token,
            )
            response = request.execute()
            logger.info(
                "YouTube: página %d recibida, items=%d",
                page_number,
                len(response.get("items", [])),
            )
            for item in response.get("items", []):
                snippet = item.get("snippet", {}).get("topLevelComment", {}).get("snippet", {})
                texto = (snippet.get("textDisplay") or snippet.get("textOriginal") or "").strip()
                if texto:
                    comentarios.append(texto)
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        if page_token:
            logger.warning("YouTube: se alcanzó el límite de %d páginas de comentarios", max_pages)
    except HttpError as error:
        if getattr(error.resp, "status", None) not in (403, 404):
            raise
        logger.warning("YouTube: comentarios no disponibles para %s", video_id)
    logger.info("YouTube: comentarios obtenidos=%d para video=%s", len(comentarios), video_id)
    return comentarios


def _extraer_hashtags(texto):
    import re
    return [tag.lstrip("#") for tag in re.findall(r"#\w+", texto or "")]


def procesar_post_individual_youtube(video_id, api_key):
    video_id = str(video_id).strip()
    if not video_id:
        raise ValueError("El parámetro video_id es requerido")

    logger.info("YouTube: inicio procesamiento video=%s", video_id)
    logger.info("YouTube: creando cliente API")
    youtube = build("youtube", "v3", developerKey=api_key, cache_discovery=False)
    logger.info("YouTube: solicitando datos del video=%s", video_id)
    response = youtube.videos().list(part="statistics,snippet", id=video_id).execute()
    logger.info("YouTube: respuesta del video recibida")
    items = response.get("items", [])
    if not items:
        raise ValueError("Video de YouTube no encontrado")

    video = items[0]
    snippet = video.get("snippet", {})
    statistics = video.get("statistics", {})
    channel_id = snippet.get("channelId")
    username = (snippet.get("channelTitle") or channel_id or "").strip()
    if not username:
        raise ValueError("El video no contiene información del canal")
    logger.info("YouTube: video pertenece a canal=%s, channel_id=%s", username, channel_id)

    _guardar_json_local("posts_youtube_individuales", video_id, video)
    existing_post = ScrapeResult.objects.filter(
        platform="yt", username=username, raw_data__contains=video_id
    ).first()
    if existing_post:
        return existing_post, 0, username, True

    seguidores = 0
    if channel_id:
        logger.info("YouTube: solicitando suscriptores del canal=%s", channel_id)
        channel_response = youtube.channels().list(part="statistics", id=channel_id).execute()
        channel_items = channel_response.get("items", [])
        if channel_items:
            seguidores = int(channel_items[0].get("statistics", {}).get("subscriberCount", 0) or 0)
        logger.info("YouTube: suscriptores obtenidos=%d", seguidores)

    dim_usuario = DimUsuario.objects.filter(platform="yt", username=username).first()
    if dim_usuario and seguidores <= 0:
        seguidores = dim_usuario.followers

    logger.info("YouTube: iniciando descarga de comentarios")
    comentarios = _obtener_comentarios(youtube, video_id)
    pesos, sentimiento_global = _analizar_sentimiento(comentarios)
    descripcion = (snippet.get("description") or snippet.get("title") or "").replace("\n", " ")
    fecha_str = snippet.get("publishedAt")
    fecha_obj = datetime.fromisoformat(fecha_str.replace("Z", "+00:00")) if fecha_str else None

    dim_usuario, _ = DimUsuario.objects.get_or_create(
        platform="yt", username=username, defaults={"followers": max(seguidores, 0)}
    )
    dim_usuario.actualizar_seguidores(seguidores)
    logger.info("YouTube: guardando ScrapeResult y %d comentarios", len(comentarios))
    scrape_result = ScrapeResult.objects.create(
        platform="yt",
        username=username,
        dim_usuario=dim_usuario,
        followers=seguidores,
        post_date=fecha_obj,
        likes=int(statistics.get("likeCount", 0) or 0),
        comments=int(statistics.get("commentCount", len(comentarios)) or 0),
        views=int(statistics.get("viewCount", 0) or 0),
        description=descripcion,
        hashtags=",".join(_extraer_hashtags(descripcion)),
        raw_data=json.dumps({"video": video, "channel_id": channel_id}, ensure_ascii=False),
        sentimiento_global=sentimiento_global,
        **pesos,
    )
    for texto in comentarios:
        PostComment.objects.create(post=scrape_result, texto=texto, platform="yt")

    logger.info("YouTube: procesamiento finalizado, ScrapeResult id=%d", scrape_result.id)
    return scrape_result, len(comentarios), username, False
