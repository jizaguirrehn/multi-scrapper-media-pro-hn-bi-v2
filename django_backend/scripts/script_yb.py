import asyncio
import re
import logging
import json
import requests
from datetime import datetime
from django.utils.timezone import make_aware
from django_backend.models import ScrapeResult

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class YouTubeScraperService:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "es-ES,es;q=0.9"
        }

    # ------------------------------------------------------------------ #
    #  PARSERS                                                             #
    # ------------------------------------------------------------------ #

    MESES_ES = {
        "ene": 1, "feb": 2, "mar": 3, "abr": 4,
        "may": 5, "jun": 6, "jul": 7, "ago": 8,
        "sep": 9, "oct": 10, "nov": 11, "dic": 12
    }

    def __parsear_fecha_es(self, texto, field_name=""):
        partes = texto.lower().replace(".", "").replace(" de ", " ").split()
        dia = int(partes[0])
        mes = self.MESES_ES[partes[1][:3]]
        anio = int(partes[2])
        return datetime(anio, mes, dia)

    def _parse_numeric_text(self, text, field_name=""):
        """Convierte strings como '1.2M', '50K' o '125,432' en enteros."""
        if not text:
            return 0
        clean = str(text).lower().strip().replace('(', '').replace(')', '').replace('\xa0', ' ')

        if clean.isdigit():
            return int(clean)

        multiplier = 1
        if 'm' in clean or 'mill' in clean:
            multiplier = 1_000_000
        elif 'k' in clean or 'mil' in clean:
            multiplier = 1_000

        match = re.search(r'([\d\.,]+)', clean)
        if not match:
            return 0

        num_str = (
            match.group(1).replace(',', '.')
            if multiplier > 1
            else match.group(1).replace('.', '').replace(',', '')
        )

        try:
            return int(float(num_str) * multiplier)
        except ValueError:
            return 0

    def _parse_youtube_date(self, date_text):
        """Devuelve la fecha actual como timezone-aware (ampliar si se necesita parseo real)."""
        return make_aware(datetime.now())

    # ------------------------------------------------------------------ #
    #  EXTRACCIÓN DE MÉTRICAS DE UN VIDEO (solo requests)                 #
    # ------------------------------------------------------------------ #

    def _fetch_video_metrics(self, video_id: str) -> dict:
        """
        Descarga la página del video y extrae vistas, likes y comentarios
        directamente desde ytInitialData / ytInitialPlayerResponse.
        Devuelve un dict con keys: title, views, likes, comments.
        """
        url = f"https://www.youtube.com/watch?v={video_id}"
        try:
            resp = requests.get(url, headers=self.headers, timeout=15)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Error al descargar video {video_id}: {e}")
            return {"title": "", "views": 0, "likes": 0, "comments": 0}

        # --- Vistas y título desde ytInitialPlayerResponse ---
        views_raw, title = "0", ""
        match_player = re.search(r'ytInitialPlayerResponse\s*=\s*({.+?});\s*(?:var |</script)', resp.text)
        if match_player:
            try:
                player = json.loads(match_player.group(1))
                views_raw = player.get("videoDetails", {}).get("viewCount", "0")
                title     = player.get("videoDetails", {}).get("title", "")
            except json.JSONDecodeError:
                pass

        # --- Likes y comentarios desde ytInitialData ---
        likes_raw, comm_raw, date_raw = "0", "0", "0"
        match_data = re.search(r'ytInitialData\s*=\s*({.+?});', resp.text)
        if match_data:
            try:
                data_v = json.loads(match_data.group(1))
                date_raw = data_v["contents"]["twoColumnWatchNextResults"]["results"]["results"]["contents"][0]["videoPrimaryInfoRenderer"]["dateText"]["simpleText"]
                panels  = data_v.get("engagementPanels", [])

                # Comentarios: primer panel → contextualInfo
                try:
                    comm_raw = (
                        panels[0]
                        ["engagementPanelSectionListRenderer"]
                        ["header"]
                        ["engagementPanelTitleHeaderRenderer"]
                        ["contextualInfo"]["runs"][0]["text"]
                    )
                except (KeyError, IndexError):
                    pass

                # Likes: panel de descripción estructurada → factoid[0]
                for panel in panels:
                    try:
                        factoid = (
                            panel
                            ["engagementPanelSectionListRenderer"]
                            ["content"]
                            ["structuredDescriptionContentRenderer"]
                            ["items"][0]
                            ["videoDescriptionHeaderRenderer"]
                            ["factoid"][0]
                            ["factoidRenderer"]
                            ["value"]["simpleText"]
                        )
                        likes_raw = factoid
                        break
                    except (KeyError, IndexError):
                        continue

            except json.JSONDecodeError:
                pass

        if not title:
            title = resp.url  # fallback

        return {
            "title":    title,
            "views":    self._parse_numeric_text(views_raw, "views"),
            "likes":    self._parse_numeric_text(likes_raw, "likes"),
            "comments": self._parse_numeric_text(comm_raw,  "comments"),
            "date" : self.__parsear_fecha_es(date_raw, "date"),
        }

    # ------------------------------------------------------------------ #
    #  GUARDADO EN DB                                                      #
    # ------------------------------------------------------------------ #

    def _guardar_en_db(self, usuario, seguidores_raw, fecha_dt,
                       views, likes, comments, title):
        try:
            seguidores = self._parse_numeric_text(seguidores_raw)
            ScrapeResult.objects.create(
                platform    = "yt",
                username    = usuario,
                followers   = seguidores,
                post_date   = fecha_dt,
                likes       = likes,
                comments    = comments,
                views       = views,
                description = title,
            )
            logger.info(
                f"DB ✔ [{usuario}] {title[:40]!r} | "
                f"V:{views:,} L:{likes:,} C:{comments:,}"
            )
        except Exception as e:
            logger.error(f"Error al guardar en DB: {e}")

    def scrape_and_save(self, usuario: str, max_videos: int = 15):
        """
        1. Descarga la página /videos del canal con requests.
        2. Extrae metadatos y lista de IDs de videos.
        3. Para cada video: extrae métricas y guarda en DB.
        """
        url_canal = f"https://www.youtube.com/@{usuario}/videos"
        logger.info(f"Conectando a {url_canal} ...")

        try:
            res = requests.get(url_canal, headers=self.headers, timeout=15)
            res.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Fallo al obtener canal @{usuario}: {e}")
            return

        match = re.search(r'ytInitialData\s*=\s*({.+?});', res.text)
        if not match:
            logger.error("No se encontró ytInitialData en la página del canal.")
            return

        try:
            data = json.loads(match.group(1))
        except json.JSONDecodeError as e:
            logger.error(f"JSON inválido en ytInitialData: {e}")
            return

        # --- Encabezado del canal ---
        seguidores_raw = "0"
        try:
            header_vm    = data["header"]["pageHeaderRenderer"]["content"]["pageHeaderViewModel"]
            nombre       = header_vm["title"]["dynamicTextViewModel"]["text"]["content"]
            seguidores_raw = (
                header_vm["metadata"]
                ["contentMetadataViewModel"]
                ["metadataRows"][1]
                ["metadataParts"][0]
                ["text"]["content"]
            )
            logger.info(f"Canal: {nombre} | Suscriptores: {seguidores_raw}")
        except KeyError:
            logger.warning("No se pudo extraer el encabezado del canal.")

        # --- Lista de video IDs ---
        video_ids = []
        try:
            tabs = data["contents"]["twoColumnBrowseResultsRenderer"]["tabs"]
            video_tab = next(
                tab for tab in tabs
                if "richGridRenderer" in tab.get("tabRenderer", {}).get("content", {})
            )
            items = video_tab["tabRenderer"]["content"]["richGridRenderer"]["contents"]

            for item in items:
                if len(video_ids) >= max_videos:
                    break
                if "richItemRenderer" in item:
                    vid = item["richItemRenderer"]["content"]["videoRenderer"]["videoId"]
                    video_ids.append(vid)

        except (KeyError, StopIteration) as e:
            logger.error(f"Error al extraer lista de videos: {e}")
            return

        logger.info(f"{len(video_ids)} videos encontrados para @{usuario}. Procesando...")

        # --- Procesar cada video ---
        for video_id in video_ids:
            metrics  = self._fetch_video_metrics(video_id)
            fecha_dt = self._parse_youtube_date("")

            self._guardar_en_db(
                usuario      = usuario,
                seguidores_raw = seguidores_raw,
                fecha_dt     = metrics["date"],
                views        = metrics["views"],
                likes        = metrics["likes"],
                comments     = metrics["comments"],
                title        = metrics["title"],
            )


# ------------------------------------------------------------------ #
#  PUNTO DE ENTRADA                                                    #
# ------------------------------------------------------------------ #

def iniciar_yt(usuarios, max_videos: int = 15):
    """
    Acepta un string o lista de usernames de YouTube y lanza el scraping.
    Ejemplo:
        iniciar_yt("shinfujiyamaReal")
        iniciar_yt(["shinfujiyamaReal", "otroCanal"])
    """
    scraper = YouTubeScraperService()
    lista   = usuarios if isinstance(usuarios, list) else [usuarios]

    for usuario in lista:
        logger.info(f"=== Iniciando scraping para @{usuario} ===")
        scraper.scrape_and_save(usuario, max_videos=max_videos)
        logger.info(f"=== Finalizado @{usuario} ===")