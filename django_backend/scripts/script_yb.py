import asyncio
import re
import logging
from datetime import datetime, timedelta
from django.utils.timezone import make_aware
from playwright.async_api import async_playwright
from django_backend.models import ScrapeResult
from asgiref.sync import sync_to_async

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class YouTubeScraperService:
    def __init__(self):
        self.hoy = datetime.now().strftime("%Y_%m_%d")

    def _parse_numeric_text(self, text, field_name=""):
        if not text: return 0
        clean_text = text.lower().strip().replace('(', '').replace(')', '')
        
        # 1. Identificar multiplicador
        multiplier = 1
        if 'm' in clean_text and 'mil' not in clean_text:
            multiplier = 1000000
        elif 'k' in clean_text or 'mil' in clean_text:
            multiplier = 1000

        # 2. Extraer bloque numérico
        match = re.search(r'([\d\.,]+)', clean_text)
        if not match: return 0
        num_str = match.group(1)

        # 3. Lógica de limpieza inteligente
        if multiplier > 1:
            num_str = num_str.replace(',', '.')
        else:
            num_str = num_str.replace('.', '').replace(',', '')

        try:
            val = float(num_str)
            result = int(val * multiplier)
            return result
        except:
            return 0

    def _parse_youtube_date(self, date_text):
        now = datetime.now()
        units = {'segundo': 'seconds', 'minuto': 'minutes', 'hora': 'hours',
                 'día': 'days', 'semana': 'weeks', 'mes': 'months', 'año': 'years'}
        match = re.search(r'(\d+)\s+(\w+)', date_text.lower())
        if not match: return make_aware(now)
        quantity = int(match.group(1))
        unit_text = match.group(2)
        delta_kwargs = {}
        for key, value in units.items():
            if key in unit_text:
                if value == 'months': delta_kwargs['days'] = quantity * 30
                elif value == 'years': delta_kwargs['days'] = quantity * 365
                else: delta_kwargs[value] = quantity
                break
        return make_aware(now - timedelta(**(delta_kwargs or {'seconds': 0})))

    def guardar_en_db_sync(self, target, seguidores_raw, fecha_dt, views, likes, comms, desc):
        try:
            seguidores_limpios = self._parse_numeric_text(seguidores_raw, "Followers")
            ScrapeResult.objects.create(
                platform='yt',
                username=target,
                followers=seguidores_limpios,
                post_date=fecha_dt,
                likes=likes,
                comments=comms,
                views=views,
                description=desc
            )
            
        except Exception as e:
            print(f" Error DB: {e}")

    async def procesar_video(self, context, video_url, usuario, seguidores_raw):
        """Procesa un video individual en una pestaña nueva."""
        page = await context.new_page()
        try:
            # Bloquear imágenes y fuentes para ganar velocidad
            await page.route("**/*.{png,jpg,jpeg,svg,woff,woff2}", lambda route: route.abort())
            
            await page.goto(video_url, wait_until="domcontentloaded")
            
            # Scroll rápido para disparar carga de comentarios
            await page.mouse.wheel(0, 1200)
            
            # Esperar lo mínimo necesario para que los datos existan
            try:
                await page.wait_for_selector('h1.ytd-watch-metadata', timeout=5000)
            except: pass

            video_data = await page.evaluate('''() => {
                const title = document.querySelector('h1.ytd-watch-metadata')?.innerText || "";
                const likeText = document.querySelector('segmented-like-dislike-button-view-model')?.innerText || "0";
                const viewsExact = document.querySelector('#info-container span.style-scope.yt-formatted-string:nth-child(1)')?.innerText || 
                                   document.querySelector('#metadata-line span:nth-child(1)')?.innerText || "0";
                const countEl = document.querySelector('ytd-comments-header-renderer #count span:nth-child(1)') || 
                                document.querySelector('#comments #count yt-formatted-string');
                return { title, likeText, commText: countEl ? countEl.innerText : "0", viewsExact };
            }''')

            likes = self._parse_numeric_text(video_data['likeText'], "Likes")
            comms = self._parse_numeric_text(video_data['commText'], "Comments")
            vistas = self._parse_numeric_text(video_data['viewsExact'], "Views Exactas")
            fecha_obj = self._parse_youtube_date("1 día")

            await sync_to_async(self.guardar_en_db_sync, thread_sensitive=True)(
                usuario, seguidores_raw, fecha_obj, vistas, likes, comms, video_data['title']
            )
        finally:
            await page.close()

    async def scrape_and_save(self, usuario):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--disable-http2'])
            context = await browser.new_context(viewport={'width': 1280, 'height': 720}, locale="es-ES")
            
            try:
                main_page = await context.new_page()
                await main_page.goto(f"https://www.youtube.com/@{usuario}/videos", wait_until="networkidle")
                
                await main_page.wait_for_selector('.page-header-view-model-wiz__metadata, #subscriber-count', timeout=5000)
                seguidores_raw = await main_page.evaluate('''() => {
                    const allSpans = Array.from(document.querySelectorAll('span'));
                    const subSpan = allSpans.find(s => s.innerText.toLowerCase().includes('suscriptores'));
                    return subSpan ? subSpan.innerText : "0";
                }''')

                # Obtener links
                links = await main_page.evaluate('''() => {
                    return Array.from(document.querySelectorAll('ytd-rich-item-renderer a#video-title-link')).map(a => a.href).slice(0, 5); 
                }''')
                await main_page.close()

                tareas = [self.procesar_video(context, url, usuario, seguidores_raw) for url in links]
                await asyncio.gather(*tareas)

                await browser.close()
            except Exception as e:
                logger.error(f"Error en {usuario}: {e}")
                await browser.close()

def iniciar_yt(usuarios):
    scraper = YouTubeScraperService()
    for u in (usuarios if isinstance(usuarios, list) else [usuarios]):
        asyncio.run(scraper.scrape_and_save(u))