import requests
import csv
import json
import os
from datetime import datetime

from apps.scraper.models import ScrapeResult
from django.utils.timezone import make_aware 

ARCHIVO_IDS = "usuarios_fb_registrados.json"
HOY = datetime.now().strftime("%Y_%m_%d")

def guardar_en_db(target, seguidores, fecha_str, likes, comentarios,vistas, desc):
    try:
        fecha_dt = None
        if fecha_str and fecha_str != "N/A":
            try:
                naive_datetime = datetime.strptime(fecha_str, '%d/%m/%Y %H:%M:%S')
                fecha_dt = make_aware(naive_datetime)
            except Exception as e_fecha:
                print(f"Error parseando fecha {fecha_str}: {e_fecha}")

        ScrapeResult.objects.create(
            platform='fb',
            username=target,
            followers=seguidores if isinstance(seguidores, int) else 0,
            post_date=fecha_dt,
            likes=likes,
            comments=comentarios,
            views=vistas,
            description=desc
        )
    except Exception as e:
        print(f"Error crítico al guardar en DB (Facebook): {e}")

def cargar_cache_ids():
    if os.path.exists(ARCHIVO_IDS):
        with open(ARCHIVO_IDS, 'r') as f:
            return json.load(f)
    return {}

def guardar_cache_ids(cache):
    with open(ARCHIVO_IDS, 'w') as f:
        json.dump(cache, f, indent=4)

def analizar_facebook_optimizado(api_key, lista_targets):
    cache_uids = cargar_cache_ids()
    nombre_csv = f"results/datos_fb_{HOY}.csv"

    if not os.path.exists('results'): os.makedirs('results')
    
    if not os.path.exists(nombre_csv):
        with open(nombre_csv, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(['USUARIO', 'SEGUIDORES', 'FECHA_POST', 'LIKES', 'COMENTARIOS', 'VISTAS', 'DESCRIPCION'])

    # Headers globales ya que solo usamos una key
    headers = {
        "x-rapidapi-key": api_key, 
        "x-rapidapi-host": "facebook-scraper3.p.rapidapi.com"
    }

    for target in lista_targets:
        info_perfil = cache_uids.get(target)
        
        # 1. Obtener ID del usuario si no está en caché
        if not info_perfil:
            print(f"Buscando ID para @{target}...")
            querystring = {"url": f"https://www.facebook.com/{target}"}
            try:
                res = requests.get("https://facebook-scraper3.p.rapidapi.com/page/details", 
                                   headers=headers, params=querystring, timeout=10)
                if res.status_code == 200:
                    user_info = res.json()
                    user_info = user_info.get("results", {})
                    info_perfil = {
                        "pageId": user_info.get('page_id'),
                        "followers": user_info.get('followers', 0)
                    }
                    cache_uids[target] = info_perfil
                    guardar_cache_ids(cache_uids)
                else:
                    print(f"Error obteniendo perfil: {res.status_code}")
                    continue
            except Exception as e:
                print(f"Error de conexión perfil: {e}")
                continue

        # 2. Obtener Posts con Cursor (7 iteraciones)
        if info_perfil:
            page_id = info_perfil.get("pageId")
            seguidores = info_perfil.get("followers")
            cursor = None
            
            
            for iteration in range(1, 8):
                params = {"page_id": page_id}
                if cursor: params["cursor"] = cursor

                try:
                    res_p = requests.get("https://facebook-scraper3.p.rapidapi.com/page/posts", 
                                        headers=headers, params=params, timeout=15)
                    
                    if res_p.status_code == 200:
                        data = res_p.json()
                        items = data.get('results', [])
                        cursor = data.get('cursor')
                        
                        if items:
                            with open(nombre_csv, mode='a', newline='', encoding='utf-8-sig') as f:
                                writer = csv.writer(f)
                                for item in items:
                                    likes = item.get('reactions_count', 0)
                                    coments = item.get('comments_count', 0)
                                    vistas = item.get('reshare_count', 0)
                                    ts = item.get('timestamp')
                                    fecha_txt = datetime.fromtimestamp(int(ts)).strftime('%d/%m/%Y %H:%M:%S') if ts else "N/A"
                                    desc = item.get('message_rich', '').replace('\n', ' ')
                                    
                                    writer.writerow([target, seguidores, fecha_txt, likes, coments, vistas, desc])
                                    guardar_en_db(target, seguidores, fecha_txt, likes, coments, vistas, desc)
                        
                        
                        if not cursor:
                            print("  No hay más posts disponibles.")
                            break
                    else:
                        print(f"  Error en nivel {iteration}: {res_p.status_code}")
                        break # Si la key falla (429), detenemos el bucle
                except Exception as e:
                    print(f"  Error de conexión en nivel {iteration}: {e}")
                    break

def iniciar_fb(key, lista_perfiles):
    print(f"\n--- INICIANDO MÓDULO FACEBOOK (SINGLE KEY) ---")
    analizar_facebook_optimizado(key, lista_perfiles)