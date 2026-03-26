import requests
import csv
import os
from datetime import datetime

from apps.scraper.models import ScrapeResult
from django.utils.timezone import make_aware

HOY = datetime.now().strftime("%Y_%m_%d")

def guardar_en_db(target, seguidores, fecha_obj, likes, comms, desc):
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

        ScrapeResult.objects.create(
            platform='ig',
            username=target,
            followers=seguidores,
            post_date=fecha_dt,
            likes=likes,
            comments=comms,
            description=desc
        )
    except Exception as e:
        print(f"Error al guardar en DB (Instagram): {e}")

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
            headers = {
                "x-rapidapi-key": lista_keys[key_actual_index],
                "x-rapidapi-host": host
            }
            
            try:
                response = requests.get(url, headers=headers, params={"username": target}, timeout=15)
                print(f"Respuesta_IG {response.json()}")
                if response.status_code == 429:
                    key_actual_index += 1
                    continue 
                
                if response.status_code == 200:
                    res_data = response.json()
                    user = res_data.get('data', {}).get('user', {}) if 'data' in res_data else res_data.get('user', {})
                    
                    if user:
                        seguidores = user.get('edge_followed_by', {}).get('count', 0)
                        edges = user.get('edge_owner_to_timeline_media', {}).get('edges', [])
                        
                        with open(nombre_archivo, mode='a', newline='', encoding='utf-8-sig') as file_append:
                            writer = csv.writer(file_append)
                            
                            for edge in edges:
                                node = edge.get('node', {})
                                timestamp = node.get('taken_at_timestamp')
                                fecha_dt_obj = datetime.fromtimestamp(timestamp) if timestamp else None
                                fecha_csv = fecha_dt_obj.strftime('%d/%m/%Y') if fecha_dt_obj else "N/A"
                                
                                caption_edges = node.get('edge_media_to_caption', {}).get('edges', [])
                                desc = caption_edges[0].get('node', {}).get('text', "").replace('\n', ' ') if caption_edges else ""
                                
                                likes = node.get('edge_liked_by', {}).get('count', 0)
                                comms = node.get('edge_media_to_comment', {}).get('count', 0)

                                writer.writerow([target, seguidores, fecha_csv, "Post", likes, comms, desc])
                                
                                guardar_en_db(target, seguidores, fecha_dt_obj, likes, comms, desc)
                        
                        print(f" @{target} procesado y guardado en DB.")
                        exito = True
                    else:
                        exito = True 
                else:
                    key_actual_index += 1
            except Exception as e:
                print(f"Error: {e}")
                key_actual_index += 1

def iniciar(mis_apis_keys, lista_perfiles):
    analizar_con_rotacion(mis_apis_keys, lista_perfiles)