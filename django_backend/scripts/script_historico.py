from django_backend.models import ScrapeResult
from django.db.models import Q

def mostrar_historico(criterio):
    """
    Busca posts por nombre de usuario exacto o mediante una expresión regular (regex).
    """
    print("="*60)
    print(f"   BÚSQUEDA DE HISTÓRICO: '{criterio.upper()}'")
    print("="*60)

    posts = ScrapeResult.objects.filter(
        Q(username__iregex=criterio) | Q(username__iexact=criterio)
    ).order_by('-created_at')

    if not posts.exists():
        print(f"[-] No se encontraron resultados para el patrón: '{criterio}'.")
        return

    print(f"{'FECHA':<18} | {'PLATAFORMA':<10} | {'USUARIO':<15} | {'DESCRIPCIÓN'}")
    print("-" * 80)

    for post in posts:
        fecha = post.created_at.strftime("%Y-%m-%d %H:%M")
        plataforma = post.platform.upper()
        usuario_db = post.username
        descripcion = (post.description[:60].replace('\n', ' ') + '...') if post.description and len(post.description) > 60 else (post.description or "Sin descripción")
        
        print(f"{fecha:<18} | {plataforma:<10} | {usuario_db:<15} | {descripcion}")