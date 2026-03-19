import email
from jose import jwt

from django.db.models import Q
from django.contrib.auth.models import User
from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.decorators import action, api_view, permission_classes
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework_simplejwt.tokens import RefreshToken

from django_backend.scripts.script_fb import iniciar_fb
from .models import ScrapeResult, ScraperKey
from .serializers import ScrapeResultSerializer, ScraperKeySerializer
from django.db.models import F, ExpressionWrapper, FloatField
from django.db.models import Count, Avg
from django.db.models.functions import ExtractWeekDay
from django.utils import timezone
import threading
import datetime
from django.http import JsonResponse
import logging
import statistics

logger = logging.getLogger(__name__)

from django_backend.scripts.script_ig import iniciar as iniciar_ig
from django_backend.scripts.script_tk import iniciar as iniciar_tk
from django_backend.scripts.script_x import iniciar as iniciar_x
from django_backend.scripts.script_yb import iniciar_yt

class ScraperViewSet(viewsets.ViewSet):

    @action(detail=False, methods=['post'])
    def bulk_update(self, request):
        data = request.data
                
        try:
            for platform, purposes in data.items():
                for purpose, keys in purposes.items():
                    logger.info(f"Updating keys for platform: {platform}, purpose: {purpose}")
                    ScraperKey.objects.filter(platform=platform, purpose=purpose).update(is_active=False)
                    
                    for k in keys:
                        if k.strip():
                            ScraperKey.objects.create(
                                platform=platform,
                                purpose=purpose,
                                key_value=k.strip(),
                                is_active=True
                            )
            return Response({'status': 'Keys updated successfully'}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"Error updating keys: {e}")
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)
    
    @action(detail=False, methods=['post'])
    def trigger_extraction(self, request):
        if request.user.groups.filter(name='Colaborador').exists():
            return Response({"error": "No tienes permiso para iniciar extracciones"}, status=403)
        platform = request.data.get('platform')
        targets = request.data.get('targets', [])
        
        start_time = timezone.now().isoformat()

        if not platform or not targets:
            return Response({'error': 'Faltan parámetros (platform o targets)'}, 
                            status=status.HTTP_400_BAD_REQUEST)

        thread = None
        target_function = None

        if platform == 'ig':
            keys = list(ScraperKey.objects.filter(platform='ig', is_active=True).values_list('key_value', flat=True))
            if keys:
                thread = threading.Thread(target=iniciar_ig, args=(keys, targets))
        elif platform == 'tk':
            keys_search = list(ScraperKey.objects.filter(platform='tk', purpose='search', is_active=True).values_list('key_value', flat=True))
            keys_posts = list(ScraperKey.objects.filter(platform='tk', purpose='posts', is_active=True).values_list('key_value', flat=True))
            
            if keys_search and keys_posts:
                print(f"DEBUG TikTok: Search Keys: {len(keys_search)}, Post Keys: {len(keys_posts)}")
                thread = threading.Thread(target=iniciar_tk, args=(keys_search, keys_posts, targets))
        elif platform == 'x':
            keys_search = list(ScraperKey.objects.filter(platform='x', purpose='search', is_active=True).values_list('key_value', flat=True))
            keys_posts = list(ScraperKey.objects.filter(platform='x', purpose='posts', is_active=True).values_list('key_value', flat=True))
            if keys_search and keys_posts:
                thread = threading.Thread(target=iniciar_x, args=(keys_search, keys_posts, targets))
            else:
                return Response({'error': 'Faltan llaves de X (search o posts)'}, status=400)
        elif platform == 'yt':
            thread = threading.Thread(target=iniciar_yt, args=(targets,))
        elif platform == 'fb':
            keys = list(ScraperKey.objects.filter(platform='fb', is_active=True).values_list('key_value', flat=True))
            if keys:
                thread = threading.Thread(target=iniciar_fb, args=(keys[0], targets))

        if thread:
            thread.daemon = True
            thread.start()
            return Response({
                'status': 'Extracción iniciada', 
                'platform': platform,
                'started_at': start_time
            }, status=status.HTTP_202_ACCEPTED)
        
        return Response({'error': 'No se pudo iniciar el hilo. Revisa las llaves.'}, status=400)

    
    @action(detail=False, methods=['get'])
    def latest_results(self, request):
        since = request.query_params.get('since')
        platform = request.query_params.get('platform')
        limit = int(request.query_params.get('limit', 1000))

        queryset = ScrapeResult.objects.all().order_by('-created_at')

        if platform:
            queryset = queryset.filter(platform=platform.lower())

        if since:
            try:
                queryset = queryset.filter(created_at__gt=since)
            except Exception as e:
                logger.error(f"Error filtrando por fecha: {e}")

        queryset = queryset[:limit]
        serializer = ScrapeResultSerializer(queryset, many=True)
        return Response(serializer.data)
    
    @action(detail=False, methods=['get'], url_path='user_history')
    def api_historico_usuario(self, request):
        if request.user.groups.filter(name='Colaborador').exists():
            return Response({"error": "No tienes permiso para iniciar extracciones"}, status=403)
        try:
            criterio = request.GET.get('query', '').strip()
            
            if criterio in ('*', '', '.*'):
                posts = ScrapeResult.objects.all().order_by('-created_at')[:500]
            else:
                posts = ScrapeResult.objects.filter(
                    Q(username__iregex=criterio)
                ).order_by('-created_at')[:500]

            serializer = ScrapeResultSerializer(posts, many=True)
            return Response(serializer.data)

        except Exception as e:
            logger.error(f"Error en api_historico_usuario: {str(e)}")
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
    @action(detail=False, methods=['get'])
    def get_metrics(self, request):
        if not request.user.groups.filter(name='Directora').exists():
            return Response({"error": "No tienes permiso para iniciar extracciones"}, status=403)
        total_posts = ScrapeResult.objects.count()
        total_profiles = ScrapeResult.objects.values('username').distinct().count()

        # Engagement: Añadimos .order_by() para evitar conflictos de ordenamiento
        queryset_engagement = ScrapeResult.objects.filter(followers__gt=0).annotate(
            engagement_val=ExpressionWrapper(
                (F('likes') + F('comments')) * 100.0 / F('followers'),
                output_field=FloatField()
            )
        ).order_by().values_list('engagement_val', flat=True)

        engagement_list = list(queryset_engagement)
        median_engagement = statistics.median(engagement_list) if engagement_list else 0

        # Distribución por plataforma: CORRECCIÓN CRUCIAL PARA SQL SERVER
        dist = ScrapeResult.objects.values('platform').annotate(
            count=Count('id')
        ).order_by() # <-- Limpia el ORDER BY implícito que causaba el error
        
        platform_distribution = {item['platform']: item['count'] for item in dist}

        # Volumen semanal: CORRECCIÓN CRUCIAL PARA SQL SERVER
        hace_una_semana = timezone.now() - datetime.timedelta(days=7)
        dias_map = {1: 'Sun', 2: 'Mon', 3: 'Tue', 4: 'Wed', 5: 'Thu', 6: 'Fri', 7: 'Sat'}
        
        volumen_raw = (
            ScrapeResult.objects.filter(created_at__gte=hace_una_semana)
            .annotate(day_num=ExtractWeekDay('created_at'))
            .values('day_num')
            .annotate(count=Count('id'))
            .order_by() # <-- Evita que intente ordenar por 'created_at' fuera del GROUP BY
        )
        
        weekly_volume = {dias_map[i]: 0 for i in range(1, 8)}
        for item in volumen_raw:
            # SQL Server a veces devuelve day_num como float o int según el driver
            d_idx = int(item['day_num'])
            weekly_volume[dias_map[d_idx]] = item['count']

        return Response({
            "total_extracted": total_posts,
            "total_profiles": total_profiles,
            "avg_engagement": round(median_engagement, 2),
            "platform_distribution": platform_distribution,
            "weekly_volume": weekly_volume
        })
    
    @action(detail=False, methods=['get'], permission_classes=[AllowAny])
    def public_status(self, request):
        return Response({"status": "Servidor Vivo", "version": "1.5.0"})
    
@api_view(['POST'])
@permission_classes([AllowAny])
def azure_login(request):
    token = request.data.get('access_token')
    
    if not token:
        return Response({"error": "No token provided"}, status=400)
    
    try:
        # Decodificación relajada para diagnóstico
        payload = jwt.decode(
            token, 
            None, 
            options={
                "verify_signature": False, 
                "verify_aud": False, 
                "verify_iss": False, 
                "verify_at_hash": False
            }
        )
        
        email = payload.get('email') or payload.get('preferred_username')
        # 2. Extraemos el nombre del payload para que no de error
        full_name = payload.get('name', 'Usuario Loto')
        
        if not email:
            return Response({"error": "No email in token"}, status=400)

        username = email.split('@')[0]

        # 3. Usamos la variable full_name que acabamos de extraer
        user, created = User.objects.get_or_create(
            email=email,
            defaults={
                'username': username, 
                'first_name': full_name
            }
        )

        refresh = RefreshToken.for_user(user)

        return Response({
            'refresh': str(refresh),
            'access': str(refresh.access_token),
            'is_new_user': created,
            'user': {
                'email': user.email,
                'name': user.first_name
            }
        })
    except Exception as e:
        logger.error(f"Error en azure_login: {str(e)}")
        # Es mejor devolver el error real en el mensaje durante pruebas
        return Response({'error': 'Token inválido', 'details': str(e)}, status=400)
