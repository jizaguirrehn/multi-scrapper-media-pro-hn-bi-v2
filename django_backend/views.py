from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.decorators import action
from .models import ScrapeResult, ScraperKey
from .serializers import ScrapeResultSerializer, ScraperKeySerializer
from django.db.models import F, ExpressionWrapper, FloatField
from django.db.models import Count, Avg
from django.db.models.functions import ExtractWeekDay
from django.utils import timezone
import threading
import datetime
from django.http import JsonResponse
from django.db.models import Q

from django_backend.scripts.script_ig import iniciar as iniciar_ig
from django_backend.scripts.script_tk import iniciar as iniciar_tk
from django_backend.scripts.script_x import iniciar as iniciar_x
from django_backend.scripts.script_metricas import mostrar_metricas
from django_backend.scripts.script_historico import mostrar_historico

class ScraperViewSet(viewsets.ViewSet):

    @action(detail=False, methods=['post'])
    def bulk_update(self, request):
        data = request.data
                
        try:
            for platform, purposes in data.items():
                for purpose, keys in purposes.items():
                    # Marcamos las llaves viejas como inactivas para esta plataforma/propósito
                    ScraperKey.objects.filter(platform=platform, purpose=purpose).update(is_active=False)
                    
                    # Creamos las nuevas
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
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)
    
    @action(detail=False, methods=['post'])
    def trigger_extraction(self, request):
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

        queryset = ScrapeResult.objects.all().order_by('-created_at')

        if since:
            try:
                queryset = queryset.filter(created_at__gte=since)
            except Exception:
                pass

        queryset = queryset[:10000]
        serializer = ScrapeResultSerializer(queryset, many=True)
        return Response(serializer.data)
    
    @action(detail=False, methods=['get'])
    def latest_results(self, request):
        since = request.query_params.get('since')
        platform = request.query_params.get('platform')

        queryset = ScrapeResult.objects.all().order_by('-created_at')

        if platform:
            queryset = queryset.filter(platform=platform.lower())

        if since:
            try:
                queryset = queryset.filter(created_at__gt=since)
            except Exception as e:
                print(f"Error filtrando por fecha: {e}")
                pass

        limit = int(request.query_params.get('limit', 100))
        queryset = queryset[:limit]

        serializer = ScrapeResultSerializer(queryset, many=True)
        return Response(serializer.data)
    
    @action(detail=False, methods=['get'], url_path='user_history')
    def user_history(request):
        try:
            criterio = request.GET.get('query', '').strip()
            
            if criterio == '*' or criterio == '':
                posts = ScrapeResult.objects.all().order_by('-created_at')
            else:
                posts = ScrapeResult.objects.filter(
                    Q(username__iregex=criterio)
                ).order_by('-created_at')

            data = []
            for post in posts:
                data.append({
                    'id': post.id,
                    'username': post.username,
                    'platform': getattr(post, 'platform', 'N/A'),
                    'description': getattr(post, 'description', ''),
                    'likes': getattr(post, 'likes', 0),
                    'comments': getattr(post, 'comments', 0),
                    'post_date': getattr(post, 'post_date', 'N/A'),
                    'sentiment': getattr(post, 'sentiment', 'neutral'),
                    'created_at': post.created_at.strftime("%Y-%m-%d %H:%M") if post.created_at else "N/A"
                })

            return JsonResponse(data, safe=False)
            
        except Exception as e:
            print(f"ERROR EN USER_HISTORY: {str(e)}")
            return JsonResponse({'error': str(e)}, status=500)
    
    @action(detail=False, methods=['get'])
    def get_metrics(self, request):
        total_posts = ScrapeResult.objects.count()
        total_profiles = ScrapeResult.objects.values('username').distinct().count()

        avg_eng = ScrapeResult.objects.filter(followers__gt=0).annotate(
            rate=ExpressionWrapper(
                (F('likes') + F('comments')) * 100.0 / F('followers'),
                output_field=FloatField()
            )
        ).aggregate(Avg('rate'))['rate__avg'] or 0

        dist = ScrapeResult.objects.values('platform').annotate(count=Count('id'))
        platform_distribution = {item['platform']: item['count'] for item in dist}

        hace_una_semana = timezone.now() - datetime.timedelta(days=7)
        dias_map = {1: 'Sun', 2: 'Mon', 3: 'Tue', 4: 'Wed', 5: 'Thu', 6: 'Fri', 7: 'Sat'}
        
        volumen_raw = (
            ScrapeResult.objects.filter(created_at__gte=hace_una_semana)
            .annotate(day_num=ExtractWeekDay('created_at'))
            .values('day_num')
            .annotate(count=Count('id'))
        )
        
        weekly_volume = {dias_map[i]: 0 for i in range(1, 8)}
        for item in volumen_raw:
            weekly_volume[dias_map[item['day_num']]] = item['count']

        return Response({
            "total_extracted": total_posts,
            "total_profiles": total_profiles,
            "avg_engagement": round(avg_eng, 2),
            "platform_distribution": platform_distribution,
            "weekly_volume": weekly_volume
        })