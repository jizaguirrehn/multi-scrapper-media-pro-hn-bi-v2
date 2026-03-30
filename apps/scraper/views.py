import email
from jose import jwt

from django.db.models import Q
from django.contrib.auth import authenticate
from django.contrib.auth.models import User
from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.decorators import action, api_view, permission_classes
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework_simplejwt.tokens import RefreshToken

from apps.scraper.services.script_fb import iniciar_fb
from .models import ScrapeResult, ScraperKey, ExtractionRequestLog
from .serializers import (
    ScrapeResultSerializer,
    ScraperKeySerializer,
    RegisterSerializer,
    LoginSerializer,
)
from django.db.models import F, ExpressionWrapper, FloatField
from django.db.models import Count, Avg
from django.db.models.functions import ExtractWeekDay
from django.utils import timezone
import threading
import datetime
from django.http import JsonResponse
import logging
import statistics
import requests

logger = logging.getLogger(__name__)

NO_PERMISSION_EXTRACTION_MSG = "No tienes permiso para iniciar extracciones"
AZURE_OPENID_CONFIG_URL = "https://login.microsoftonline.com/common/v2.0/.well-known/openid-configuration"

from apps.scraper.services.script_ig import iniciar as iniciar_ig
from apps.scraper.services.script_tk import iniciar as iniciar_tk
from apps.scraper.services.script_x import iniciar as iniciar_x
from apps.scraper.services.script_yb import iniciar_yt


def _get_client_ip(request):
    forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if forwarded_for:
        return forwarded_for.split(',')[0].strip()
    return request.META.get('REMOTE_ADDR', '')


def _update_request_log(request_log, status_value, detail):
    request_log.status = status_value
    request_log.detail = detail
    request_log.save(update_fields=['status', 'detail'])


def _build_extraction_thread(platform, targets):
    if platform == 'ig':
        keys = list(ScraperKey.objects.filter(platform='ig', is_active=True).values_list('key_value', flat=True))
        if keys:
            return threading.Thread(target=iniciar_ig, args=(keys, targets)), None
        return None, None

    if platform == 'tk':
        keys_search = list(ScraperKey.objects.filter(platform='tk', purpose='search', is_active=True).values_list('key_value', flat=True))
        keys_posts = list(ScraperKey.objects.filter(platform='tk', purpose='posts', is_active=True).values_list('key_value', flat=True))
        if keys_search and keys_posts:
            logger.info("TikTok keys loaded. search=%s posts=%s", len(keys_search), len(keys_posts))
            return threading.Thread(target=iniciar_tk, args=(keys_search, keys_posts, targets)), None
        return None, None

    if platform == 'x':
        keys_search = list(ScraperKey.objects.filter(platform='x', purpose='search', is_active=True).values_list('key_value', flat=True))
        keys_posts = list(ScraperKey.objects.filter(platform='x', purpose='posts', is_active=True).values_list('key_value', flat=True))
        if keys_search and keys_posts:
            return threading.Thread(target=iniciar_x, args=(keys_search, keys_posts, targets)), None
        return None, 'Faltan llaves de X (search o posts)'

    if platform == 'yt':
        return threading.Thread(target=iniciar_yt, args=(targets,)), None

    if platform == 'fb':
        keys = list(ScraperKey.objects.filter(platform='fb', is_active=True).values_list('key_value', flat=True))
        if keys:
            return threading.Thread(target=iniciar_fb, args=(keys[0], targets)), None

    return None, None


def _get_azure_signing_key(token):
    header = jwt.get_unverified_header(token)
    kid = header.get('kid')
    if not kid:
        raise ValueError('Token sin kid en el header')

    oidc_config_resp = requests.get(AZURE_OPENID_CONFIG_URL, timeout=10)
    oidc_config_resp.raise_for_status()
    jwks_uri = oidc_config_resp.json().get('jwks_uri')
    if not jwks_uri:
        raise ValueError('No se encontró jwks_uri en OpenID configuration')

    jwks_resp = requests.get(jwks_uri, timeout=10)
    jwks_resp.raise_for_status()
    keys = jwks_resp.json().get('keys', [])

    for key in keys:
        if key.get('kid') == kid:
            return key

    raise ValueError('No se encontró llave pública para validar la firma del token')


def _decode_azure_access_token(token):
    signing_key = _get_azure_signing_key(token)
    return jwt.decode(
        token,
        signing_key,
        algorithms=[signing_key.get('alg', 'RS256')],
        options={
            'verify_aud': False,
            'verify_iss': False,
            'verify_at_hash': False,
        },
    )

class ScraperViewSet(viewsets.ViewSet):

    @action(detail=False, methods=['post'])
    def bulk_update(self, request):
        if not request.user.groups.filter(name__in=['Admin_Scraper']).exists():
            return Response({"error": "No tienes permiso para actualizar las llaves"}, status=403)
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
        platform = (request.data.get('platform') or '').lower()
        targets = request.data.get('targets', [])
        request_log = ExtractionRequestLog.objects.create(
            user=request.user if request.user.is_authenticated else None,
            platform=platform,
            targets=targets if isinstance(targets, list) else [targets],
            ip_address=_get_client_ip(request),
            status='PENDING',
        )

        if not request.user.groups.filter(name__in=['Admin_Scraper', 'Usuario', 'Gerente', 'Director']).exists():
            _update_request_log(request_log, 'DENIED', 'No tiene permiso para iniciar extracciones')
            return Response({"error": NO_PERMISSION_EXTRACTION_MSG}, status=403)
        
        start_time = timezone.now().isoformat()

        if not platform or not targets:
            _update_request_log(request_log, 'INVALID', 'Faltan parametros platform o targets')
            return Response({'error': 'Faltan parámetros (platform o targets)'}, 
                            status=status.HTTP_400_BAD_REQUEST)

        thread, thread_error = _build_extraction_thread(platform, targets)
        if thread_error:
            _update_request_log(request_log, 'ERROR', thread_error)
            return Response({'error': thread_error}, status=400)

        if thread:
            thread.daemon = True
            thread.start()
            _update_request_log(request_log, 'STARTED', 'Extraccion iniciada')
            return Response({
                'status': 'Extracción iniciada', 
                'platform': platform,
                'started_at': start_time
            }, status=status.HTTP_202_ACCEPTED)
        
        _update_request_log(request_log, 'ERROR', 'No se pudo iniciar el hilo. Revisa las llaves.')
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
        if not request.user.groups.filter(name__in=['Admin_Scraper', 'Usuario', 'Gerente', 'Director']).exists():
            return Response({"error": NO_PERMISSION_EXTRACTION_MSG}, status=403)
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
        if not request.user.groups.filter(name__in=["Director", "Gerente", "Admin_Scraper"]).exists():
            return Response({"error": NO_PERMISSION_EXTRACTION_MSG}, status=403)
        total_posts = ScrapeResult.objects.count()
        total_profiles = ScrapeResult.objects.values('username').distinct().count()

        queryset_engagement = ScrapeResult.objects.filter(followers__gt=0).annotate(
            engagement_val=ExpressionWrapper(
                (F('likes') + F('comments')) * 100.0 / F('followers'),
                output_field=FloatField()
            )
        ).order_by().values_list('engagement_val', flat=True)

        engagement_list = list(queryset_engagement)
        median_engagement = statistics.median(engagement_list) if engagement_list else 0

        dist = ScrapeResult.objects.values('platform').annotate(
            count=Count('id')
        ).order_by() 
        
        platform_distribution = {item['platform']: item['count'] for item in dist}

        hace_una_semana = timezone.now() - datetime.timedelta(days=7)
        dias_map = {1: 'Sun', 2: 'Mon', 3: 'Tue', 4: 'Wed', 5: 'Thu', 6: 'Fri', 7: 'Sat'}
        
        volumen_raw = (
            ScrapeResult.objects.filter(created_at__gte=hace_una_semana)
            .annotate(day_num=ExtractWeekDay('created_at'))
            .values('day_num')
            .annotate(count=Count('id'))
            .order_by()
        )
        
        weekly_volume = {dias_map[i]: 0 for i in range(1, 8)}
        for item in volumen_raw:
            d_idx = int(item['day_num'])
            weekly_volume[dias_map[d_idx]] = item['count']

        # Obtener usuarios con cantidad de llamados desde ExtractionRequestLog (últimos 7 días)
        user_calls_raw = (
            ExtractionRequestLog.objects
            .filter(created_at__gte=hace_una_semana)
            .values('user__username')
            .annotate(call_count=Count('id'))
            .order_by('-call_count')
        )
        
        users_api_calls = {
            (item['user__username'] or 'Anonymous'): item['call_count']
            for item in user_calls_raw
        }

        return Response({
            "total_extracted": total_posts,
            "total_profiles": total_profiles,
            "avg_engagement": round(median_engagement, 2),
            "platform_distribution": platform_distribution,
            "weekly_volume": weekly_volume,
            "users_api_calls": users_api_calls
        })
    
    @action(detail=False, methods=['get'], permission_classes=[AllowAny])
    def public_status(self, request):
        return Response({"status": "Servidor Vivo", "version": "1.6.1"})
    
    @action(detail=False, methods=['post'], url_path='assign_role')
    def assign_role(self, request):
        """
        Endpoint para asignar grupos a un usuario.
        Cuerpo esperado: { "username": "nombre", "group_name": "Gerente", "clear_existing": true }
        """
        if not request.user.groups.filter(name='Admin_Scraper').exists() and not request.user.is_superuser:
            return Response({"error": "No tienes permiso para gestionar roles"}, status=403)

        username = request.data.get('username')
        group_name = request.data.get('group_name')
        clear_existing = request.data.get('clear_existing', False) # Opcional

        if not username or not group_name:
            return Response({"error": "Faltan parámetros: username y group_name"}, status=400)

        try:
            from django.contrib.auth.models import Group
            user = User.objects.get(username=username)
            group = Group.objects.get(name=group_name)

            if clear_existing:
                user.groups.clear()
                logger.info(f"Grupos limpiados para el usuario {username}")

            user.groups.add(group)
            
            if group_name in ['Admin_Scraper', 'Director']:
                user.is_staff = True
                user.save()

            return Response({
                "status": "success",
                "message": f"Usuario {username} asignado al grupo {group_name}"
            }, status=status.HTTP_200_OK)

        except User.DoesNotExist:
            return Response({"error": "El usuario no existe"}, status=404)
        except Group.DoesNotExist:
            return Response({"error": "El grupo no existe. Verifica que post_migrate haya corrido."}, status=404)
        except Exception as e:
            logger.error(f"Error en assign_role: {str(e)}")
            return Response({"error": str(e)}, status=500)
    
    @action(detail=False, methods=['get'], url_path='list_users')
    def list_users(self, request):
        """Devuelve la lista de usuarios para el selector del frontend."""
        if not request.user.is_staff and not request.user.groups.filter(name='Admin_Scraper').exists():
            return Response({"error": "No tienes permiso"}, status=403)
            
        users = User.objects.all().values('id', 'username', 'first_name', 'email')
        return Response(list(users), status=200)

    @action(detail=False, methods=['get'], url_path='influencer_profile')
    def influencer_profile(self, request):
        if not request.user.groups.filter(name__in=['Admin_Scraper', 'Usuario', 'Gerente', 'Director']).exists():
            return Response({"error": "No tienes permiso para consultar perfiles"}, status=403)

        username = request.query_params.get('username', '').strip()

        influencers_list = list(
            ScrapeResult.objects.exclude(username__isnull=True)
            .exclude(username='')
            .order_by('username')
            .values_list('username', flat=True)
            .distinct()
        )

        if not username:
            return Response({
                "error": "Debes enviar el parámetro username",
                "influencers_list": influencers_list
            }, status=400)

        influencer_qs = ScrapeResult.objects.filter(username__iexact=username).order_by('-created_at')
        if not influencer_qs.exists():
            return Response({
                "error": f"No se encontró información para {username}",
                "influencers_list": influencers_list
            }, status=404)

        influencer_posts = ScrapeResultSerializer(influencer_qs, many=True).data
        latest_record = influencer_qs.first()

        return Response({
            "influencer": {
                "username": latest_record.username,
                "total_posts": influencer_qs.count(),
                "latest_platform": latest_record.platform,
                "latest_followers": latest_record.followers,
                "latest_post_date": latest_record.post_date,
                "last_updated": latest_record.created_at,
                "sentimiento_global": latest_record.sentimiento_global,
                "is_loto": latest_record.is_loto,
                "alegria": latest_record.alegria,
                "confianza": latest_record.confianza,
                "miedo": latest_record.miedo,
                "sorpresa": latest_record.sorpresa,
                "tristeza": latest_record.tristeza,
                "aversion": latest_record.aversion,
                "ira": latest_record.ira,
                "anticipacion": latest_record.anticipacion,
                "posts": influencer_posts
            },
            "influencers_list": influencers_list
        }, status=200)
    
@api_view(['POST'])
@permission_classes([AllowAny])
def azure_login(request):
    token = request.data.get('access_token')
    
    if not token:
        return Response({"error": "No token provided"}, status=400)
    
    try:
        payload = _decode_azure_access_token(token)
        
        email = payload.get('email') or payload.get('preferred_username')
        full_name = payload.get('name', 'Usuario Loto')
        
        if not email:
            return Response({"error": "No email in token"}, status=400)

        username = email.split('@')[0]

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
        return Response({'error': 'Token inválido', 'details': str(e)}, status=400)


@api_view(['POST'])
@permission_classes([AllowAny])
def register_user(request):
    serializer = RegisterSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)

    validated = serializer.validated_data
    user = User(
        username=validated['username'],
        email=validated['email'],
        first_name=validated.get('first_name', ''),
        last_name=validated.get('last_name', ''),
    )
    user.set_password(validated['password'])
    user.save()

    refresh = RefreshToken.for_user(user)

    return Response({
        'message': 'Usuario registrado correctamente',
        'refresh': str(refresh),
        'access': str(refresh.access_token),
        'user': {
            'id': user.id,
            'username': user.username,
            'email': user.email,
            'name': user.first_name,
        },
        'registros_auth_user': User.objects.count(),
    }, status=status.HTTP_201_CREATED)


@api_view(['POST'])
@permission_classes([AllowAny])
def login_user(request):
    serializer = LoginSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)

    identifier = serializer.validated_data['identifier'].strip()
    password = serializer.validated_data['password']

    target_user = User.objects.filter(
        Q(email__iexact=identifier) | Q(username__iexact=identifier)
    ).first()

    if not target_user:
        return Response({'error': 'Usuario o correo no encontrado'}, status=status.HTTP_401_UNAUTHORIZED)

    user = authenticate(request, username=target_user.username, password=password)
    if not user:
        return Response({'error': 'Credenciales inválidas'}, status=status.HTTP_401_UNAUTHORIZED)

    refresh = RefreshToken.for_user(user)
    return Response({
        'refresh': str(refresh),
        'access': str(refresh.access_token),
        'user': {
            'id': user.id,
            'username': user.username,
            'email': user.email,
            'name': user.first_name,
        },
        'registros_auth_user': User.objects.count(),
    }, status=status.HTTP_200_OK)
