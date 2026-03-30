import os
import sys
import django
import datetime
import json
import statistics
from django.db.models import Count, Avg, F, ExpressionWrapper, FloatField
from django.db.models.functions import ExtractWeekDay
from django.utils import timezone

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')
django.setup()

from apps.scraper.models import ScrapeResult, ExtractionRequestLog

def mostrar_metricas():
    print("="*40)
    print("   DASHBOARD DE MÉTRICAS AVANZADAS")
    print("="*40)

    ahora = timezone.now()
    hace_una_semana = ahora - datetime.timedelta(days=7)
    
    dias_map = {1: 'Sun', 2: 'Mon', 3: 'Tue', 4: 'Wed', 5: 'Thu', 6: 'Fri', 7: 'Sat'}

    total_posts = ScrapeResult.objects.count()
    if total_posts == 0:
        print("La base de datos está vacía.")
        return

    queryset_engagement = ScrapeResult.objects.filter(followers__gt=0).annotate(
        engagement_val=ExpressionWrapper(
            (F('likes') + F('comments')) * 100.0 / F('followers'),
            output_field=FloatField()
        )
    ).values_list('engagement_val', flat=True)

    engagement_list = list(queryset_engagement)
    avg_engagement = round(statistics.mean(engagement_list), 2) if engagement_list else 0
    median_engagement = round(statistics.median(engagement_list), 2) if engagement_list else 0

    volumen_semanal_raw = (
        ScrapeResult.objects.filter(created_at__gte=hace_una_semana)
        .annotate(day_num=ExtractWeekDay('created_at'))
        .values('day_num')
        .annotate(count=Count('id'))
        .order_by('day_num')
    )
    
    weekly_stats = {dias_map.get(item['day_num'], "NA"): item['count'] for item in volumen_semanal_raw}

    dist = ScrapeResult.objects.values('platform').annotate(total=Count('id'))
    platform_dist = {
        item['platform'].upper(): f"{round((item['total'] / total_posts) * 100, 1)}%"
        for item in dist
    }

    user_request_counts = (
        ExtractionRequestLog.objects
        .values('user_id')
        .annotate(call_count=Count('id'))
        .order_by('-call_count')
    )
    
    users_calls = {
        item['user__username'] if item['user__username'] else 'Anonymous': item['call_count']
        for item in user_request_counts
    }

    metricas = {
        "Posts Extracted": total_posts,
        "Total Profiles": ScrapeResult.objects.values('username').distinct().count(),
        "Avg Engagement": median_engagement,
        "Platform Distribution": platform_dist,
        "Extraction Volume (Weekly)": weekly_stats,
        "Users API Calls": users_calls
    }

    print(json.dumps(metricas, indent=4))

if __name__ == "__main__":
    mostrar_metricas()