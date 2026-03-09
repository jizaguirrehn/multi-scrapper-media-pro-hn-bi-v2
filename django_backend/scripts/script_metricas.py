import os
import sys
import django
import datetime
import json
import statistics
from django.db.models import Count, Avg, F, ExpressionWrapper, FloatField
from django.utils import timezone

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')
django.setup()

from django_backend.models import ScrapeResult

def mostrar_metricas():
    print("="*40)
    print("   DASHBOARD DE MÉTRICAS AVANZADAS")
    print("="*40)

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

    if engagement_list:
        avg_engagement = round(sum(engagement_list) / len(engagement_list), 2)
        median_engagement = round(statistics.median(engagement_list), 2)
    else:
        avg_engagement = 0
        median_engagement = 0

    dist = ScrapeResult.objects.values('platform').annotate(total=Count('id'))
    platform_dist = {
        item['platform'].upper(): f"{round((item['total'] / total_posts) * 100, 1)}%"
        for item in dist
    }

    metricas = {
        "Total Database Posts": total_posts,
        "Total Unique Profiles": ScrapeResult.objects.values('username').distinct().count(),
        "Engagement Metrics": {
            "Average (Mean)": f"{avg_engagement}%",
            "Median": f"{median_engagement}%",
            "Sample Size": len(engagement_list)
        },
        "Platform Distribution": platform_dist,
    }

    print(json.dumps(metricas, indent=4))

if __name__ == "__main__":
    mostrar_metricas()