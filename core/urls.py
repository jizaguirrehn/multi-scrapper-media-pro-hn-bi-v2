from django.contrib import admin
from django.urls import path, include
from django.http import JsonResponse
from rest_framework.routers import DefaultRouter
from apps.scraper.views import (
    ScraperViewSet,
    azure_login,
    register_user,
    login_user,
)
from rest_framework_simplejwt.views import (
    TokenObtainPairView,
    TokenRefreshView,
)

def api_root(request):
    return JsonResponse({
        "status": "online",
        "message": "Backend Scraper API Operativa",
        "endpoints": ["/api/scraper/"]
    })

router = DefaultRouter()
router.register(r'scraper', ScraperViewSet, basename='scraper')

urlpatterns = [
    path('admin/', admin.site.urls),

    path('api/', include(router.urls)),

    path('', api_root, name='index'),

    path('api/token/', TokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('api/token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
    path('api/auth/azure-login/', azure_login, name='azure_login'), # Ruta para el login de React
    path('api/auth/register/', register_user, name='register_user'),
    path('api/auth/login/', login_user, name='login_user'),
    path('', include(router.urls)),
]