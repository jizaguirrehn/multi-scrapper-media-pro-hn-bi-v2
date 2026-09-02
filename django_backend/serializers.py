
from rest_framework import serializers
from .models import ScrapeResult, ScraperKey, PostComment, DimUsuario

class DimUsuarioSerializer(serializers.ModelSerializer):
    class Meta:
        model = DimUsuario
        fields = ['id', 'platform', 'username', 'followers', 'followers_updated_at']

class ScrapeResultSerializer(serializers.ModelSerializer):
    usuario = DimUsuarioSerializer(source='dim_usuario', read_only=True)

    class Meta:
        model = ScrapeResult
        fields = '__all__'
        read_only_fields = ['usuario']

class ScraperKeySerializer(serializers.ModelSerializer):
    class Meta:
        model = ScraperKey
        fields = ['platform', 'purpose', 'is_active', 'last_used']

class PostCommentSerializer(serializers.ModelSerializer):
    post_username = serializers.SerializerMethodField()
    post_platform = serializers.SerializerMethodField()
    post_description = serializers.SerializerMethodField()
    
    class Meta:
        model = PostComment
        fields = ['id', 'texto', 'platform', 'created_at', 'post', 'post_username', 'post_platform', 'post_description']
    
    def get_post_username(self, obj):
        return obj.post.username
    
    def get_post_platform(self, obj):
        return obj.post.platform
    
    def get_post_description(self, obj):
        return obj.post.description
