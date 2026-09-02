from django.db import migrations, models
import django.db.models.deletion
from django.utils import timezone


def enlazar_usuarios(apps, schema_editor):
    DimUsuario = apps.get_model('django_backend', 'DimUsuario')
    ScrapeResult = apps.get_model('django_backend', 'ScrapeResult')

    usuarios = {}
    for resultado in ScrapeResult.objects.all().iterator():
        clave = (resultado.platform, resultado.username)
        usuario = usuarios.get(clave)
        if usuario is None:
            usuario, _ = DimUsuario.objects.get_or_create(
                platform=resultado.platform,
                username=resultado.username,
                defaults={
                    'followers': resultado.followers or 0,
                    'followers_updated_at': timezone.now() if resultado.followers and resultado.followers > 0 else None,
                },
            )
            usuarios[clave] = usuario
        elif resultado.followers and resultado.followers > usuario.followers:
            usuario.followers = resultado.followers
            usuario.followers_updated_at = timezone.now()
            usuario.save(update_fields=['followers', 'followers_updated_at'])

        resultado.dim_usuario_id = usuario.pk
        resultado.save(update_fields=['dim_usuario'])


class Migration(migrations.Migration):
    dependencies = [
        ('django_backend', '0005_postcomment'),
    ]

    operations = [
        migrations.CreateModel(
            name='DimUsuario',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('platform', models.CharField(max_length=10)),
                ('username', models.CharField(max_length=255)),
                ('followers', models.BigIntegerField(default=0)),
                ('followers_updated_at', models.DateTimeField(blank=True, null=True)),
            ],
            options={
                'db_table': 'DimUsuario',
            },
        ),
        migrations.AddConstraint(
            model_name='dimusuario',
            constraint=models.UniqueConstraint(
                fields=('platform', 'username'),
                name='unique_dim_usuario_platform_username',
            ),
        ),
        migrations.AddIndex(
            model_name='dimusuario',
            index=models.Index(fields=['platform', 'username'], name='DimUsuario_platfor_76e659_idx'),
        ),
        migrations.AddField(
            model_name='scraperesult',
            name='dim_usuario',
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name='scrape_results',
                to='django_backend.dimusuario',
            ),
        ),
        migrations.RunPython(enlazar_usuarios, migrations.RunPython.noop),
    ]
