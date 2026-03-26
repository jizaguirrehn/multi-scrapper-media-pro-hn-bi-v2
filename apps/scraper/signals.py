from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib.auth.models import User, Group

@receiver(post_save, sender=User)
def asignar_grupo_automatico(sender, instance, created, **kwargs):
    if created:
        try:
            grupo_colaborador = Group.objects.get(name='Colaborador')
            instance.groups.add(grupo_colaborador)
            instance.save()
            print(f"Grupo 'Colaborador' asignado a: {instance.email}")
        except Group.DoesNotExist:
            print("El grupo 'Colaborador' no existe. Créalo en el admin.")