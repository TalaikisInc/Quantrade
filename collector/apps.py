

from django.apps import AppConfig


class CollectorConfig(AppConfig):
    name = 'collector'
    verbose_name = 'Trading signals application.'

    def ready(self):
        import collector.signals
