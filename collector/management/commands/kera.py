from django.core.management.base import BaseCommand, CommandError

from collector.kera import main


class Command(BaseCommand):
    help = 'Manages A.I.'

    def handle(self, *args, **options):
        main()
        self.stdout.write(self.style.SUCCESS('Successfully done A.I. jobs'))
