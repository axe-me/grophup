from django.core.management.base import BaseCommand, CommandError
import pymssql


class Command(BaseCommand):
    help = 'port group data from sql server to neo4j.'

    def add_arguments(self, parser):
        parser.add_argument('env', nargs=1, type=str)

    def handle(self, *args, **options):
        env = options['env'][0]
        self.stdout.write(env)
        if env == 'dev':
            conn = pymssql.connect(
                server='SX-DEV',
                database="GroupData1_Data"
            )

            cursor = conn.cursor()

            cursor.execute('SELECT TOP 1000 * FROM Group10')

            row = cursor.fetchone()

            while row:
                print(row)
                row = cursor.fetchone()

            conn.close()

            self.stdout.write(self.style.SUCCESS('Successfully imported all data to "%s" neo4j server' % env))
        else:
            raise CommandError('Wrong env parameter (dev/prod)')
