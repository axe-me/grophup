from django.core.management.base import BaseCommand
import pymssql
from django.conf import settings

class Command(BaseCommand):
    help = 'port group data from sql server to neo4j.'

    def __init__(self, *args, **kwargs):
        super(Command, self).__init__(*args, **kwargs)
        self._mssql_conn = pymssql.connect(
            server='SX-DEV',
            database="GroupData1_Data"
        )
        print(settings.NEO4J['HOST'])
        print(settings.NEO4J['PORT'])
        print(settings.NEO4J['USER'])
        print(settings.NEO4J['PWD'])

    def handle(self, *args, **options):
            self._start_import()

    def _start_import(self):
        cursor = self._mssql_conn.cursor()

        cursor.execute('SELECT TOP 1000 * FROM Group10')

        row = cursor.fetchone()

        # while row:
        #     print(row)
        #     row = cursor.fetchone()

        self._mssql_conn.close()

        self.stdout.write(self.style.SUCCESS('Successfully imported all data to neo4j server'))

    def _close_mssql_conn(self):
        self._mssql_conn.close()
