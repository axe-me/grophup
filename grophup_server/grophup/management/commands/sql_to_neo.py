from django.core.management.base import BaseCommand
import pymssql
from py2neo import Graph
from django.conf import settings
from ...graph_models import Group, Person
from tqdm import tqdm


class Command(BaseCommand):
    help = 'port group data from sql server to neo4j.'

    def __init__(self, *args, **kwargs):
        super(Command, self).__init__(*args, **kwargs)

        self._sql_server_conn = pymssql.connect(
            server='SX-DEV'
        )

        self._init_graph()

    def handle(self, *args, **options):
        self._start_import()

    def _init_graph(self):
        self._graph = Graph(
            host=settings.NEO4J['HOST'],
            http_port=settings.NEO4J['PORT'],
            user=settings.NEO4J['USER'],
            password=settings.NEO4J['PWD']
        )

    def _start_import(self):
        self.stdout.write('Start to migrate data from sql server to neo4j')

        group_db, person_db = self._get_databases()

        # create all the group nodes
        for db in group_db:
            for table in self._get_db_tables(db):
                self._create_group(db, table)

        # create all group users nodes and build relations
        for db in person_db:
            for table in self._get_db_tables(db):
                self._create_person(db, table)

        self._close_mssql_conn()

        self.stdout.write(self.style.SUCCESS('Successfully imported all data to neo4j server'))

    def _close_mssql_conn(self):
        self._sql_server_conn.close()

    def _get_databases(self):
        cursor = self._sql_server_conn.cursor()
        cursor.execute('SELECT name FROM sys.databases;')
        dbs = cursor.fetchall()

        group_db = []
        person_db = []
        for db in dbs:
            db_name = db[0]
            if 'GroupData' in db_name:
                person_db.append(db_name)
            elif 'QunInfo' in db_name:
                group_db.append(db_name)

        return group_db, person_db

    def _get_db_tables(self, db_name):
        cursor = self._sql_server_conn.cursor()
        cursor.execute("SELECT TABLE_NAME FROM %s.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';" % db_name)
        return [
            tb[0]
            for tb in cursor.fetchall()
            if 'QunList' in tb[0] or 'Group' in tb[0]
        ]

    def _create_group(self, db_name, table_name, start_id=0):
        curr_id = start_id
        cursor = self._sql_server_conn.cursor()
        cursor.execute('SELECT count(*) FROM %s.dbo.%s where id > %d' % (db_name, table_name, start_id))

        total = cursor.fetchall()[0][0]

        cursor = self._sql_server_conn.cursor()
        cursor.execute('SELECT * FROM %s.dbo.%s where id > %d ORDER BY id' % (db_name, table_name, start_id))

        pbar = tqdm(
            desc='Creating Group Nodes from [%s.%s]' % (db_name, table_name),
            total=total
        )
        try:
            g = cursor.fetchone()
            while g:
                curr_id = g[0]
                group = Group()
                group.number = g[1]
                group.mastqq = g[2]
                group.date = g[3]
                group.title = g[4]
                group.groupclass = g[5]
                group.intro = g[6]
                self._graph.merge(group)
                pbar.update(1)
                g = cursor.fetchone()
        except:
            print('Catch an Exception, resume group creating from id: %d' % (curr_id-1))
            pbar.close()
            self._init_graph()
            self._create_group(db_name, table_name, curr_id-1)
        pbar.close()

    def _create_person(self, db_name, table_name, start_id=0):
        curr_id = start_id
        cursor = self._sql_server_conn.cursor()
        cursor.execute('SELECT count(*) FROM %s.dbo.%s where id > %d' % (db_name, table_name, start_id))

        total = cursor.fetchall()[0][0]

        cursor = self._sql_server_conn.cursor()
        cursor.execute('SELECT * FROM %s.dbo.%s where id > %d ORDER BY id' % (db_name, table_name, start_id))

        pbar = tqdm(
            desc='Creating Person Nodes and Relations from [%s.%s]' % (db_name, table_name),
            total=total
        )
        try:
            p = cursor.fetchone()
            while p:
                curr_id = p[0]
                person = Person()
                person.qq = p[1]
                person.nick = p[2]
                person.age = p[3]
                person.gender = p[4]
                person.auth = p[5]
                group_number = p[6]
                # get group node
                group = Group.select(self._graph, group_number).first()
                if group:
                    # build relations
                    person.groups.add(group)
                    group.members.add(person)
                    # update group node
                    self._graph.push(group)
                self._graph.merge(person)
                pbar.update(1)
                p = cursor.fetchone()
        except:
            print('Catch an Exception, resume person creating from id: %d' % (curr_id-1))
            pbar.close()
            self._init_graph()
            self._create_person(db_name, table_name, curr_id-1)
        pbar.close()
