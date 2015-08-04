import os
import sys
import MySQLdb


class DatabaseConnection(object):
    '''Connect to OLRC's mysql server.'''

    def __init__(self):
        '''Initiate connection the database. If connection credentials are not
        available or connection fails throw exception.'''
        try:

            self.db = MySQLdb.connect(
                host=os.environ["MYSQL_HOST"],
                user=os.environ["MYSQL_USER"],
                passwd=os.environ["MYSQL_PASSWD"],
                db=os.environ["MYSQL_DB"],
                charset='utf8',
            )
            self.cursor = self.db.cursor()
        except KeyError:
            sys.exit("Please make sure all required environment variables"
                     " are set:\n$MYSQL_HOST\n$MYSQL_DB\n$MYSQL_USER\n"
                     "$MYSQL_PASSWD\n")
        except MySQLdb.Error, e:
            sys.exit("ERROR {0} IN CONNECTION: {1}".format(
                e.args[0], e.args[1]
            ))

    def get_cursor(self):
        '''Return a cursor for the database.'''

        return self.cursor

    def create_table(self, table_name):
        '''Given a table_name, create a table in the database.

        '''

        query = "CREATE TABLE {0} ( \
            `id` INTEGER  NOT NULL AUTO_INCREMENT,\
            path VARCHAR(1000),\
            uploaded BOOL DEFAULT '0',\
            INDEX `path_index` (`id`)\
            )".format(table_name)

        try:
            self.cursor.execute(query)
        except MySQLdb.Error, e:
            sys.exit("ERROR {0} IN TABLE CREATION: {1}".format(
                e.args[0],
                e.args[1]
            ))

    def insert_path(self, path, table_name, alt=False):
        '''Insert the given path to the table_name. If alt, create
        the query with reversed quotes.'''

        if not alt:
            query = "INSERT INTO {0} (path) VALUES ('{1}');".format(
                table_name,
                path
            )
        else:
            query = 'INSERT INTO {0} (path) VALUES ("{1}");'.format(
                table_name,
                path
            )

        self.cursor.execute(query)
        self.db.commit()

    def execute_query(self, query):
        '''Execute the given query and return the cursor object.'''

        try:
            self.cursor.execute(query)
            self.db.commit()
        except MySQLdb.Error, e:
            sys.exit("ERROR {0} IN QUERY: {1}\nQuery:{2}".format(
                e.args[0],
                e.args[1],
                query
            ))
        return self.cursor
