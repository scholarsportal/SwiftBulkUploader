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
            )
        except KeyError:
            sys.exit("Please make sure all required environment variables"
                     " are set:\n$MYSQL_HOST\n$MYSQL_DB\n$MYSQL_USER\n"
                     "$MYSQL_PASSWD\n")
        except MySQLdb.Error, e:
            sys.exit("ERROR %d IN CONNECTION: %s".format(e.args[0], e.args[1]))

    def get_cursor(self):
        '''Return a cursor for the database.'''
        return self.db.cursor()
