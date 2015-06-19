import sys
import olrcdb
import os

import datetime
# Globals
COUNT = 0
FAILED = 0


class FileParser(object):
    '''Object used to parse through a directory for all it's files. Collects
    the paths of all the files and stores a record of these in a new table in
    the database.

    The Schema of the database is:

    NewTable(path, uploaded=false)
    '''

    def __init__(self, directory, table_name):
        self.directory = directory
        self.table_name = table_name


def prepare_upload(connect, directory, table_name):
    '''Given a database connection, directory and table_name,
    -Create the table in the database
    -populate the table with (path, uploaded=false)
    where each path is a file in the given directory.'''

    global COUNT, FAILED

    for filename in os.listdir(directory):

        file_path = os.path.join(directory, filename)

        # Add file name to the list.
        if os.path.isfile(file_path):
            try:
                connect.insert_path(file_path, table_name)
                COUNT += 1
            except:
                FAILED += 1
                error_log = open(table_name + 'error.log', 'a')
                error_log.write("\rFailed: {0}\n".format(file_path))
                error_log.close()
            sys.stdout.flush()
            sys.stdout.write("\r{0} parsed. ".format(COUNT))

            #Output status to a file.
            final_count = open(table_name + ".out", 'w+')
            final_count.write("\r{0} parsed. ".format(COUNT))
            final_count.close()
        else:
            prepare_upload(connect, file_path, table_name)


if __name__ == "__main__":

    # Check for proper parameters
    if len(sys.argv) != 3:
        sys.stderr.write(
            'Usage: python prepareupload.py path-to-drive table-name\n'
        )
        sys.exit(1)

     #Open error log:
    error_log = open('error.log', 'w+')
    error_log.write("From execution {0}:\n".format(
        str(datetime.datetime.now())
    ))
    error_log.close()

    connect = olrcdb.DatabaseConnection()
    connect.create_table(sys.argv[2])
    prepare_upload(connect, sys.argv[1], sys.argv[2])

    sys.stdout.flush()
    sys.stdout.write("\r{0} parsed. ".format(COUNT))
    if FAILED != 0:
        sys.stdout.write("\n{0} FAILED. See error.log.".format(FAILED))

    #Log the final count
    final_count = open(sys.argv[2] + ".out", 'w+')
    final_count.write("\r{0} parsed. ".format(COUNT))
    final_count.close()
