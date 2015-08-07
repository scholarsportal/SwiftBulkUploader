import sys
import olrcdb
import os

import datetime

from bulkupload import env_vars_set

# Globals
COUNT = 0
FAILED = 0

REQUIRED_VARIABLES = [
    "MYSQL_HOST",
    "MYSQL_USER",
    "MYSQL_PASSWD",
    "MYSQL_DB",
]


def prepare_upload(connect, directory, table_name):
    '''Given a database connection, directory and table_name,
    -Create the table in the database
    -populate the table with (path, uploaded=false)
    where each path is a file in the given directory.'''

    global COUNT, FAILED

    # Loop through all items in the directory.
    for filename in os.listdir(directory):

        file_path = os.path.join(directory, filename)

        # Add file name to the list.
        if os.path.isfile(file_path):
            try:
                connect.insert_path(file_path, table_name)
                COUNT += 1
            except:

                # Try again with the alternative query.
                try:
                    connect.insert_path(file_path, table_name, True)
                    COUNT += 1
                except:

                    FAILED += 1
                    error_log = open(table_name + '.prepare.error.log', 'a')
                    error_log.write("\rFailed: {0}\n".format(file_path))
                    error_log.close()
            sys.stdout.flush()
            sys.stdout.write("\r{0} parsed. ".format(COUNT))

            #Output status to a file.
            final_count = open(table_name + ".prepare.out", 'w+')
            final_count.write("\r{0} parsed. ".format(COUNT))
            final_count.close()

        # Recursive call for sub directories.
        else:
            prepare_upload(connect, file_path, table_name)


if __name__ == "__main__":

    # Check for proper parameters
    if len(sys.argv) != 3:
        sys.stderr.write(
            'Usage: python prepareupload.py path-to-directory table-name\n'
        )
        sys.exit(1)
    else:
        table_name = sys.argv[2]
        directory = sys.argv[1]

    # Check required environment variables have been set
    if not env_vars_set():
        set_env_message = "The following environment variables need to be " \
            "set:\n"
        set_env_message += " \n".join(REQUIRED_VARIABLES)
        set_env_message += "\nPlease set these environment variables to " \
            "connect to the OLRC."
        print(set_env_message)
        exit(0)

    #Open error log:
    error_log = open(table_name + '.prepare.error.log', 'w+')
    error_log.write("From execution {0}:\n".format(
        str(datetime.datetime.now())
    ))
    error_log.close()

    connect = olrcdb.DatabaseConnection()
    connect.create_table(table_name)
    prepare_upload(connect, directory, table_name)

    sys.stdout.flush()
    sys.stdout.write("\r{0} parsed. ".format(COUNT))
    if FAILED != 0:
        sys.stdout.write("\n{0} FAILED. See error.log.".format(FAILED))

    #Log the final count
    final_count = open(table_name + ".prepare.out", 'w+')
    final_count.write("\r{0} parsed. ".format(COUNT))
    final_count.close()