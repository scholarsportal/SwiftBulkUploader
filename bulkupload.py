import filesegmenter
import hashlib
import os
import shutil
import swiftclient
import sys
import datetime
import socket
import time
import olrcdb
from multiprocessing import Process, Lock, Value


#Settings
AUTH_VERSION = 2
SWIFT_AUTH_URL = ''
USERNAME = ''
PASSWORD = ''
CONTAINER = ''
AUTH_TOKEN = ''
STORAGE_URL = ''
TEMP_DIRECTORY = 'temp'
FILE_LIMIT = 0.5*10**9  # Max file size in bytes that a file can be uploaded.
# Anything larger is segmented
SEGMENT_SIZE = 100*10**6
COUNT = 0
TOTAL = 0
FAILED_COUNT = 0
LIMIT = 1000
RANGE = 0  # protected variable
SLEEP = 100

REQUIRED_VARIABLES = [
    'OS_AUTH_URL',
    'OS_USERNAME',
    'OS_TENANT_NAME',
    'OS_PASSWORD',
    "MYSQL_HOST",
    "MYSQL_USER",
    "MYSQL_PASSWD",
    "MYSQL_DB"
]


def olrc_upload(path):
    ''' Given a path, upload it to the OLRC. Return False if upload
    unsuccessful.'''

    global FAILED_COUNT

    # Check file not already online.
    if not is_uploaded(path):

        if not (olrc_upload_file(path)):
            FAILED_COUNT += 1
            error_log = open('error.log', 'a')
            error_log.write("\rFailed: {0}\n".format(path))
            error_log.close()
            return False

    return True


def olrc_upload_segments(source_file, target_directory):
    ''' Break up the source_file into segments and upload them into
    target_directory'''

    segments = filesegmenter.split_file(
        source_file,
        'temp',
        SEGMENT_SIZE
    )

    sys.stdout.flush()
    sys.stdout.write("\rPartitioning file {0}".format(source_file))

    for segment in segments:

        # Files are within the temp directory locally. On the server the file
        # will live in the target_directory so we need to remove 'temp/' from
        # target_file.
        target_file = os.path.join(target_directory, segment.split('/', 1)[1])
        sys.stdout.flush()
        sys.stdout.write("\rUploading file {0}".format(segment))

        # MDCheck
        if not is_uploaded(segment, target_file):
            if not olrc_upload_file(segment, target_file):
                return False
        else:

            sys.stdout.flush()
            sys.stdout.write(
                "\rSkipping: {0}, already uploaded.".format(source_file)
            )

    # Create and upload readme file.

    readme = "The file in this directory has been segmented for convenient" \
        " upload and download. To assemble the file, run the following " \
        "command on your machine in the directory with all the segments: " \
        "\n\ncat * >> {0}".format(source_file.split('/')[-1])
    outFile = open("temp/readme.txt", "wt")
    outFile.write(readme)
    outFile.close()

    olrc_upload_file(
        "temp/readme.txt",
        os.path.join(target_directory, "readme.txt")
    )
    #Clean up temp files
    if os.path.isdir(TEMP_DIRECTORY):
        shutil.rmtree(TEMP_DIRECTORY)

    return True


def olrc_upload_file(path):
    '''Given String source_file, upload the file to the OLRC to target_file
     and return True if successful. '''

    global SLEEP
    try:
        opened_source_file = open(path, 'r')
    except IOError:
        print("Error opening: " + path)
        return False
    try:
        swiftclient.client.put_object(
            STORAGE_URL,
            AUTH_TOKEN,
            CONTAINER,
            path,
            opened_source_file)

    except Exception, e:

        olrc_connect()
        sys.stdout.flush()
        sys.stdout.write("\rError! {0}\n".format(e))
        sys.stdout.write(
            "Error! {0} Upload to OLRC failed."
            " Trying again in {1} seconds.\n".format(
                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                SLEEP
            )
        )
        time.sleep(SLEEP)
        SLEEP += SLEEP
        olrc_upload_file(path)

    return True


def olrc_connect():
    '''Connect to the OLRC with the global variables. Exit if connection
    fails.'''

    global SLEEP
    try:
        (connection_storage_url, auth_token) = swiftclient.client.get_auth(
            SWIFT_AUTH_URL, USERNAME, PASSWORD,
            auth_version=AUTH_VERSION)
        global AUTH_TOKEN
        AUTH_TOKEN = auth_token
        global STORAGE_URL
        STORAGE_URL = connection_storage_url
    except swiftclient.client.ClientException, e:
        print(e)
        sys.stdout.flush()
        sys.stdout.write(
            "\rError! {0} Connection to OLRC failed."
            " Trying again in {1} seconds.\n".format(
                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                SLEEP
            )
        )
        time.sleep(SLEEP)
        SLEEP += SLEEP

        olrc_connect()


def is_uploaded(file_name):
    '''Return True if String file is already on the server and its etag
    matches it's md5. Delete the file from the server if the
    md5 does not match.'''

    # Swift stat on filename.
    try:
        object_stat = swiftclient.client.head_object(
            STORAGE_URL,
            AUTH_TOKEN,
            CONTAINER,
            file_name
        )
        try:
            etag = object_stat['etag']
        except:
            # Return if no etag
            return False

        md5 = checksum_md5(file_name)
        match = etag == md5

        # Delete the file if the md5 does not match.
        if not match:
            try:
                swiftclient.client.delete_object(
                    STORAGE_URL,
                    AUTH_TOKEN,
                    CONTAINER,
                    file_name
                )
            except:
                pass

        return match
    except:
        return False


def checksum_md5(filename):
    md5 = hashlib.md5()
    with open(filename, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            md5.update(chunk)
    return md5.hexdigest()


def is_env_vars_set():
    '''Check all the required environment variables are set. Return false if
    any of them are undefined.'''

    global REQUIRED_VARIABLES
    for required_variable in REQUIRED_VARIABLES:
        if not os.environ.get(required_variable):
            return False

    return True


def set_env_vars():
    '''Set the global variables for swift client assuming they exist in the
    environment.'''

    global SWIFT_AUTH_URL
    SWIFT_AUTH_URL = os.environ.get("OS_AUTH_URL")

    global USERNAME
    USERNAME = os.environ.get("OS_TENANT_NAME") + \
        ":" + os.environ.get("OS_USERNAME")

    global PASSWORD
    PASSWORD = os.environ.get("OS_PASSWORD")

    return


def upload_table(lock, range, table_name, counter):
    '''
    Given a table_name, upload all the paths from the table where upload is 0.
    Upload within a LIMIT range at a time.
    '''
    global FAILED_COUNT, LIMIT, RANGE

    lock.acquire()
    while range.value <= TOTAL:

        # Access protected variable.
        # We want to make we only fetch within a unique range,
        # so RANGE is locked.
            # "grab" a LIMITs with of rows at a time.
        query = (
            "SELECT * FROM {0} WHERE uploaded=0"
            " AND id >= {1} AND id <{2}".format(
                table_name, range.value, range.value + LIMIT))
        # Let other processes know this range.value has been accounted for.
        range.value += LIMIT
        lock.release()

        # Connect to the database and fetch results.
        connect = olrcdb.DatabaseConnection()
        result = connect.execute_query(query)
        path_tuple = result.fetchone()
        # Loop until we run out of rows from the database.
        while (path_tuple):

            # if the upload is successful, update the database
            if olrc_upload(path_tuple[1]):
                lock.acquire()
                counter.value += 1
                lock.release()
                set_uploaded(path_tuple[0], table_name)

            print_status(counter, lock)

            path_tuple = result.fetchone()
        lock.acquire()
        #Executes on the last range.
    lock.release()


def get_total_to_upload(table_name):
    '''Given a table_name, get the total number of rows'''

    query = "SELECT COUNT(*) FROM {0}".format(table_name)

    connect = olrcdb.DatabaseConnection()
    result = connect.execute_query(query)
    result_tuple = result.fetchone()
    return result_tuple[0]


def get_total_uploaded(table_name):
    '''Given a table_name, get the total number of rows where upload is 1.'''

    query = "SELECT COUNT(*) FROM {0} WHERE uploaded=1".format(table_name)

    connect = olrcdb.DatabaseConnection()
    result = connect.execute_query(query)
    result_tuple = result.fetchone()
    return result_tuple[0]


def set_uploaded(id, table_name):
    '''For the given path, set uploaded to 1 in table_name.'''
    query = "UPDATE {0} set uploaded='1' WHERE id='{1}'".format(
        table_name,
        id
    )

    connect = olrcdb.DatabaseConnection()
    connect.execute_query(query)


def check_env_args():
    '''Do checks on the environment and args.'''
    # Check environment variables
    if not is_env_vars_set():
        set_env_message = "The following environment variables need to be " \
            "set:\n"
        set_env_message += " \n".join(REQUIRED_VARIABLES)
        set_env_message += "\nPlease set these environment variables to " \
            "connect to the OLRC."
        print(set_env_message)
        exit(0)
    else:
        set_env_vars()

    total = len(sys.argv)
    cmd_args = sys.argv
    usage = "Please pass in a few arguments, see example below \n" \
        "python bulkupload.py container-name mysql-table n-processes\n" \
        "where mysql-table is table created from prepareupload.py and " \
        "n-process is the number of processes created to run this script. " \

    # Do not execute if no directory provided.
    if total != 4:
        print(usage)
        exit(0)


def start_reporting():
    '''Do the setup work for reporting.'''

    #Open error log:
    error_log = open('error.log', 'w+')
    error_log.write("From execution {0}:\n".format(
        str(datetime.datetime.now())
    ))
    error_log.close()


def end_reporting(counter):
    '''Do the wrap uup work for reporting.'''
    #Save report in file.
    report_log = open('report.log', 'w+')
    report_log.write("From execution {0}:\n".format(
        str(datetime.datetime.now())
    ))
    report = "\nTotal uploaded: {0}\nTotal failed uploaded: {1}\n" \
        "Failed uploads stored in error.log\n" \
        "Reported saved in report.log.\n" \
        .format(counter.value, FAILED_COUNT)
    report_log.write(report)
    report_log.close()

    #Output report to user
    sys.stdout.flush()
    sys.stdout.write(report)


def print_status(counter, lock):
    '''Print the current status of uploaded files.'''
    global TOTAL

    lock.acquire()
    percentage_uploaded = format(
        (float(counter.value) / float(TOTAL)) * 100,
        '.8f'
    )
    lock.release()

    sys.stdout.flush()
    sys.stdout.write("\r{0}% Uploaded. ".format(percentage_uploaded))


def get_min_id(table_name):
    '''Return the minimum id from table_name where uploaded=0'''

    query = "SELECT MIN(id) FROM {0} WHERE uploaded=0".format(table_name)

    connect = olrcdb.DatabaseConnection()
    result = connect.execute_query(query)
    result_tuple = result.fetchone()
    if not result_tuple[0]:
        sys.exit("Nothing to upload from table {0}".format(table_name))
    return int(result_tuple[0])


if __name__ == "__main__":

    check_env_args()
    start_reporting()

    CONTAINER = sys.argv[1]
    table_name = sys.argv[2]
    n_processes = sys.argv[3]

    TOTAL = get_total_to_upload(table_name)
    counter = Value("i", get_total_uploaded(table_name))
    RANGE = get_min_id(table_name)
    lock = Lock()
    id_range = Value("i", get_min_id(table_name))
    processes = []

    olrc_connect()
    # Limit is the number of rows a process uploads at a time.
    # Range is the range of ids a process uploads.
    for process in range(int(n_processes)):
        p = Process(
            target=upload_table,
            args=(
                lock,
                id_range,
                table_name, counter))
        p.start()
        processes.append(p)

    #Join all processes
    for process in processes:
        process.join()
    end_reporting(counter)
