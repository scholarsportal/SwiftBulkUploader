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
from math import floor


#Settings
AUTH_VERSION = 2
SWIFT_AUTH_URL = ''
USERNAME = ''
PASSWORD = ''
CONTAINER = ''
AUTH_TOKEN = ''
STORAGE_URL = ''
SEGMENT_SIZE = 100*10**6
COUNT = 0
TOTAL = 0
FAILED_COUNT = 0
BATCH = 1000  # the number of rows a process uploads at a time.
SLEEP = 1  # Sleep timeout when trying to connect to the database.

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


def upload_file(path, attempts=0):
    '''Given String source_file, upload the file to the OLRC to target_file
     and return True if successful. '''

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
        time.sleep(1)
        if (attempts > 5):

            sys.stdout.flush()
            sys.stdout.write("\rError! {0}\n".format(e))
            sys.stdout.write(
                "Error! {0} Upload to OLRC failed"
                " after {1} attempts.\n".format(
                    time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                    attempts
                )
            )
            return False
        return upload_file(path, attempts + 1)

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
        SLEEP += 1

        olrc_connect()


def env_vars_set():
    '''Check all the required environment variables are set. Return false if
    any of them are undefined.'''

    global REQUIRED_VARIABLES
    for required_variable in REQUIRED_VARIABLES:
        if (not os.environ.get(required_variable)
                and os.environ.get(required_variable) != ""):
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


def upload_table(lock, range, table_name, counter, speed):
    '''
    Given a table_name, upload all the paths from the table where upload is 0.
    Using the range value, complete a BATCH worth of uploads at a time.
    '''
    global FAILED_COUNT, BATCH

    connect = olrcdb.DatabaseConnection()

    # In order for the current process to upload a unique set of files,
    # acquire the lock to read from range's value.
    lock.acquire()
    while range.value <= TOTAL:

        # Grab a "BATCH" worth of file paths to upload.
        query = (
            "SELECT * FROM {0} WHERE uploaded=0"
            " AND id >= {1} AND id <{2}".format(
                table_name, range.value, range.value + BATCH))

        # Let other processes know this batch has been accounted for.
        range.value += BATCH
        lock.release()

        # Fetch results.
        result = connect.execute_query(query)
        path_tuple = result.fetchone()

        # Loop until we run out of rows from the batch
        while (path_tuple):

            # If the upload is successful, update the database
            if upload_file(path_tuple[1]):
                lock.acquire()
                counter.value += 1
                lock.release()
                set_uploaded(path_tuple[0], table_name)

            else:
                FAILED_COUNT += 1
                error_log = open(table_name+'.upload.error.log', 'a')
                error_log.write(
                    "\rFailed: {0}\n".format(
                        path_tuple[1].encode('utf-8')))
                error_log.close()

            print_status(counter, lock, speed, table_name)

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
    if not env_vars_set():
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


def start_reporting(table_name):
    '''Create an error log file. Note the time of execution.'''

    #Open error log:
    error_log = open(table_name + '.upload.error.log', 'w+')
    error_log.write("From execution {0}:\n".format(
        str(datetime.datetime.now())
    ))
    error_log.close()


def end_reporting(counter, table_name):
    '''Create a report log. Output upload summary.'''

    report_log = open(table_name + '.upload.report.log', 'w+')
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


def print_status(counter, lock, speed, table_name):
    '''Print the current status of uploaded files.'''
    global TOTAL

    lock.acquire()
    percentage_uploaded = format(
        (float(counter.value) / float(TOTAL)) * 100,
        '.8f'
    )
    lock.release()

    sys.stdout.flush()
    sys.stdout.write("\r{0}% Uploaded at {1:.2f} uploads/second. ".format(
        percentage_uploaded, speed.value))

    #Log the final count
    report = open(table_name + ".upload.out", 'w+')
    report.write(
        "\r{0}% Uploaded at {1:.2f} uploads/second. ".format(
            percentage_uploaded, speed.value))
    report.close()


def get_min_id(table_name):
    '''Return the minimum id from table_name where uploaded=0'''

    query = "SELECT MIN(id) FROM {0} WHERE uploaded=0".format(table_name)

    connect = olrcdb.DatabaseConnection()
    result = connect.execute_query(query)
    result_tuple = result.fetchone()
    if not result_tuple[0]:
        sys.exit("Nothing to upload from table {0}".format(table_name))
    return int(result_tuple[0])


def set_speed(lock, counter, speed, range):
    '''Calculate the upload speed for the next minute and set it in the
    speed.'''

    while range.value <= TOTAL:
        lock.acquire()
        start_count = counter.value
        start_time = time.time()
        lock.release()

        time.sleep(5)  # Sleep for 60 seconds.

        lock.acquire()
        stop_count = counter.value
        stop_time = time.time()
        lock.release()

        lock.acquire()
        speed.value = (
            float(stop_count - start_count) /
            float(stop_time - start_time)
        )

        # Save the speed calculation.
        lock.release()


if __name__ == "__main__":

    check_env_args()

    CONTAINER = sys.argv[1]  # Swift container files will be uploaded to.
    table_name = sys.argv[2]  # Name of table to read file paths from.
    n_processes = sys.argv[3]  # Number of processes to create for uploading.

    olrc_connect()

    TOTAL = get_total_to_upload(table_name)

    # Integer value of uploaded files within target table.
    counter = Value("i", get_total_uploaded(table_name))
    lock = Lock()

    # Processes share this id_range value and block of a chunk of ids
    # for themselves to upload.
    id_range = Value("i", get_min_id(table_name))

    speed = Value("d", 0.0)  # Tracker for upload speed.

    processes = []

    start_reporting(table_name)

    # Create a new process n times.
    for process in range(int(n_processes)):
        p = Process(
            target=upload_table,
            args=(
                lock,
                id_range,
                table_name,
                counter,
                speed))

        # Execute the upload_table function
        p.start()
        processes.append(p)

    # Create a process to calculate the speed of uploads.
    p = Process(
        target=set_speed,
        args=(
            lock,
            counter,
            speed,
            id_range))
    p.start()
    processes.append(p)

    #Join all processes
    for process in processes:
        process.join()

    end_reporting(counter, table_name)
