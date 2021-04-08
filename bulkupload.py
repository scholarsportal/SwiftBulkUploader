import datetime
import os
import sys
import time
from multiprocessing import Process, Lock, Value, Manager

import swiftclient

import olrcdb

# Settings
SEGMENT_SIZE = 100 * 10 ** 6
COUNT = 0
FAILED_COUNT = 0
SLEEP = 1  # Sleep timeout when trying to connect to the database.
LOGDIR = '/data/swiftbulkuploader/logs_upload/'

REQUIRED_VARIABLES = [
    'OS_PASSWORD',
    'OS_USERNAME',
    'OS_PROJECT_NAME',
    'OS_PROJECT_DOMAIN_NAME',
    'OS_USER_DOMAIN_NAME',
    'OS_AUTH_URL',
    'OS_REGION_NAME',
    'OS_INTERFACE',
    'OS_IDENTITY_API_VERSION',
    'MYSQL_HOST',
    'MYSQL_USER',
    'MYSQL_PASSWD',
    'MYSQL_DB'
]


def upload_file(path, connection_storage_url, auth_token, container, path_cutoff="", attempts=0):
    """Given String source_file, upload the file to the OLRC to target_file
     and return True if successful. """
    try:
        opened_source_file = open(path, 'rb')
    except IOError as e:
        try:
            print("Error opening %s: %s" % (path, str(e)))
            return False
        except UnicodeEncodeError:
            print("Error opening (+ unicode error): " + path.encode('utf-8'))
            return False

    swift_path = path

    if path_cutoff:
        swift_path = swift_path.lstrip(path_cutoff)

    # Paths beginning with "/" will lose their folder structure on swift.
    # Removing it will preserve it.
    if swift_path == "/":
        swift_path = swift_path[1:]

    try:
        swiftclient.client.put_object(
            connection_storage_url,
            auth_token,
            container,
            swift_path,
            opened_source_file)
    except (UnicodeDecodeError, ConnectionError, swiftclient.client.ClientException) as e:
        sys.stderr.flush()
        sys.stderr.write(
            "\rError! {0} Uploading {1} to OLRC encountered the following issue: "
            "{2}".format(
                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                path,
                str(e)
            )
        )
        return False

    return True


def olrc_connect():
    """Connect to the OLRC with the global variables. Exit if connection
    fails."""

    global SLEEP

    swift_auth_url, username, password, identity_api_version, os_options = get_env_vars()

    try:
        return swiftclient.client.get_auth(
            swift_auth_url, username, password,
            auth_version=identity_api_version,
            os_options=os_options
        )
    except swiftclient.client.ClientException as e:
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

        return olrc_connect()


def create_container(storage_url, auth_token, container):
    """Create the container on swift."""

    try:
        swiftclient.client.put_container(storage_url, auth_token, container)

    except swiftclient.client.ClientException as e:
        print(e)
        sys.stdout.flush()
        sys.stdout.write(
            "\rError! {0} Failed to create container {1}".format(
                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                container
            )
        )


def env_vars_set(required_variables):
    """Check all the required environment variables are set. Return false if
    any of them are undefined."""
    for required_variable in required_variables:
        if (not os.environ.get(required_variable)
                and os.environ.get(required_variable) != ""):
            return False

    return True


def get_env_vars():
    """Get the global variables for swift client assuming they exist in the
    environment."""

    swift_auth_url = os.environ.get("OS_AUTH_URL")
    username = os.environ.get("OS_USERNAME")
    password = os.environ.get("OS_PASSWORD")

    try:
        identity_api_version = int(os.environ.get("OS_IDENTITY_API_VERSION"))
    except ValueError:
        raise ValueError("Environment variable OS_IDENTITY_API_VERSION needs to be a number. Got %s instead."
                         % os.environ.get("OS_IDENTITY_API_VERSION"))

    os_options = {
        "user_domain_name": os.environ.get("OS_USER_DOMAIN_NAME"),
        "project_name": os.environ.get("OS_PROJECT_NAME"),
        "project_domain_name": os.environ.get("OS_PROJECT_DOMAIN_NAME"),
        "auth_version": identity_api_version,
        "region_name": os.environ.get("OS_REGION_NAME"),
        "endpoint_type": os.environ.get("OS_INTERFACE")
    }

    return swift_auth_url, username, password, identity_api_version, os_options


def upload_table(lock, table_name, container, counter, failed_counter, speed, connection_storage_url,
                 auth_token, entries, path_cutoff=""):
    """
    Given a table_name, upload all the paths from the table where upload is 0.
    Using the range value, complete a BATCH worth of uploads at a time.
    """

    def get_entry():
        try:
            return entries.pop()
        except IndexError:
            return None

    global FAILED_COUNT

    total = get_total_to_upload(table_name)

    # In order for the current process to upload a unique set of files,
    # acquire the lock to read from range's value.
    lock.acquire()
    cur_entry = get_entry()
    lock.release()

    while cur_entry is not None:
        retry = 0
        success = False

        while retry < 5 and not success:
            # If the upload is successful, update the database
            if upload_file(cur_entry[1], connection_storage_url, auth_token, container, path_cutoff=path_cutoff):
                lock.acquire()
                counter.value += 1
                lock.release()
                set_uploaded(cur_entry[0], table_name)
                success = True
            else:
                retry += 1
                time.sleep(1)
                connection_storage_url, auth_token = olrc_connect()

        if not success:
            lock.acquire()
            sys.stdout.flush()
            sys.stdout.write(
                "Error! {0} Upload of {1} to OLRC failed"
                " after {2} attempts.\n".format(
                    time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                    cur_entry[1],
                    retry
                )
            )

            failed_counter.value += 1
            error_log = open(LOGDIR + table_name + '.upload.error.log', 'a')
            error_log.write(
                "\rFailed: {0}\n".format(
                    cur_entry[1].encode('utf-8')))
            error_log.close()
            lock.release()

        print_status(counter, lock, speed, table_name, total)

        lock.acquire()
        cur_entry = get_entry()
        lock.release()


def get_total_to_upload(table_name):
    """Given a table_name, get the total number of rows"""

    query = "SELECT COUNT(*) FROM {0}".format(table_name)

    connect = olrcdb.DatabaseConnection()
    result = connect.execute_query(query)
    result_tuple = result.fetchone()
    return result_tuple[0]


def get_total_uploaded(table_name):
    """Given a table_name, get the total number of rows where upload is 1."""

    query = "SELECT COUNT(*) FROM {0} WHERE uploaded=1".format(table_name)

    connect = olrcdb.DatabaseConnection()
    result = connect.execute_query(query)
    result_tuple = result.fetchone()
    return result_tuple[0]


def set_uploaded(id, table_name):
    """For the given path, set uploaded to 1 in table_name."""
    query = "UPDATE {0} set uploaded='1' WHERE id='{1}'".format(
        table_name,
        id
    )

    connect = olrcdb.DatabaseConnection()
    connect.execute_query(query)


def check_env_args():
    """Do checks on the environment and args."""
    # Check environment variables
    if not env_vars_set(REQUIRED_VARIABLES):
        set_env_message = "The following environment variables need to be " \
                          "set:\n"
        set_env_message += " \n".join(REQUIRED_VARIABLES)
        set_env_message += "\nPlease set these environment variables to " \
                           "connect to the OLRC."
        print(set_env_message)
        exit(0)

    total = len(sys.argv)
    usage = "Please pass in a few arguments, see example below \n" \
            "python bulkupload.py container-name mysql-table n-processes path-cutoff\n" \
            "where mysql-table is table created from prepareupload.py, " \
            "n-process is the number of processes created to run this script and" \
            " path-cutoff is the string that indicates from where the path is" \
            " truncated from the front."

    # Do not execute if no directory provided.
    if total != 4 and total != 5:
        print(usage)
        exit(0)


def start_reporting(table_name):
    """Create an error log file. Note the time of execution."""

    # Open error log:
    error_log = open(LOGDIR + table_name + '.upload.error.log', 'w+')
    error_log.write("From execution {0}:\n".format(
        str(datetime.datetime.now())
    ))
    error_log.close()


def end_reporting(counter, failed_counter, table_name):
    """Create a report log. Output upload summary."""

    report_log = open(LOGDIR + table_name + '.upload.report.log', 'w+')
    report_log.write("From execution {0}:\n".format(
        str(datetime.datetime.now())
    ))
    report = "\nTotal uploaded: {0}\nTotal failed uploaded: {1}\n" \
             "Failed uploads stored in error.log\n" \
             "Reported saved in report.log.\n" \
        .format(counter.value, failed_counter.value)
    report_log.write(report)
    report_log.close()

    # Output report to user
    sys.stdout.flush()
    sys.stdout.write(report)


def print_status(counter, lock, speed, table_name, total):
    """Print the current status of uploaded files."""
    lock.acquire()
    percentage_uploaded = format(
        (float(counter.value) / float(total)) * 100,
        '.8f'
    )
    lock.release()

    sys.stdout.flush()
    sys.stdout.write("\r{0}% Uploaded at {1:.2f} uploads/second. ".format(
        percentage_uploaded, speed.value))

    # Log the final count
    report = open(LOGDIR + table_name + ".upload.out", 'w+')
    report.write(
        "\r{0}% Uploaded at {1:.2f} uploads/second. ".format(
            percentage_uploaded, speed.value))
    report.close()


def get_min_id(table_name):
    """Return the minimum id from table_name where uploaded=0"""

    query = "SELECT MIN(id) FROM {0} WHERE uploaded=0".format(table_name)

    connect = olrcdb.DatabaseConnection()
    result = connect.execute_query(query)
    result_tuple = result.fetchone()
    if not result_tuple[0]:
        sys.exit("Nothing to upload from table {0}".format(table_name))
    return int(result_tuple[0])


def get_all_entries_to_upload():
    """Return a tuple of all database entries that need to be uploaded."""
    # We order the entries descending because we are going to be popping this list (i.e. starting by the end)
    query = "SELECT * FROM {0} WHERE uploaded=0 ORDER BY id DESC".format(table_name)
    connect = olrcdb.DatabaseConnection()
    result = connect.execute_query(query)
    return result.fetchall()


def set_speed(lock, counter, speed, entries):
    """Calculate the upload speed for the next minute and set it in the
    speed."""

    while entries:
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

    container = sys.argv[1]  # Swift container files will be uploaded to.
    table_name = sys.argv[2]  # Name of table to read file paths from.
    n_processes = int(sys.argv[3])  # Number of processes to create for uploading.
    if len(sys.argv) == 5:
        path_cutoff = sys.argv[4]  # The path cutoff
    else:
        path_cutoff = ''

    storage_url, auth_token = olrc_connect()
    create_container(storage_url, auth_token, container)

    start_reporting(table_name)

    manager = Manager()
    # Integer value of uploaded files within target table.
    counter = Value("i", get_total_uploaded(table_name))
    failed_counter = Value("i", 0)
    lock = Lock()

    # Load entries into manager list
    entries = manager.list(get_all_entries_to_upload())

    speed = Value("d", 0.0)  # Tracker for upload speed.

    processes = []

    # Create a new process n times.
    for process in range(n_processes):
        p = Process(
            target=upload_table,
            args=(
                lock,
                table_name,
                container,
                counter,
                failed_counter,
                speed,
                storage_url,
                auth_token,
                entries
            ),
            kwargs={"path_cutoff": path_cutoff}
        )

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
            entries
        ))
    p.start()
    processes.append(p)

    # Join all processes
    for process in processes:
        process.join()

    end_reporting(counter, failed_counter, table_name)
