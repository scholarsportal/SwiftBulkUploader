import filesegmenter
import hashlib
import os
import shutil
import swiftclient
import sys
import datetime
import socket

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
FAILED_COUNT = 0

REQUIRED_VARIABLES = [
    'OS_AUTH_URL',
    'OS_USERNAME',
    'OS_TENANT_NAME',
    'OS_PASSWORD'
]


def list_files_rec(source_directory):
    '''
        Given the String target_directory, return a list of
        all files in the directory and all files in it's subdirectories.
    '''
    files = []

    for filename in os.listdir(source_directory):

        file_path = os.path.join(source_directory, filename)

        # Add file name to the list.
        if os.path.isfile(file_path):
            files.append(file_path)
        else:
            sys.stdout.flush()
            sys.stdout.write("\rSearching directory {0}".format(file_path))
            files.extend(list_files_rec(file_path))

    return files


def olrc_upload(files, target_directory):
    ''' Given an array of pathnames, upload these to the olrc under
    target_directory. '''

    # Check connection to OLRC.
    olrc_connect()
    global COUNT, FAILED_COUNT
    total = len(files)
    for source_file in files:

        # Prepend target directory to files and remove the source_directory
        # which is the first part of souce_file.
        target_file = os.path.join(
            target_directory,
            source_file.split('/', 1)[1]
        )

        # Check file not already online.
        if not is_uploaded(source_file, target_file):

            # Upload files less than 1GB
            if os.stat(source_file).st_size < FILE_LIMIT:
                if (olrc_upload_file(source_file, target_file)):
                    COUNT += 1
                else:
                    FAILED_COUNT += 1
                    error_log = open('error.log', 'a')
                    error_log.write("\rFailed: {0}\n".format(source_file))
                    error_log.close()

            # Partition files if they are greater than 1GB before uploading
            else:

                if (olrc_upload_segments(source_file, target_file)):
                    COUNT += 1
                else:
                    FAILED_COUNT += 1
                    error_log = open('error.log', 'a')
                    error_log.write("\rFailed: {0}\n".format(source_file))
                    error_log.close()

        else:
            sys.stdout.flush()
            sys.stdout.write(
                "\rSkipping: {0}, already uploaded.".format(source_file)
            )
            COUNT += 1


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


def olrc_upload_file(source_file, target_file):
    '''Given String source_file, upload the file to the OLRC to target_file
     and return True if successful. '''

    try:
        opened_source_file = open(source_file, 'r')
    except IOError:
        print("Error opening: " + source_file)
        return False
    try:
        sys.stdout.flush()
        sys.stdout.write("\rUploading file {0}".format(source_file))
        swiftclient.client.put_object(
            STORAGE_URL,
            AUTH_TOKEN,
            CONTAINER,
            target_file,
            opened_source_file)
    except swiftclient.ClientException, e:
        return False

    except:
        sys.stdout.flush()
        sys.stdout.write("\rError!\n".format(source_file))
        user_input = raw_input(
            "Please enter anything to continue. Type 'stop' to stop."
        )
        if (user_input == 'stop'):
            sys.exit("Exiting.")
        else:
            olrc_upload_file(source_file, target_file)

    return True


def olrc_connect():
    '''Connect to the OLRC with the global variables. Exit if connection
    fails.'''

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
        sys.exit("Connection to OLRC failed. Check credentials.")


def is_uploaded(source_file, target_file):
    '''Return True if String target is already on the server and its etag
    matches the md5 of the source_file. Delete the file from the server if the
    md5 does not match.'''

    # Swift stat on filename.
    try:
        object_stat = swiftclient.client.head_object(
            STORAGE_URL,
            AUTH_TOKEN,
            CONTAINER,
            target_file
        )
        try:
            etag = object_stat['etag']
        except:
            # Return if no etag
            return False

        md5 = checksum_md5(source_file)
        match = etag == md5

        # Delete the file if the md5 does not match.
        if not match:
            try:
                swiftclient.client.delete_object(
                    STORAGE_URL,
                    AUTH_TOKEN,
                    CONTAINER,
                    target_file
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


def upload_drive(source_directory, target_directory):
    '''
    Given a source directory, loop through it and upload all it's contents
    to the target_directory.
    '''
    global COUNT, FAILED_COUNT

    for filename in os.listdir(source_directory):

        file_path = os.path.join(source_directory, filename)

        # Add file name to the list.
        if os.path.isfile(file_path):
            olrc_upload([file_path], target_directory)
        else:
            upload_drive(file_path, target_directory)

if __name__ == "__main__":

    if not is_env_vars_set():
        set_env_message = "The following environment variables have not " \
            "been set:\n"
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
        "python bulkupload.py container-name source_directory " \
        "target_directory\n" \
        "where source_directory is the directory to be uploaded and " \
        "target_directory is the directory where files and directories in " \
        "source_directory will be stored."

    # Do not execute if no directory provided.
    if total != 4:
        print(usage)
        exit(0)

    CONTAINER = cmd_args[1]
    source_directory = cmd_args[2]
    target_directory = cmd_args[3]

    #Open error log:
    error_log = open('error.log', 'w+')
    error_log.write("From execution {0}:\n".format(
        str(datetime.datetime.now())
    ))
    error_log.close()

    #Upload files without searching first.
    upload_drive(source_directory, target_directory)

    #Save report in file.
    report_log = open('report.log', 'w+')
    report_log.write("From execution {0}:\n".format(
        str(datetime.datetime.now())
    ))
    report = "\nTotal uploaded: {0}\nTotal failed uploaded: {1}\n" \
        "Failed uploads stored in error.log\n" \
        "Reported saved in report.log.\n" \
        .format(COUNT, FAILED_COUNT)
    report_log.write(report)
    report_log.close()

    #Output report to user
    sys.stdout.flush()
    sys.stdout.write(report)
