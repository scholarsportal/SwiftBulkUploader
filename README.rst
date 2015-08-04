===============================
Swift Bulk Uploader
===============================

.. image:: https://img.shields.io/travis/OLRC/swiftbulkuploader.svg
        :target: https://travis-ci.org/OLRC/swiftbulkuploader

.. image:: https://img.shields.io/pypi/v/swiftbulkuploader.svg
        :target: https://pypi.python.org/pypi/swiftbulkuploader


Swift Bulk Uploader makes it easy to upload entire directories to the OLRC. The Ontario Library Research Cloud (OLRC) project is a collaboration of Ontario’s university libraries to build a high capacity, geographically distributed storage network, based on OpenStack and the Swift object store.

* Documentation: https://swiftbulkuploader.readthedocs.org.

These scripts assist in uploading an entire directory onto swift. They were intended for directories containing millions of files up to several terabytes large.

*******************
Requirements
*******************

* Python 2.7+
* python-mysqldb 
* `Python Swiftclient <https://pypi.python.org/pypi/python-swiftclient>`_
* MySQL database
* The following environment variables
 * OS_AUTH_URL
 * OS_USERNAME
 * OS_TENANT_NAME
 * MYSQL_HOST
 * MYSQL_USER
 * MYSQL_PASSWD
 * MYSQL_DB

*******************
Usage
*******************

1. Index target directory with prepareupload.py:: 

    $ prepareupload PathTodirectory MysqlTableName

This creates a table MysqlTableName and populates it with paths to all files in PathToDirectory. It outputs the following log files:

* MysqlTableName.prepare.error.log # Will log any file path that failed when written to the database.
* MysqlTableName.prepare.out # A real time log file as file paths are being parsed.

While the above command is running, in a new tab run the following command to watch the progress of the parsing::

    $ tail -f MysqlTableName.prepare.out

2. Upload files as stored in step 1::

    $ bulkupload containername MysqlTableName 3

This creates 3 processes that reads from MysqlTableName and uploads files into the container containername. If the upload process is stopped, it can be re-run and continue uploading without reuploading already uploaded files. Increase 3 to an appropriate number that your CPU can handle for faster speeds.

This script outputs the following files:

* MysqlTableName.upload.out # Real time progress of upload
* MysqlTableName.error.log # Logs failed uploads
* MysqlTableName.report.log # Created when upload is complete with summary of results.

To check the progress of the upload, run the following command::

    $ tail -f MysqlTableName.upload.out
