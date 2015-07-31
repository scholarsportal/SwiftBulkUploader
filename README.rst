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
* pip (virtualenv encouraged)

*******************
Installing
*******************

Until we're in pypi::

    virtualenv swiftbulkuploader
    cd swiftbulkuploader
    source bin/activate
    git clone https://github.com/cudevmaxwell/SwiftBulkUploader.git 
    cd SwiftBulkUploader 
    git checkout removemysql
    cd .. 
    pip install --editable SwiftBulkUploader

*******************
Usage
*******************

1. Index target directory with prepare:: 

    $ swiftbulkuploader prepare /path/to/dir /path/to/other/dir

This creates a database and populates it with paths.

2. Upload files as stored in step 1::

    $ swiftbulkuploader upload --os-username=username --os-password=password --os-tenant-name=tenant --os-auth-url=url test 
 
