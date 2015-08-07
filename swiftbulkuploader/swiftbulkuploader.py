# -*- coding: utf-8 -*-
from __future__ import division
import click
import sqlite3
import os
import collections
import contextlib
import signal
import swiftclient
import keystoneclient
import subprocess32 as subprocess
import copy

DEFAULT_DB_FILENAME = 'swiftbulkuploader.db'
AUTH_VERSION = 2

# An object which we can pass values from the group to subcommands.
State = collections.namedtuple('state', ('verbose', 'connection', 'db'))

# Fall back to the usual sigpipe behaviour.
# This fixes cases of weird errors when you pipe to less.
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

def echoerror(message):
    """A utility function to print the message to stderr"""

    click.secho(message, fg='red', err=True)


@contextlib.contextmanager
def _error_check():
    """This contextmanager is used to encapsulate reused error checking."""

    try:
        yield None
    except sqlite3.Error, e:
        echoerror(u'Database Error: {}.'.format(e))
        raise click.Abort


# ====================================
# Main Command Group
# ====================================

@click.group()
@click.option('--verbose', default=False, is_flag=True,
              help='Enable verbose output.')
@click.option('--db', type=click.Path(), default=DEFAULT_DB_FILENAME,
              help='The file path for the database file. Default: {}'.format(
              os.path.join(os.getcwd(),DEFAULT_DB_FILENAME)))
@click.pass_context
def cli(ctx, verbose, db):
    if verbose:
        click.echo('Verbose mode on.')
    try:
        connection = sqlite3.connect(db)
        connection.row_factory = sqlite3.Row
        ctx.obj = State(verbose, connection, db)
    except sqlite3.Error, e:
        echoerror('Unable to connect to {}.'.format(db))
        echoerror('{}.'.format(e))
        raise click.Abort


# ====================================
# Prepare Subcommand
# ====================================

@cli.command(short_help='Add file paths from directories.')
@click.argument('directories', type=click.Path(exists=True,
                dir_okay=True, readable=True, resolve_path=True),
                nargs=-1)
@click.option('--cleanup',
              help=('Remove the parent directory path '
                    'from the completed file name in Swift. '
                    'For example: /home/archivist/a1/b2/c3/file.txt '
                    'found under the provided path /home/archivist/a1/b2/c3 '
                    'would be found in Swift with the filename file.txt.'),
              default=False, is_flag=True)
@click.pass_context
def prepare(ctx, directories, cleanup):
    """Add the file paths of files in the given directories to the database.

    This command does not follow symbolic links to directories,
    to avoid infinite recursion.

    Any files which cannot be read or directories which cannot
    be entered are stored as well, for later review.
    """

    if ctx.obj.verbose and len(directories) == 0:
        click.echo('No directories specified!')

    with ctx.obj.connection as connection, _error_check():
        if ctx.obj.verbose:
            click.echo("Creating tables, if they don't already exist...", nl=False)        
        connection.execute("""CREATE TABLE IF NOT EXISTS goodfiles
                              (filepath TEXT NOT NULL UNIQUE, 
                               status TEXT, 
                               bytes INT, 
                               objectname TEXT)""")
        connection.execute("""CREATE TABLE IF NOT EXISTS badfiles
                              (filepath TEXT NOT NULL UNIQUE)""")
        connection.execute("""CREATE TABLE IF NOT EXISTS baddirectories
                              (directorypath TEXT NOT NULL UNIQUE)""")        
        if ctx.obj.verbose:
            click.echo('Done!')
        
        file_counter = 0

        # Create a closure which accepts an error thrown by os.listdir()
        # if there's an error during os.walk()
        def error_catch(error):
            if ctx.obj.verbose:
                echoerror('Error accessing {}'.format(error.filename))
            if os.path.isdir(error.filename):
                connection.execute("""INSERT OR IGNORE 
                                      INTO baddirectories 
                                      VALUES (?)""",(error.filename,))
            else:
                connection.execute("""INSERT OR IGNORE 
                                      INTO badfiles 
                                      VALUES (?)""",(error.filename,))

        for directory in directories:
            for root, dirs, files in os.walk(directory, onerror=error_catch):
                for name in files:                        
                    filepath = os.path.join(root, name)
                    if os.access(filepath, os.R_OK): # Readable
                        size = os.path.getsize(filepath)
                        if cleanup and filepath.startswith(directory):
                            objectname = filepath[len(directory):]
                        else:
                            objectname = filepath
                        connection.execute("""INSERT OR IGNORE 
                                              INTO goodfiles  
                                              VALUES (?, ?, ?, ?)""",(filepath, 
                                                                      'unprocessed', 
                                                                      size, 
                                                                      objectname))
                        if ctx.obj.verbose:
                            click.secho('{}'.format(filepath), fg='green')
                    else:
                        connection.execute("""INSERT OR IGNORE 
                                              INTO badfiles 
                                              VALUES (?)""", (filepath, ))
                        if ctx.obj.verbose:
                            echoerror('Error accessing {}'.format(filepath))

                    file_counter += 1                     
    
        if ctx.obj.verbose:
            click.echo('Number of files processed: {}'.format(file_counter))


# ====================================
# Count Subcommand
# ====================================

@cli.command(short_help='Count stored paths.')
@click.pass_context
def count(ctx):
    """Outputs the number of file and directory paths stored in the database."""

    with ctx.obj.connection as connection, _error_check():            
        cursor = connection.cursor()

        # First thing, we see if any of the tables have been created yet.
        cursor.execute("""SELECT name FROM sqlite_master 
                          WHERE type='table' AND name='goodfiles'""")

        if cursor.fetchone() == None:
            click.echo('No paths have been added to the database yet.')
            ctx.abort()
               
        cursor.execute("""SELECT COUNT(*) FROM goodfiles""")
        click.echo('Stored accessible file paths: {}'.format(cursor.fetchone()[0]))

        cursor.execute("""SELECT COUNT(*) FROM goodfiles
                          WHERE status = 'unprocessed'""")
        click.echo('  Not Uploaded: {}'.format(cursor.fetchone()[0])) 
        
        cursor.execute("""SELECT COUNT(*) FROM goodfiles
                          WHERE status = 'uploaded'""")
        click.echo('      Uploaded: {}'.format(cursor.fetchone()[0]))      

        cursor.execute("""SELECT COUNT(*) FROM goodfiles
                          WHERE status = 'error'""")
        click.echo('  Upload Error: {}'.format(cursor.fetchone()[0]))
        
        cursor.execute("""SELECT COUNT(*) FROM badfiles""")    
        click.echo('Inaccessible file paths: {}'.format(cursor.fetchone()[0]))        
        
        cursor.execute("""SELECT COUNT(*) FROM baddirectories""")            
        click.echo('Inaccessible directory paths: {}'.format(cursor.fetchone()[0]))  


# ====================================
# Dump Subcommand
# ====================================

@cli.command(short_help='Dump stored paths.')
@click.option('--table', type=click.Choice(['goodfiles', 'badfiles',
              'baddirectories', 'all']), default='all')
@click.pass_context
def dump(ctx, table):
    """Outputs the file and directory paths stored in the database. 

    You can select which of the three tables you would like to output. 

    \b
    goodfiles       - The files which were readable when 
                      the prepareupload command was run.
    \b
    badfiles        - The files which were inaccessible when 
                      the prepareupload command was run. 
                      This is usually caused by file permission issues.
    \b
    baddirectories  - The directories which were inaccessible when 
                      the prepareupload command was run. 
                      This is usually caused by file permission issues.

    \b 
    all             - The default, output all three.

    After fixing file access issues, the stored file paths should be
    cleared and prepared again. 
    """

    with ctx.obj.connection as connection, _error_check():        
        cursor = connection.cursor()

        # First thing, we see if any of the tables have been created yet.
        cursor.execute("""SELECT name FROM sqlite_master 
                          WHERE type='table' AND name='goodfiles'""")

        if cursor.fetchone() == None:
            click.echo('No paths have been added to the database yet.')
            ctx.abort()

        if table == 'goodfiles' or table == 'all':
            click.echo('Files which the current user can access:')
            click.echo('Path, Status, Size (in bytes), Object Name In Stack')
            cursor.execute("""SELECT filepath, status, bytes, objectname  
                              FROM goodfiles ORDER BY filepath""")
            for row in cursor:
                click.echo('{}, {}, {}, {}'.format(row['filepath'], 
                                                   row['status'], 
                                                   row['bytes'], 
                                                   row['objectname']))

        if table == 'badfiles' or table == 'all':
            click.echo('Files which the current user cannot access:' )
            cursor.execute("""SELECT filepath 
                              FROM badfiles ORDER BY filepath""")
            for row in cursor:
                click.echo('{}'.format(row['filepath']))

        if table == 'baddirectories' or table == 'all':
            click.echo('Directories which the current user cannot access:')
            cursor.execute("""SELECT directorypath 
                              FROM baddirectories ORDER BY directorypath""")
            for row in cursor:
                click.echo('{}'.format(row['directorypath']))


# ====================================
# Clear Subcommand
# ====================================

@cli.command(short_help='Delete all stored paths.')
@click.pass_context
def clear(ctx):
    """Clears the database of stored paths.

    The three tables are dropped, and the sqlite VACUUM
    command is run, to reclaim the disk space used by the
    database file.
    """

    with ctx.obj.connection as connection,  _error_check():
        connection.execute("""DROP TABLE IF EXISTS goodfiles""")
        connection.execute("""DROP TABLE IF EXISTS badfiles""")
        connection.execute("""DROP TABLE IF EXISTS baddirectories""")
        connection.execute("""VACUUM""")
        click.echo('Database cleared.')


# ====================================
# Upload Subcommand
# ====================================

@cli.command(short_help='Upload all stored paths.')
@click.option('--username',
              help='Username, or use OS_USERNAME environment variable.',
              required=True, envvar='OS_USERNAME')
@click.option('--password',
              help='Password, or use OS_PASSWORD environment variable.',
              required=True, prompt=True, hide_input=True, envvar='OS_PASSWORD',)
@click.option('--tenant-name',
              help='Tenant Name, or use OS_TENANT_NAME environment variable.',
              required=True, envvar='OS_TENANT_NAME')
@click.option('--auth-url',
              help='Auth URL, or use OS_AUTH_URL environment variable.', 
              required=True, envvar='OS_AUTH_URL')
@click.option('--auth-version',
              help='Auth version.', 
              default=AUTH_VERSION)
@click.option('--region-name',
              help='Region Name, or use OS_REGION_NAME environment variable.',
              required=True, envvar='OS_REGION_NAME')
@click.option('--debug',
              help=('Pass swift upload the debug option, '
                    'which will show the curl commands and results of all '
                    'http queries regardless of result status.'),
              default=False, is_flag=True)
@click.option('--info',
              help=('Pass swift upload the info option, '
                    'which will show the curl commands and results of all '
                    'http queries which return an error.'),
              default=False, is_flag=True)
@click.option('--segment-size',
              help='Pass swift upload the segment-size option. Default is 1G.',
              default='1G')
@click.option('--batch-size',
              help='Number of subcommands to run in parallel. Default is 5.',
              default=5)
@click.argument('container', required=True, nargs=1)
@click.pass_context
def upload(ctx, username, password, tenant_name, auth_url, auth_version, region_name,
           debug, info, segment_size, batch_size, container):
    """Upload all stored paths which the user can access to the given container."""

    with ctx.obj.connection as connection, _error_check():
        cursor = connection.cursor()

        # First thing, we see if any of the tables have been created yet.
        cursor.execute("""SELECT name FROM sqlite_master 
                          WHERE type='table' AND name='goodfiles'""")

        if cursor.fetchone() == None:
            click.echo('No paths have been added to the database yet.')
            ctx.abort()

        cursor.execute("""SELECT COUNT(*) FROM goodfiles
                          WHERE status = 'unprocessed'""")
        number_of_paths = cursor.fetchone()[0]
        width_of_number_of_paths = len(str(number_of_paths))

        if number_of_paths == 0:
            click.echo('No files are ready to upload.')
            ctx.abort()

        cursor.execute("""SELECT SUM(bytes) FROM goodfiles
                          WHERE status = 'unprocessed'""")
        total_bytes = cursor.fetchone()[0]
        width_of_total_bytes = len(str(total_bytes))

        cursor.execute("""SELECT filepath, bytes, objectname FROM goodfiles 
                          WHERE status = 'unprocessed' 
                          ORDER BY filepath""")

        # Build up the swift command
        command = ['swift']

        if ctx.obj.verbose:
            command.append('--verbose')
        else:
            command.append('--quiet')

        if debug:
            command.append('--debug')
        if info:
            command.append('--info')

        command.append('--os-username={}'.format(username))
        command.append('--os-password={}'.format(password))
        command.append('--os-tenant-name={}'.format(tenant_name))
        command.append('--os-auth-url={}'.format(auth_url))
        command.append('--auth-version={}'.format(auth_version))
        command.append('upload')
        command.append('--segment-size={}'.format(segment_size))
        command.append(container)

        row_counter = 0 
        processed_bytes = 0

        click.echo(('Current File Path | '
                    'Number of Paths Processed | '
                    'Number of Bytes Processed'))

        # Keep track of our batched jobs
        Job = collections.namedtuple('job', ('row', 'process'))
        jobs = []

        def check_result(job):
            returncode = job.process.wait()
            # An extra connection is needed here because of this bug:
            # http://bugs.python.org/issue23129
            # An update within the row iterator causes duplicate rows
            # to be emitted. 
            with sqlite3.connect(ctx.obj.db) as update_connection: 
                if returncode != 0:           
                    connection.execute("""UPDATE goodfiles
                                          SET status = 'error' 
                                          WHERE filepath = ?""",(job.row['filepath'],))                

                    echoerror('File Upload Error: {}.'.format(job.row['filepath']))
                
                else:
                    connection.execute("""UPDATE goodfiles
                                          SET status = 'uploaded' 
                                          WHERE filepath = ?""",(job.row['filepath'],))

        try:
            for row in cursor:       
                uploadcommand = copy.deepcopy(command)
                uploadcommand.append('--object-name={}'.format(row['objectname']))
                uploadcommand.append(row['filepath'])
    
                if ctx.obj.verbose:
                    click.echo('\nRunning command:\n{}'.format(' '.join(uploadcommand)))
              
                jobs.append(Job(row, subprocess.Popen(uploadcommand, start_new_session=True)))

                processed_bytes += row['bytes']
                row_counter += 1 

                # Use the carriage return to move the cursor
                # back to the beginning of the line.
                if not ctx.obj.verbose:
                    click.echo('\r', nl=False)

                click.echo('{} | '.format(row['filepath']), nl=False)
                justified_counter = str(row_counter).rjust(width_of_number_of_paths, '0')
                click.echo('{}/{} {:.2%} | '.format(justified_counter, number_of_paths, 
                                                   row_counter/number_of_paths), nl=False)
                justified_processed_bytes = str(processed_bytes).rjust(width_of_total_bytes, '0')
                click.echo('{}/{} {:.2%}   '.format(justified_processed_bytes, total_bytes, 
                                                   processed_bytes/total_bytes), nl=False)
    
                if len(jobs) >= batch_size:  
                    for job in jobs:
                        check_result(job)
                    jobs = []                                 

            # Process any jobs remaining. 
            # This happens at the end of the iterator, 
            # when the number of jobs remaining
            # isn't more than the batch size.  
            for job in jobs:
                check_result(job)

        except KeyboardInterrupt:
            try: 
                click.echo("\nWaiting for upload commands to complete, then exiting...")
                for job in jobs:
                    check_result(job)                 
            except KeyboardInterrupt:
                click.echo("\nKilling outstanding upload commands...")
                for job in jobs:
                    job.process.kill()           
                raise     
            raise   

        click.echo("\nDone!")

if __name__ == '__main__':
    cli()         
