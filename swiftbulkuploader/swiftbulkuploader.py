# -*- coding: utf-8 -*-
from __future__ import division
import click
import os
import collections
import contextlib
import signal
import swiftclient
import keystoneclient
import subprocess32 as subprocess
import copy
import sqlalchemy

AUTH_VERSION = 2

# An object which we can use to pass state 
# from the group to subcommands.
State = collections.namedtuple('state', ('verbose', 'engine'))

# Fall back to the usual sigpipe behaviour.
# This fixes cases of weird errors when you pipe to less.
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

def echo_error(message):
    """A utility function to print the message to stderr."""

    click.secho(message, fg='red', err=True)

# ====================================
# Database Definitions
# ====================================

DEFAULT_DB_URL = 'sqlite:///swiftbulkuploader.db'

metadata = sqlalchemy.MetaData()

paths = sqlalchemy.Table('paths', metadata,
    sqlalchemy.Column('id', sqlalchemy.Integer, 
                            sqlalchemy.Sequence('paths_id_seq'), 
                            primary_key=True),
    sqlalchemy.Column('path', sqlalchemy.Text()),
    sqlalchemy.Column('pathtype', sqlalchemy.Enum('directory', 
                                                  'file')),
    sqlalchemy.Column('accessible', sqlalchemy.Boolean),
    sqlalchemy.Column('status', sqlalchemy.Enum('unprocessed', 
                                                'processed', 
                                                'error')),
    sqlalchemy.Column('bytes', sqlalchemy.BigInteger),
    sqlalchemy.Column('objectname', sqlalchemy.Text),
)

# ====================================
# Main Command Group
# ====================================

@click.group()
@click.option('--verbose', default=False, is_flag=True,
              help='Enable verbose output.')
@click.option('--db-url', default=DEFAULT_DB_URL,
              help='The sqlalchemy database URL to use. Default: {}'.format(
              DEFAULT_DB_URL))
@click.pass_context
def cli(ctx, verbose, db_url):
    if verbose:
        click.echo('Verbose mode on.')

    try:
        engine = sqlalchemy.create_engine(db_url, echo=verbose)
        metadata.create_all(engine) 
        if verbose: click.echo("Created table 'paths' if it did not exist.")        
        ctx.obj = State(verbose, engine)
    except sqlalchemy.exc.SQLAlchemyError as e:
        echo_error('Unable to connect to {}.'.format(db_url))
        echo_error('{}.'.format(e))
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

    # Create a closure which accepts an error thrown by os.listdir()
    # if there's an error during os.walk()
    def error_catch(error):
        if ctx.obj.verbose:
            echo_error('Error accessing {}'.format(error.filename))  

        pathtype = 'directory' if os.path.isdir(error.filename) else 'file'
        with ctx.obj.engine.begin() as transaction:
            exists = transaction.execute(
                                sqlalchemy.select([paths.c.id]).\
                                where(paths.c.path == error.filename)).\
                                fetchone()
            if exists == None:
                transaction.execute(paths.insert().values(
                                          path=error.filename,
                                          pathtype=pathtype,
                                          accessible=False,
                                          status='unprocessed',
                                          bytes=0,
                                          objectname=''))
    
    file_counter = 0
    for directory in directories:
        for root, dirs, files in os.walk(directory, onerror=error_catch):
            for name in files:                        
                filepath = os.path.join(root, name)
                access = os.access(filepath, os.R_OK)
                size = os.path.getsize(filepath) if access else 0
                if cleanup and filepath.startswith(directory):
                    objectname = filepath[len(directory):]
                else:
                    objectname = filepath

                with ctx.obj.engine.begin() as transaction:
                    exists = transaction.execute(
                                         sqlalchemy.select([paths.c.id]).\
                                         where(paths.c.path == filepath)).\
                                         fetchone()
                    if exists == None:
                        transaction.execute(paths.insert().values(
                                                  path=filepath,
                                                  pathtype='file',
                                                  accessible=access,
                                                  status='unprocessed',
                                                  bytes=size,
                                                  objectname=objectname))
                file_counter += 1
                if ctx.obj.verbose:
                    click.echo(filepath)                       
    
    if ctx.obj.verbose:
        click.echo('Number of files processed: {}'.format(file_counter))

# ====================================
# Count Subcommand
# ====================================

@cli.command(short_help='Count stored paths.')
@click.pass_context
def count(ctx):
    """Outputs the number paths stored in the database."""

    count_all = sqlalchemy.select([sqlalchemy.func.count(paths.c.id)])
    count_unprocessed = count_all.\
                        where(paths.c.status == 'unprocessed').\
                        where(paths.c.accessible == True)
    count_processed = count_all.\
                      where(paths.c.status == 'processed').\
                      where(paths.c.accessible == True)
    count_error = count_all.\
                      where(paths.c.status == 'error').\
                      where(paths.c.accessible == True)
    count_inaccessible_files = count_all.\
                               where(paths.c.accessible == False).\
                               where(paths.c.pathtype == 'file')
    count_inaccessible_dirs = count_all.\
                              where(paths.c.accessible == False).\
                              where(paths.c.pathtype == 'directory')

    with ctx.obj.engine.connect() as connection:

        click.echo('           Stored file paths: {}'.format(
            connection.execute(count_all).fetchone()[0]))

        click.echo('    Accessible, Not Uploaded: {}'.format(
            connection.execute(count_unprocessed).fetchone()[0]))

        click.echo('        Accessible, Uploaded: {}'.format(
            connection.execute(count_processed).fetchone()[0]))   

        click.echo('    Accessible, Upload Error: {}'.format(
            connection.execute(count_error).fetchone()[0]))

        click.echo('     Inaccessible file paths: {}'.format(
            connection.execute(count_inaccessible_files).fetchone()[0]))
         
        click.echo('Inaccessible directory paths: {}'.format(
            connection.execute(count_inaccessible_dirs).fetchone()[0]))

# ====================================
# Dump Subcommand
# ====================================

@cli.command(short_help='Dump stored paths.')
@click.pass_context
def dump(ctx):
    """Outputs the file and directory paths stored in the database."""    

    with ctx.obj.engine.connect() as connection:  
        count_all = sqlalchemy.select([sqlalchemy.func.count(paths.c.id)])
        if connection.execute(count_all).fetchone()[0] == 0:
            click.echo('No paths in database.')
            ctx.abort()

        click.echo(('Path, Type, Accessible ' 
                    'Status, Size in Bytes, Objectname'))
        for row in connection.execute(sqlalchemy.select([paths]).\
                                                  order_by(paths.c.path)):
            click.echo('{}, {}, {}, {}, {}, {}'.format(
                        row[paths.c.path], 
                        row[paths.c.pathtype], 
                        row[paths.c.accessible], 
                        row[paths.c.status], 
                        row[paths.c.bytes], 
                        row[paths.c.objectname]))

# ====================================
# Clear Subcommand
# ====================================

@cli.command(short_help='Delete all stored paths.')
@click.pass_context
def clear(ctx):
    """Clears the database of stored paths."""

    metadata.drop_all(ctx.obj.engine)

# ====================================
# Upload Subcommand
# ====================================

@cli.command(short_help='Upload all stored paths.')
@click.option('--username',
              help='Username, or use OS_USERNAME environment variable.',
              required=True, envvar='OS_USERNAME')
@click.option('--password',
              help='Password, or use OS_PASSWORD environment variable.',
              prompt=True, hide_input=True,
              required=True, envvar='OS_PASSWORD',)
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
def upload(ctx, username, password, tenant_name, 
           auth_url, auth_version, region_name,
           debug, info, segment_size, batch_size, container):
    """Upload all accessible paths to the given container."""

    count = sqlalchemy.select([sqlalchemy.func.count(paths.c.id)]).\
                       where(paths.c.status == 'unprocessed').\
                       where(paths.c.accessible == True) 

    with ctx.obj.engine.connect() as connection:    
        number_of_paths = connection.execute(count).fetchone()[0]
    width_number_paths = len(str(number_of_paths))

    if number_of_paths == 0:
        click.echo('No files are ready to upload.')
        ctx.abort()

    bytes_sum = sqlalchemy.select([sqlalchemy.func.sum(paths.c.bytes)]).\
                           where(paths.c.status == 'unprocessed').\
                           where(paths.c.accessible == True)       

    with ctx.obj.engine.connect() as connection:
        total_bytes = connection.execute(bytes_sum).fetchone()[0]
    width_total_bytes = len(str(total_bytes))

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

    # Make the output more pretty by justifying the headers.
    widest_possible_paths_progress = len("{0}/{0} 100.00%".format(
                                          number_of_paths))    
    widest_possible_bytes_progress = len("{0}/{0} 100.00%".format(
                                          total_bytes))
    paths_header = 'Number of Paths Processed'.ljust(
                              widest_possible_paths_progress)
    bytes_header = 'Number of Bytes Processed'.ljust(
                              widest_possible_bytes_progress)

    # Print the header
    click.echo('{} | {} | Current File Path'.format(
                paths_header, 
                bytes_header))

    # Keep track of our batched jobs
    Job = collections.namedtuple('job', ('path', 'process'))
    jobs = []

    # A closure that is run to collect the output of each job.
    def check_result(job):
        returncode = job.process.wait()

        if returncode != 0:           
            status_update = 'error'      
            echoerror('File Upload Error: {}.'.format(job.path))            
        else:
            status_update = 'processed'

        with ctx.obj.engine.begin() as transaction: 
            transaction.execute(paths.update().\
                                      values(status=status_update).\
                                      where(paths.c.path == job.path)) 

    # A select statement to retrieve one unprocessed path.
    path_to_upload = sqlalchemy.select([paths]).\
                                where(paths.c.status == 'unprocessed').\
                                where(paths.c.accessible == True).\
                                limit(batch_size) 

    # Keep track of our progress with these variables
    processed_paths = 0 
    processed_bytes = 0                             

    # Using a surrounding try to catch KeyboardInterrupt (ctrl^c)
    # during execution, loop forever while there are still paths 
    # left to process.
    try:
        while(True):

            with ctx.obj.engine.connect() as connection:
                rows = connection.execute(path_to_upload).fetchall()
            if len(rows) == 0: #out of rows
                break

            for row in rows:
    
                uploadcommand = copy.deepcopy(command)
                uploadcommand.append('--object-name={}'.format(
                                              row[paths.c.objectname]))
                uploadcommand.append(row[paths.c.path])
            
                if ctx.obj.verbose:
                    click.echo('\nRunning command:\n{}'.format(
                               ' '.join(uploadcommand)))
    
                # subprocess.Popen issues the command in a new process. 
                # start_new_session=True means that the subprocess
                # won't get the SIGINT signal when we press ctrl-c 
                # or equivalent, letting the last batch of subcommands
                # complete.   
                jobs.append(Job(row[paths.c.path], 
                                subprocess.Popen(uploadcommand, 
                                                 start_new_session=True)))
        
                processed_bytes += row[paths.c.bytes]
                processed_paths += 1 
        
                # Use the carriage return to move the cursor
                # back to the beginning of the line.
                if not ctx.obj.verbose:
                    click.echo('\r', nl=False)
    
                # Pretty print the progress indicators 
    
                j_processed_paths = str(processed_paths).rjust(width_number_paths)
                paths_percent = processed_paths/number_of_paths
                paths_progess = '{}/{} {:.2%}'.format(j_processed_paths, 
                                                      number_of_paths, 
                                                      paths_percent)
                j_paths_progess = paths_progess.rjust(len(paths_header))
    
                j_processed_bytes = str(processed_bytes).rjust(width_total_bytes)
                bytes_percent = processed_bytes/total_bytes
                bytes_progress = '{}/{} {:.2%}'.format(j_processed_bytes,
                                                       total_bytes, 
                                                       bytes_percent)
                j_bytes_progress = bytes_progress.rjust(len(bytes_header))
    
                click.echo('{} | {} | {}'.format(j_paths_progess,
                                                 j_bytes_progress,
                                                 row[paths.c.path]), nl=False) 
                 
            for job in jobs:
                check_result(job)
            jobs = []                                 
    
    except KeyboardInterrupt:
        try: 
            click.echo("\nWaiting for upload commands to complete... ", nl=False)
            for job in jobs:
                check_result(job) 
            click.echo("Done!")                
        except KeyboardInterrupt:
            click.echo("\nKilling outstanding upload commands... ", nl=False)
            for job in jobs:
                job.process.kill()  
            click.echo("Done!")          
            raise     
        raise   

    click.echo("\nDone!")

if __name__ == '__main__':
    cli()         
