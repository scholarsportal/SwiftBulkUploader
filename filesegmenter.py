import os

def split_file(file_path, directory, size):
	'''Given a path to a file and create size byte partitions of the file. Return a list of 
	file paths to these partitions. Partitions are stored in the directory directory.

	Create the directory if it does not exist.'''

	fileNumber = 0
	files_created = [];

	# Check that the directory exists by attempting to create it. Creation successful when it doens't exist.
	try:
		os.makedirs(directory)
	except OSError:
		if not os.path.isdir(directory):
			raise

	file_name = file_path.split('/')[-1]
	with open(file_path, "rt") as f:
		while True:
			buf = f.read(int(size))
			if not buf:
				 # we've read the entire file in, so we're done.
				 break

			create_file = os.path.join(directory, "{}-{}.txt".format(file_name,fileNumber))
			outFile = open(create_file, "wt")
			outFile.write(buf)
			outFile.close()
			files_created.append(create_file)
			fileNumber += 1 
	return files_created

def create_file_path(directory, file_path):
	'''Given a directory to store a file in and a file_path, create the requried
	directories within directory to mimic file_path. Return the path created.'''

	try:
		os.makedirs(directory)
	except OSError:
		if not os.path.isdir(directory):
			raise

	# End case, when file_path is just a file within directory
	directories = file_path.split('/', 1);
	if os.path.isfile(file_path) or len(directories) <= 1:
		return ''

	# Make directory
	create_directory = directories[0]
	os.makedirs(create_directory)
	# Create subdirectories
	return os.path.join(create_directory,
	    create_file_path(create_directory, directories[1]))
