import os
import sys
import shutil

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


if __name__ == "__main__":
	total = len(sys.argv)
	cmd_args = sys.argv

	if cmd_args[1] == "clean":
		if os.path.isdir('temp'):
	 		shutil.rmtree("temp")
		exit(0)

	# Do not execute if no directory provided. 
	if total != 4:
		print("Need 3 arguments")
		exit(0)

	filename = cmd_args[1]
	target_directory = cmd_args[2]
	size = cmd_args[3]

	print(split_file(filename, target_directory, size));
