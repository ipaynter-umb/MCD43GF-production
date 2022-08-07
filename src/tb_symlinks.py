from os import symlink, environ
from os.path import exists
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

input_path = Path(environ['input_files_path'])

file_path = Path(environ['output_files_path'], 'test.txt')

# Create a symbolic link
symlink(file_path, Path(input_path / 'test.txt'))