from pathlib import Path
from os.path import exists
from os import mkdir

plist = [3, 1, 2]

plist = plist[0]

print(plist)
print(type(plist))

test = False

if not test:
    print("False!")

file_path = Path(r"F:\UMB\path_test\support\6_MCD43D31_files_09022022.json")

check_path = Path()

for part in file_path.parts[0:-1]:
    check_path = Path(str(check_path), part)
    if exists(check_path):
        print(f"{str(check_path)} exists.")
    else:
        print(f"Creating {str(check_path)}")
        mkdir(check_path)


