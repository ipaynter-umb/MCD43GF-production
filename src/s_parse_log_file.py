import json
from pathlib import Path


file_path = Path(r"F:\UMB\20220907105725.log")
file_log = open(file_path, 'r')

link_fails = 0
failed_links = []

while True:

    line = file_log.readline()

    if not line:
        break

    # Split the line
    split_line = line.split('-')

    # Get the level
    level = split_line[0].strip()

    # If this is warning
    if level == "WARNING":
        if "Could not create link" in split_line[4]:
            link_fails += 1

            file_name = split_line[4].split(' ')[6].strip()[:-1]
            if file_name not in failed_links:
                failed_links.append(file_name)
            else:
                print(f"Duplicate of {file_name} found.")



file_path = Path(r"F:\UMB\20220904214500.log")
file_log = open(file_path, 'r')

checksum_mismatches = 0
failures = 0

overwrites = 0

failed_files = []

line_count = 0

files_in_common = 0


while True:

    line = file_log.readline()

    if not line:
        break

    line_count += 1

    # Split the line
    split_line = line.split('-')

    # Get the level
    level = split_line[0].strip()

    # If this is warning
    if level == "WARNING":

        if "Checksum did not match validation" in split_line[4]:
            checksum_mismatches += 1
        elif "Already a file at" in split_line[4]:
            overwrites += 1


    elif level == "ERROR":
        #if "A valid request could not be made for" in split_line[4]:


         #   file_name = split_line[4].split(' ')[9].strip()[:-1].split('/')[-1]

          #  if file_name not in failed_files:

           #     failures += 1

            #    failed_files.append(file_name)

             #   if file_name in failed_links:
              #      files_in_common += 1
        if "failed after 3" in split_line[4]:

            file_name = split_line[4].split(' ')[4].strip().split('/')[-1]

            if file_name not in failed_files:
                failures += 1

                failed_files.append(file_name)

                if file_name in failed_links:
                    files_in_common += 1



print(f"Total of {link_fails} failed symlink creations of {len(failed_links)} unique files.")
#print(f'Total of {line_count} lines parsed from log file.')
print(f'Total of {checksum_mismatches} checksum mismatches.')
print(f'Total of {len(failed_files)} failed after maximum attempts.')
print(f'Total of {overwrites} files overwritten.')
print(f"Total of {files_in_common} files in common between the failed lists.")

print("______________")
for link in failed_links:
    print(link)

print("_______________")
for file in failed_files:
    print(file)