import os
import csv

with open("notev4.csv", 'w+', newline='', encoding='utf-8') as f:
    csv_writer = csv.writer(f)
    dir=r"D:\bsc\code\bsc"
    for parent, dirnames, filenames in os.walk(dir):
        for filename in filenames:
            csv_writer.writerow([filename, parent])
            file_path = os.path.join(parent, filename)
            desc_str=''
            with open(file_path, 'r',encoding='utf-8',errors='ignore') as child_file:
                for i in range(7):
                    desc_str += child_file.readline()
            csv_writer.writerow([desc_str])
