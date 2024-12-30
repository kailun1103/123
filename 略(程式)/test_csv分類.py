import csv

input_file_path = '20241007_address_txn_statistics.csv'
output_file_path = '20241007_address_txn_statistics_cluster=-1,all-txn.csv'

count = 0
with open(input_file_path, 'r', newline='', encoding='cp1252') as input_file, \
     open(output_file_path, 'w', newline='', encoding='utf-8') as output_file:
    
    csv_reader = csv.reader(input_file)
    csv_writer = csv.writer(output_file)
    
    header = next(csv_reader)
    csv_writer.writerow(header)  # Write header to output file
    
    for row in csv_reader:
        if row[1] == "-1" and row[3] != "0" and row[4] != "0":
            csv_writer.writerow(row)
            print(row[0])
            count += 1

print(f"Total matching rows: {count}")
print(f"Filtered data saved to: {output_file_path}")