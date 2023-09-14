import os
import re
import sys
from glob import glob
import psycopg2

db = psycopg2.connect(dbname='wt_sandbox', user='postgres', 
                        password='postgrespw', host='localhost', port=55000)
cursor = db.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS web_requests (
    date TIMESTAMP,
    ip VARCHAR(255),
    login VARCHAR(255),
    url VARCHAR(65535),
    method VARCHAR(255),
    path VARCHAR(65535),
    navigator VARCHAR(65535)
);
''')

log_files = glob("./logs/*.log")

butch_size = 1000;

for file_path in log_files:
    file_name = os.path.basename(file_path)
    file_date = re.match(r"web\_request\-(\d\d\d\d\-\d\d\-\d\d)\.log", file_name).group(1)

    with open(file_path) as fp:
        file_lines = fp.readlines()

        line_number = 0
        total_lines = len(file_lines)

        butch_values = "";
        for line in file_lines:
            line_number += 1
            percent = (line_number / total_lines)

            sys.stdout.write('\r')
            sys.stdout.write("[%-20s] %d%%" % ('='*int(20*percent), percent*100))
            sys.stdout.flush()
            
            separated_line = line.split(" ");
            
            if (len(separated_line) < 14):
                continue
    
            date, time, ip, login, url, _, method, path, _, _, _, _, _, navigator = separated_line
            
            login = login.replace("\"", "")
            navigator = navigator.replace("\"", "")[:-1]

            butch_values += '''(
                    TO_TIMESTAMP('{date} {time}', 'YYYY-MM-DD HH24:MI:SS'),
                    '{ip}',
                    '{login}',
                    '{url}',
                    '{method}',
                    '{path}',
                    '{navigator}'
                ),'''.format(date = date, time = time, ip = ip, login = login, url = url, method = method, path = path, navigator = navigator)

            if (line_number % butch_size == 0):
                cursor.execute('''
                    INSERT INTO web_requests (date, ip, login, url, method, path, navigator)
                    VALUES
                    {butch_values}'''.format(butch_values = butch_values)[:-1])
                db.commit()
                butch_values = ""

cursor.execute('''
                    INSERT INTO web_requests (date, ip, login, url, method, path, navigator)
                    VALUES
                    {butch_values}'''.format(butch_values = butch_values)[:-1])
db.commit()
butch_values = ""

cursor.close()
db.close()
