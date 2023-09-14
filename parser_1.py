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
        # while True:
            # file_lines = fp.readlines(65536)
            # if not file_lines:
            #     break
        file_lines = fp.readlines()

        line_number = 0
        total_lines = len(file_lines)

        butch_values = "";
        for line in file_lines:
            # sys.stdout.write('\r')
            # # the exact output you're looking for:
            # sys.stdout.write("[%-20s] %d%%" % ('='*int(20*percent), 100*percent))
            # sys.stdout.flush()

            line_number += 1
            percent = int((line_number / total_lines) * 100)
            print("{percent}%   {line_number} / {total_lines}".format(line_number = line_number, total_lines = total_lines, percent = percent))
            
            separated_line = line.split(" ");
            
            if (len(separated_line) < 14):
                continue
    
            date, time, ip, login, url, _, method, path, _, _, _, _, _, navigator = separated_line
            
            login = login.replace("\"", "") #.replace("\\", "\\\\")
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

# with open("./logs/web_request-2022-07-11.log") as fp:
#     file_name = os.path.basename(fp.name)

#     file_date = re.match(regex, file_name).group(1)


#     print(file_date)


    # for matchNum, match in enumerate(matches, start=1):
    #     print("Match {matchNum} was found at {start}-{end}: {match}".format(matchNum = matchNum, start = match.start(), end = match.end(), match = match.group()))
        
    #     for groupNum in range(0, len(match.groups())):
    #         groupNum = groupNum + 1
            
    #         print("Group {groupNum} found at {start}-{end}: {group}".format(groupNum = groupNum, start = match.start(groupNum), end = match.end(groupNum), group = match.group(groupNum)))

    # print(fileName)

    # Lines = fp.readlines()
    # for line in Lines:
    #     count += 1
    #     print("Line{}: {}".format(count, line.strip()))