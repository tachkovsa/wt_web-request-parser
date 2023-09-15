import os
import re
import sys
from glob import glob
import psycopg2

db = psycopg2.connect(dbname='wt_requests', user='postgres_admin',
                      password='postgres_admin', host='localhost', port=5432)
cursor = db.cursor()


def create_table():
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS web_requests (
        date TIMESTAMP,
        ip VARCHAR(255),
        login VARCHAR(255),
        url VARCHAR(65535),
        method VARCHAR(255),
        path VARCHAR(65535),
        mode VARCHAR(65535),
        object_id BIGINT,
        navigator VARCHAR(65535)
    );
    ''')


def past_to_db(butch):
    cursor.execute('''
        INSERT INTO web_requests (date, ip, login, url, method, path, mode, object_id, navigator)
        VALUES
        {butch}'''.format(butch=butch)[:-1])
    db.commit()


create_table()

log_files = glob("./logs/*.log")

butch_size = 1000

for file_path in log_files:
    file_name = os.path.basename(file_path)
    file_date = re.match(r"web_request-(\d\d\d\d-\d\d-\d\d)\.log", file_name).group(1)

    with open(file_path) as fp:
        file_lines = fp.readlines()

        line_number = 0
        total_lines = len(file_lines)

        butch_count = 0
        butch_values = ""

        for line in file_lines:
            line_number += 1
            percent = (line_number / total_lines)

            sys.stdout.write('\r')
            sys.stdout.write("[%-20s] %d%%" % ('=' * int(20 * percent), percent * 100))
            sys.stdout.flush()

            separated_line = line.split(" ")

            if len(separated_line) < 14:
                continue

            date, time, ip, login, url, _, method, path, _, _, _, _, _, navigator = separated_line

            path_substrings = ["custom_web_template", "view_doc", "_wt"]

            has_substring = False

            for substring in path_substrings:
                if re.search(substring, path):
                    has_substring = True

            if (not has_substring):
                continue

            login = login.replace("\"", "")
            navigator = navigator.replace("\"", "")[:-1]

            regex = r"mode=([a-z_\d]*)|\/_wt\/([a-z_\d]*)|(object_id|course_id|assessment_id)=(\d*)"

            match = re.findall(regex, path)
            if (match is None or not match):
                continue

            first_match = match[0]
            if (first_match is None or not first_match):
                continue

            # group 1 - mode
            # group 2 - mode or id
            # group 4 - id

            mode = first_match[0]
            object_id = first_match[3]

            try:
                if not object_id:
                    object_id = int(first_match[1])
            except:
                if not mode:
                    mode = first_match[1]

            try:
                object_id = int(object_id)
            except:
                object_id = "NULL"

            # print(mode)
            # print(object_id)

            butch_values += '''(
                    TO_TIMESTAMP('{date} {time}', 'YYYY-MM-DD HH24:MI:SS'),
                    '{ip}',
                    '{login}',
                    '{url}',
                    '{method}',
                    '{path}',
                    '{mode}',
                    {object_id},
                    '{navigator}'
                ),'''.format(date=date, time=time, ip=ip, login=login, url=url, method=method, path=path, mode=mode,
                             object_id=object_id, navigator=navigator)
            butch_count += 1

            if butch_count % butch_size == 0:
                past_to_db(butch_values)
                butch_count = 0
                butch_values = ""

past_to_db(butch_values)
butch_count = 0
butch_values = ""

cursor.close()
db.close()
