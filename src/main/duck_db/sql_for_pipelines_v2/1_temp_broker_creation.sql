CREATE OR REPLACE TEMP TABLE temp_broker AS
        SELECT * FROM read_csv('C:/lopu-kg-test/project/src/data/Batch1/HR.csv', delim=',', columns={
            'employeeid': 'BIGINT',
            'managerid': 'BIGINT',
            'employeefirstname': 'STRING',
            'employeelastname': 'STRING',
            'employeemi': 'STRING',
            'employeejobcode': 'STRING',
            'employeebranch': 'STRING',
            'employeeoffice': 'STRING',
            'employeephone': 'STRING'
        }, header=False, auto_detect=false) -- Set auto_detect=false when specifying columns
        WHERE employeejobcode = '314';