
                CREATE OR REPLACE TEMP TABLE temp_broker AS 
                SELECT * FROM read_csv('src/data/Batch1\HR.csv', delim=',', columns={
                    'employeeid': 'BIGINT',
                    'managerid': 'BIGINT',
                    'employeefirstname': 'STRING',
                    'employeelastname': 'STRING',
                    'employeemi': 'STRING',
                    'employeejobcode': 'STRING',
                    'employeebranch': 'STRING',
                    'employeeoffice': 'STRING',
                    'employeephone': 'STRING'
                }, header=False) 
                WHERE employeejobcode = '314';
            