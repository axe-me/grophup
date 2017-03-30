import pymssql

conn = pymssql.connect(
    server='SX-DEV',
    database="GroupData1_Data"
)

cursor = conn.cursor()

cursor.execute('SELECT TOP 1000 * FROM Group10')

row = cursor.fetchone()

while row:
    print(row)
    row = cursor.fetchone()

conn.close()
