import pymssql

conn = pymssql.connect(server = '(local)', database = "GroupData1_Data")
cursor = conn.cursor()

cursor.execute('SELECT TOP 1000 * FROM Group10')
row = cursor.fetchone()
while row:
    print(row)
    print("ID=%d, Name=%s" % (row[0], row[1]))
    row = cursor.fetchone()

conn.close()
