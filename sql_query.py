from clickhouse_driver import Client


client = Client('localhost', port=9000, user='airflow', password='airflow')
query="""
SELECT area
FROM airflow.cell_towers
WHERE mcc = 250
AND radio != 'LTE'
GROUP BY area
HAVING COUNT(DISTINCT cell) > 2000;
"""
result = client.execute(query)
print(result)
client.disconnect()