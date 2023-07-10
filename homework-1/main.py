import psycopg2
from config_north import USER, PASSWORD, HOST, DB_NAME
import csv

try:
    connection = psycopg2.connect(host=HOST,
                                  database=DB_NAME,
                                  user=USER,
                                  password=PASSWORD)
    print('[INFO] Соединение с базой данных установлено.')
    with connection:
        with connection.cursor() as cursor:
            with open('north_data/customers_data.csv', 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    cursor.execute('INSERT INTO customers VALUES(%s, %s, %s)', (row[0], row[1], row[2]))
            with open('north_data/employees_data.csv', 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    cursor.execute('INSERT INTO employees VALUES(%s, %s, %s, %s, %s, %s)',
                                   (row[0], row[1], row[2], row[3], row[4], row[5]))
            with open('north_data/orders_data.csv', 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    cursor.execute('INSERT INTO orders VALUES (%s, %s, %s, %s, %s)',
                                   (row[0], row[1], row[2], row[3], row[4]))

except (Exception, psycopg2.DatabaseError) as error:
    print(f'[INFO] {error}')

finally:
    if connection:
        connection.close()
        print('[INFO] Соединение с базой данных закрыто.')
