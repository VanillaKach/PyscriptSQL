import psycopg2
import csv
from datetime import datetime

# Чтение данных из CSV файлов
with open('customers_data.csv', newline='') as file:
    customers_data = [row for row in csv.reader(file) if 'customer_id' not in row]

with open('employees_data.csv', newline='') as file:
    employees_data = [row for row in csv.reader(file) if 'first_name' not in row]

with open('orders_data.csv', newline='') as file:
    orders_data = [row for row in csv.reader(file) if 'order_id' not in row]

# Подключение к базе данных
conn = psycopg2.connect(
    host="sql_db",
    port=5432,
    database="analysis",
    user="simple",
    password="qweasd963"
)

# Создание курсора
cur = conn.cursor()

# Создание схемы и удаление таблиц
cur.execute("create schema if not exists itresume14953;")
cur.execute("DROP TABLE IF EXISTS itresume14953.orders")
cur.execute("DROP TABLE IF EXISTS itresume14953.customers")
cur.execute("DROP TABLE IF EXISTS itresume14953.employees")

# Создание таблиц
cur.execute("""
    CREATE TABLE itresume14953.customers (
        customer_id CHAR(5) NOT NULL,
        company_name VARCHAR(100) NOT NULL,
        contact_name VARCHAR(100) NOT NULL,
        PRIMARY KEY (customer_id)
    )
""")

cur.execute("""
    CREATE TABLE itresume14953.employees (
        employee_id INTEGER NOT NULL,
        first_name VARCHAR(25) NOT NULL,
        last_name VARCHAR(35) NOT NULL,
        title VARCHAR(100) NOT NULL,
        birth_date DATE NOT NULL,
        notes TEXT,
        PRIMARY KEY (employee_id)
    )
""")

cur.execute("""
    CREATE TABLE itresume14953.orders (
        order_id INTEGER NOT NULL,
        customer_id CHAR(5) NOT NULL,
        employee_id INTEGER NOT NULL,
        order_date DATE NOT NULL,
        ship_city VARCHAR(100) NOT NULL,
        PRIMARY KEY (order_id),
        FOREIGN KEY (customer_id) REFERENCES itresume14953.customers(customer_id),
        FOREIGN KEY (employee_id) REFERENCES itresume14953.employees(employee_id)
    )
""")

# Фиксация изменений
conn.commit()

# Вставка данных в таблицу customers
for row in customers_data:
    if len(row) >= 3:
        cur.execute("""
            INSERT INTO itresume14953.customers (customer_id, company_name, contact_name)
            VALUES (%s, %s, %s) returning *
        """, row[:3])

conn.commit()
res_customers = cur.fetchall()

# Вставка данных в таблицу employees
for i, row in enumerate(employees_data, 1):
    if len(row) >= 5:
        try:
            employee_id = i
            first_name = row[0]
            last_name = row[1]
            title = row[2]
            birth_date = datetime.strptime(row[3], '%Y-%m-%d').date()
            notes = row[4] if len(row) > 4 else None

            cur.execute("""
                INSERT INTO itresume14953.employees 
                (employee_id, first_name, last_name, title, birth_date, notes)
                VALUES (%s, %s, %s, %s, %s, %s) returning *
            """, (employee_id, first_name, last_name, title, birth_date, notes))
        except:
            continue

conn.commit()
res_employees = cur.fetchall()

# Получаем списки существующих ID
cur.execute("SELECT customer_id FROM itresume14953.customers")
existing_customers = {row[0] for row in cur.fetchall()}

cur.execute("SELECT employee_id FROM itresume14953.employees")
existing_employees = {row[0] for row in cur.fetchall()}

# Вставка данных в таблицу orders
for row in orders_data:
    if len(row) >= 5:
        try:
            order_id = int(row[0])
            customer_id = row[1]
            employee_id = int(row[2])

            if customer_id in existing_customers and employee_id in existing_employees:
                order_date = datetime.strptime(row[3], '%Y-%m-%d').date()
                ship_city = row[4]

                cur.execute("""
                    INSERT INTO itresume14953.orders 
                    (order_id, customer_id, employee_id, order_date, ship_city)
                    VALUES (%s, %s, %s, %s, %s) returning *
                """, (order_id, customer_id, employee_id, order_date, ship_city))
        except:
            continue

conn.commit()
res_orders = cur.fetchall()

# Закрытие соединения
cur.close()
conn.close()
