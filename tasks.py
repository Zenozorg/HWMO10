import psycopg2
from psycopg2 import sql
#task1
conn = psycopg2.connect(
    dbname="postgres_db", 
    user="postgres", 
    password="postgres",     
    host="localhost",             
    port="5432"                  
)
cur = conn.cursor()

cur.execute('''
    CREATE TABLE IF NOT EXISTS employees (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        position VARCHAR(50),
        salary NUMERIC
    )
''')
conn.commit() 

# 2. Вставка данных (Create)
try:
    cur.execute("BEGIN")
    cur.execute('''
        INSERT INTO employees (name, position, salary)
        VALUES 
        ('John Doe', 'Developer', 70000),
        ('Jane Smith', 'Manager', 85000),
        ('Emily Davis', 'Designer', 65000)
    ''')
    conn.commit()  # Подтверждаем вставку данных
except Exception as e:
    conn.rollback()
    print(f"Ошибка при вставке данных: {e}")

# 3. Чтение всех строк (Read all)
cur.execute("SELECT * FROM employees")
employees = cur.fetchall()
print("Все сотрудники:", employees)

# 4. Чтение одной строки по id (Read one)
cur.execute("SELECT * FROM employees WHERE id = %s", (1,))
employee = cur.fetchone()
print("Сотрудник с id=1:", employee)

# 5. Обновление записи (Update)
try:
    cur.execute("BEGIN")
    cur.execute('''
        UPDATE employees
        SET salary = %s
        WHERE id = %s
    ''', (75000, 1))
    conn.commit()  # Подтверждаем обновление данных
except Exception as e:
    conn.rollback()
    print(f"Ошибка при обновлении данных: {e}")

# 6. Чтение обновлённой записи
cur.execute("SELECT * FROM employees WHERE id = %s", (1,))
updated_employee = cur.fetchone()
print("Обновленный сотрудник с id=1:", updated_employee)

# 7. Удаление записи (Delete)
try:
    cur.execute("BEGIN")
    cur.execute("DELETE FROM employees WHERE id = %s", (2,))
    conn.commit()  # Подтверждаем удаление данных
except Exception as e:
    conn.rollback()
    print(f"Ошибка при удалении данных: {e}")

# 8. Чтение всех строк после удаления
cur.execute("SELECT * FROM employees")
remaining_employees = cur.fetchall()
print("Оставшиеся сотрудники:", remaining_employees)

# 9. Пример использования ROLLBACK
try:
    cur.execute("BEGIN")
    cur.execute('''
        UPDATE employees
        SET salary = %s
        WHERE id = %s
    ''', (100000, 3))
    
    # Откат изменений
    conn.rollback()
    print("Изменения откатились")
except Exception as e:
    print(f"Ошибка при обновлении данных: {e}")

# Проверка, что изменения не были применены
cur.execute("SELECT * FROM employees WHERE id = %s", (3,))
rollback_employee = cur.fetchone()
print("Сотрудник с id=3 после ROLLBACK:", rollback_employee)

# Закрываем курсор и соединение
cur.close()
conn.close()
