import psycopg2

# Функція для створення таблиці user_counter
def create_table():
    with psycopg2.connect(dbname="lab1", user="postgres", password="Rthj76bn", host="localhost", port="5432") as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_counter (
                    user_id SERIAL PRIMARY KEY,
                    counter INTEGER NOT NULL DEFAULT 0,
                    version INTEGER NOT NULL DEFAULT 0
                );
            """)
            conn.commit()

# Функція для наповнення таблиці user_counter 100 000 рядками
def fill_table():
    with psycopg2.connect(dbname="lab1", user="postgres", password="Rthj76bn", host="localhost", port="5432") as conn:
        with conn.cursor() as cursor:
            for i in range(100000):
                cursor.execute("INSERT INTO user_counter (counter, version) VALUES (%s, %s)", (0, 0))
                conn.commit()

# Виклик функцій для створення та наповнення таблиці
create_table()
fill_table()
