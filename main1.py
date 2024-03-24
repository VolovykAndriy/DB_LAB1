import psycopg2
from concurrent.futures import ThreadPoolExecutor
import time
import random

def reset_counter():
    with psycopg2.connect(dbname="lab1", user="postgres", password="Rthj76bn", host="localhost", port="5432") as conn:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE user_counter SET counter = 0 WHERE user_id = 1")
            conn.commit()

def update_counter_lost(conn):
    cursor = conn.cursor()
    for i in range(10000):
        cursor.execute("SELECT counter FROM user_counter WHERE user_id = %s", (random.randint(1, 100000),))
        counter = cursor.fetchone()[0]
        counter += 1
        cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, random.randint(1, 100000)))
        conn.commit()

def update_counter_in_place(conn):
    cursor = conn.cursor()
    for i in range(10000):
        cursor.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = %s", (random.randint(1, 100000),))
        conn.commit()

def update_counter_row_locking(conn):
    with conn.cursor() as cursor:
        for i in range(10000):
            cursor.execute("SET statement_timeout = 1000")
            cursor.execute("SELECT counter FROM user_counter WHERE user_id = %s FOR UPDATE", (random.randint(1, 100000),))
            counter = cursor.fetchone()[0]
            counter += 1
            cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, random.randint(1, 100000)))
            conn.commit()

def update_counter_optimistic(conn):
    cursor = conn.cursor()
    for i in range(10000):
        while True:
            cursor.execute("SELECT counter, version FROM user_counter WHERE user_id = %s", (random.randint(1, 100000),))
            counter, version = cursor.fetchone()
            counter += 1
            cursor.execute("UPDATE user_counter SET counter = %s, version = %s WHERE user_id = %s AND version = %s",
                           (counter, version + 1, random.randint(1, 100000), version))
            conn.commit()
            count = cursor.rowcount
            if count > 0:
                break

def time_update_counter(update_func):
    with psycopg2.connect(dbname="lab1", user="postgres", password="Rthj76bn", host="localhost", port="5432") as conn:
        with ThreadPoolExecutor() as executor:
            start_time = time.time()
            for _ in range(10):
                executor.submit(update_func, conn)
            executor.shutdown(wait=True)
            end_time = time.time()
            print(f"Time taken: {end_time - start_time:.3f} seconds")
            with conn.cursor() as cursor:
                cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
                result = cursor.fetchone()[0]
                print(f"Result: {result}")

def time_update_counter_row_locking():
    with ThreadPoolExecutor() as executor:
        start_time = time.time()
        for _ in range(10):
            with psycopg2.connect(dbname="lab1", user="postgres", password="Rthj76bn", host="localhost", port="5432") as conn:
                executor.submit(update_counter_row_locking, conn)
        executor.shutdown(wait=True)
        end_time = time.time()
        print(f"Time taken: {end_time - start_time:.3f} seconds")
        with psycopg2.connect(dbname="lab1", user="postgres", password="Rthj76bn", host="localhost", port="5432") as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
                result = cursor.fetchone()[0]
                print(f"Result: {result}")

time_update_counter_row_locking()