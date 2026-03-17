import mysql.connector
from mysql.connector import Error
import csv


class DatabaseManager:
    def __init__(self, host, user, password, database, port=3306):
        self.config = {
            "host": "srv221-h-st.jino.ru",
            "user": "j30084097_13418",
            "password": "pPS090207/()",
            "database": "j30084097_13418",
            "port": 3306
        }
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.config)
            if self.connection.is_connected():
                print("Подключение к базе данных успешно")
        except Error as e:
            print("Ошибка подключения:", e)

    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Соединение закрыто")

    def execute(self, query, params=None, fetch=False):
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query, params or ())
            if fetch:
                result = cursor.fetchall()
                return result
            self.connection.commit()
        except Error as e:
            print("Ошибка запроса:", e)
        finally:
            cursor.close()

    def create(self, table, data: dict):
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        self.execute(query, tuple(data.values()))
        print("Запись добавлена")

    def read(self, table, conditions=None):
        query = f"SELECT * FROM {table}"
        params = None

        if conditions:
            where_clause = " AND ".join([f"{k}=%s" for k in conditions.keys()])
            query += f" WHERE {where_clause}"
            params = tuple(conditions.values())

        result = self.execute(query, params, fetch=True)
        return result

    def update(self, table, data: dict, conditions: dict):
        set_clause = ", ".join([f"{k}=%s" for k in data.keys()])
        where_clause = " AND ".join([f"{k}=%s" for k in conditions.keys()])

        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        params = tuple(data.values()) + tuple(conditions.values())

        self.execute(query, params)
        print("Запись обновлена")

    def delete(self, table, conditions: dict):
        where_clause = " AND ".join([f"{k}=%s" for k in conditions.keys()])
        query = f"DELETE FROM {table} WHERE {where_clause}"

        self.execute(query, tuple(conditions.values()))
        print("Запись удалена")

    def create_users_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.execute(query)
        print("Таблица users готова")

    #вывод конкретного столбца в порядке убывания или возрастания
    def get_column_sorted(self, table, column, order="ASC"):
        query = f"SELECT {column} FROM {table} ORDER BY {column} {order}"
        return self.execute(query, fetch=True)

    #вывод диапазона строк по айди
    def get_rows_by_id_range(self, table, start_id, end_id):
        query = f"SELECT * FROM {table} WHERE id BETWEEN %s AND %s"
        return self.execute(query, (start_id, end_id), fetch=True)

    #удаление диапазона строк по айди
    def delete_rows_by_id_range(self, table, start_id, end_id):
        query = f"DELETE FROM {table} WHERE id BETWEEN %s AND %s"
        self.execute(query, (start_id, end_id))
        print(f"Удалены строки с id от {start_id} до {end_id}")

    #вывод структуры таблицы
    def get_table_structure(self, table):
        query = f"DESCRIBE {table}"
        return self.execute(query, fetch=True)

    # вывод строк содержащих конкретное значение в конкретном столбце
    def find_by_value(self, table, column, value):
        query = f"SELECT * FROM {table} WHERE {column} = %s"
        return self.execute(query, (value,), fetch=True)

    #удаление таблицы
    def drop_table(self, table):
        query = f"DROP TABLE IF EXISTS {table}"
        self.execute(query)
        print(f"Таблица {table} удалена")

    #добавление нового столбца
    def add_column(self, table, column_name, column_type):
        query = f"ALTER TABLE {table} ADD COLUMN {column_name} {column_type}"
        self.execute(query)
        print(f"Столбец {column_name} добавлен")

    #удаление столбца
    def drop_column(self, table, column_name):
        query = f"ALTER TABLE {table} DROP COLUMN {column_name}"
        self.execute(query)
        print(f"Столбец {column_name} удален")

    # экспорт таблицы в csv
    def export_to_csv(self, table, filename):
        data = self.read(table)
        if not data:
            print("Таблица пуста")
            return
        
        with open(filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        print(f"Экспортировано в {filename}")

    # импорт таблицы из csv
    def import_from_csv(self, table, filename):
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                self.create(table, row)
        print(f"Импортировано из {filename}")


if __name__ == "__main__":

    db = DatabaseManager(
        host="srv221-h-st.jino.ru",      
        user="j30084097_13418",            
        password="pPS090207/()",    
        database="j30084097_13418"    
    )

    db.connect()
    db.create_users_table()


    print("Вывод столбца name по возрастанию:")
    print(db.get_column_sorted("users", "name", "ASC"))

    print("Вывод строк с id от 1 до 3:")
    print(db.get_rows_by_id_range("users", 1, 3))

    print("Структура таблицы:")
    print(db.get_table_structure("users"))

    print("Поиск пользователя с email primer@eye.com:")
    print(db.find_by_value("users", "email", "primer@eye.com"))

    print("Добавление столбца phone:")
    db.add_column("users", "phone", "VARCHAR(20)")

    print("Экспорт в CSV:")
    db.export_to_csv("users", "users.csv")

    db.close()