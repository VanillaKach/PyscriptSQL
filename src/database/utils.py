import psycopg2
from src.config import Config

def create_tables():
    """Создание таблиц в БД если они не существуют."""
    conn = psycopg2.connect(
        host=Config.DB_HOST,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD,
        database=Config.DB_NAME
    )
    cursor = conn.cursor()

    try:
        # Таблица companies
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                url VARCHAR(255),
                description TEXT
            )
        """)

        # Таблица vacancies
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vacancies (
                id INTEGER PRIMARY KEY,
                company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
                title VARCHAR(255) NOT NULL,
                salary_from INTEGER,
                salary_to INTEGER,
                currency VARCHAR(10),
                url VARCHAR(255)
            )
        """)
        conn.commit()
    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")
    finally:
        cursor.close()
        conn.close()
