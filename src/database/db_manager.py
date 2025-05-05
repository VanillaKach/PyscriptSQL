from typing import List, Dict, Optional
import psycopg2
from src.config import Config

class DBManager:
    """Класс для управления данными в PostgreSQL."""

    def __init__(self):
        self.conn = psycopg2.connect(
            host=Config.DB_HOST,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            database=Config.DB_NAME
        )

    def __del__(self):
        self.conn.close()

    def get_companies_and_vacancies_count(self) -> List[Dict]:
        """Получить список компаний и количество вакансий."""
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT c.name, COUNT(v.id) 
                FROM companies c 
                LEFT JOIN vacancies v ON c.id = v.company_id 
                GROUP BY c.name
            """)
            return [{"name": row[0], "count": row[1]} for row in cursor.fetchall()]

    def get_all_vacancies(self) -> List[Dict]:
        """Получить все вакансии с деталями."""
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT c.name, v.title, 
                       v.salary_from, v.salary_to, v.currency, v.url
                FROM vacancies v
                JOIN companies c ON v.company_id = c.id
            """)
            return [{
                "company": row[0],
                "title": row[1],
                "salary_from": row[2],
                "salary_to": row[3],
                "currency": row[4],
                "url": row[5]
            } for row in cursor.fetchall()]

    def get_avg_salary(self) -> float:
        """Получить среднюю зарплату."""
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT AVG((salary_from + salary_to)/2)
                FROM vacancies
                WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL
            """)
            return cursor.fetchone()[0] or 0

    def get_vacancies_with_higher_salary(self) -> List[Dict]:
        """Получить вакансии с зарплатой выше средней."""
        avg = self.get_avg_salary()
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT title, (salary_from + salary_to)/2 as salary, url
                FROM vacancies
                WHERE (salary_from + salary_to)/2 > %s
            """, (avg,))
            return [{"title": row[0], "salary": row[1], "url": row[2]} for row in cursor.fetchall()]

    def get_vacancies_with_keyword(self, keyword: str) -> List[Dict]:
        """Получить вакансии по ключевому слову."""
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT title, company_id, url 
                FROM vacancies 
                WHERE title ILIKE %s
            """, (f"%{keyword}%",))
            return [{"title": row[0], "company_id": row[1], "url": row[2]} for row in cursor.fetchall()]
