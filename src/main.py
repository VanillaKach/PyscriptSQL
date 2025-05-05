from src.api.hh_api import HHAPI
from src.database.db_manager import DBManager
from src.database.utils import create_tables
from typing import List
import time


def load_data_to_db(employer_ids: List[str]):
    """Загрузка данных в БД."""
    hh = HHAPI()
    db = DBManager()

    for emp_id in employer_ids:
        try:
            # Получаем данные о компании
            emp_data = hh.get_employers([emp_id])
            if not emp_data:
                continue
            emp = emp_data[0]

            # Получаем вакансии компании
            vacancies = hh.get_vacancies(emp_id)

            # Сохраняем компанию
            with db.conn.cursor() as cursor:
                try:
                    cursor.execute(
                        """INSERT INTO companies (id, name, url, description)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING""",
                        (emp["id"], emp["name"], emp.get("site_url"), emp.get("description"))
                    )

                    # Сохраняем вакансии
                    for vac in vacancies:
                        salary = vac.get("salary")
                        cursor.execute(
                            """INSERT INTO vacancies 
                            (id, company_id, title, salary_from, salary_to, currency, url) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id) DO NOTHING""",
                            (
                                vac["id"],
                                emp["id"],
                                vac["name"],
                                salary["from"] if salary else None,
                                salary["to"] if salary else None,
                                salary["currency"] if salary else None,
                                vac["alternate_url"]
                            )
                        )
                    db.conn.commit()
                except Exception as e:
                    print(f"Ошибка при сохранении данных для компании {emp['name']}: {e}")
                    db.conn.rollback()

            # Задержка чтобы не превысить лимиты API
            time.sleep(0.5)

        except Exception as e:
            print(f"Ошибка при обработке компании {emp_id}: {e}")
