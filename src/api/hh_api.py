import requests
from typing import Dict, List, Optional
from src.config import Config

class HHAPI:
    """Класс для работы с API HeadHunter."""

    @staticmethod
    def get_employers(employer_ids: List[str]) -> List[Dict]:
        """Получить данные о компаниях по их ID."""
        employers = []
        for emp_id in employer_ids:
            url = f"{Config.HH_API_URL}/employers/{emp_id}"
            response = requests.get(url)
            if response.status_code == 200:
                employers.append(response.json())
        return employers

    @staticmethod
    def get_vacancies(employer_id: str) -> List[Dict]:
        """Получить вакансии компании по её ID."""
        url = f"{Config.HH_API_URL}/vacancies"
        params = {
            "employer_id": employer_id,
            "per_page": 100  # Максимальное количество на странице
        }
        response = requests.get(url, params=params)
        return response.json().get("items", []) if response.status_code == 200 else []
