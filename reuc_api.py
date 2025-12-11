import requests
import api_key
import pandas as pd
from datetime import datetime
import openpyxl


class ApiClient:
    def __init__(self, base_url, api_key):
        self.base_url = base_url
        self.api_key = api_key


class ReucApiClient(ApiClient):
    def __init__(
        self,
        base_url: str = "https://citizen-cen-api.apps.prod-os-1.coordinador.cl/reuc/v1/coordinados",
        api_key: str = None,
    ):
        super().__init__(base_url, api_key)

    def fetch_json(self):
        response = requests.get(
            self.base_url, params={"user_key": self.api_key}, timeout=30, verify=False
        )
        response.raise_for_status()
        return response.json()

    def get_agents(self):
        agents_df = pd.json_normalize(self.fetch_json())
        return agents_df


if __name__ == "__main__":
    client = ReucApiClient(api_key=api_key.reuc_api_key)
    data = client.get_agents()

    data.to_excel(
        f"output/reuc_agents at {datetime.now().strftime('%Y-%m-%d %H.%M.%S')}.xlsx"
    )
