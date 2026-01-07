import requests
import pandas as pd
from dataclasses import dataclass


@dataclass
class Substation:
    id: int
    id_propietario: int
    propietario_nombre: str
    numero: int
    codigo: str
    nemotecnico: str
    nombre: str
    descripcion: str
    flag_equipocom: bool
    flag_scada: bool
    flag_pararrayos: bool
    id_coordinado: int
    coordinado_nombre: str
    id_centro_control: int
    centro_control_nombre: str


@dataclass
class GeneratingUnit:
    id: int
    id_central: int
    central_nombre: str
    id_propietario: int
    propietario_nombre: str
    id_tipo_tecnologia: int
    tipo_tecnologia_nombre: str
    id_conexion1: int
    id_conexion2: int
    id_turbina_tipo: int
    id_turbina_marca: int
    id_combustible: int
    numero: int
    codigo: str
    nemotecnico: str
    nombre: str
    descripcion: str


class InfoFetcher:
    def __init__(self):
        self.url_substations = "http://api-infotecnica.coordinador.cl/v1/subestaciones/"
        self.url_bays = "http://api-infotecnica.coordinador.cl/v1/panos/"
        self.url_market_agents = "https://api-infotecnica.coordinador.cl/v1/grupos"
        self.url_power_plants = "https://api-infotecnica.coordinador.cl/v1/centrales/"
        self.url_generating_units = (
            "http://api-infotecnica.coordinador.cl/v1/unidades-generadoras/"
        )

    def fetch_json(self, url, verify: bool = False):
        resp = requests.get(url, verify=verify, timeout=30)  # 👈 OJO: verify=False
        resp.raise_for_status()
        return resp.json()

    def fetch_substations(self):
        return pd.json_normalize(self.fetch_json(self.url_substations))

    def fetch_bays(self):
        return pd.json_normalize(self.fetch_json(self.url_bays))

    def fetch_agents(self):
        return pd.json_normalize(self.fetch_json(self.url_market_agents))

    def fetch_power_plants(self):
        return pd.json_normalize(self.fetch_json(self.url_power_plants))

    def fetch_generating_units(self):
        return pd.json_normalize(self.fetch_json(self.url_generating_units))


if __name__ == "__main__":
    fetcher = InfoFetcher()

    substations_df = fetcher.fetch_substations()
    substations_df.to_excel("temp/substations.xlsx", index=False)

    bays_df = fetcher.fetch_bays()
    bays_df.to_excel("temp/bays.xlsx", index=False)

    units_df = fetcher.fetch_generating_units()
    units_df.to_excel("temp/generating_units.xlsx", index=False)
