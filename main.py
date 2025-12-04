import requests
import pandas as pd

# 1. Fetch JSON from API
agents_url = "https://api-infotecnica.coordinador.cl/v1/grupos"
plants_url = "https://api-infotecnica.coordinador.cl/v1/centrales/"
units_url = "http://api-infotecnica.coordinador.cl/v1/unidades-generadoras/"


agents_resp = requests.get(agents_url)
plants_resp = requests.get(plants_url)
units_resp = requests.get(units_url)

agents_resp.raise_for_status()
plants_resp.raise_for_status()
units_resp.raise_for_status()

agents_json = agents_resp.json()
plants_json = plants_resp.json()
units_json = units_resp.json()

# 2. Convert to DataFrame
agents_df = pd.json_normalize(agents_json)
plants_df = pd.json_normalize(plants_json)
units_df = pd.json_normalize(units_json)

agents_df.rename(columns={"id": "AgentID"}, inplace=True)
agents_df["reuc_id"] = agents_df["descripcion"].str.split("_").str[-1]

plants_df = plants_df.merge(
    agents_df[["AgentID", "reuc_id"]],
    how="left",
    left_on="id_coordinado",
    right_on="AgentID",
)
plants_df.drop(columns=["AgentID"], inplace=True)

# Filter PMGD
distr_plants_df = plants_df[
    plants_df["nombre"].str.contains("PMGD ", case=False, na=False)
]

# 3. Write to Excel
output_file = "output.xlsx"

with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    agents_df.to_excel(writer, sheet_name="MarketAgents", index=False)
    plants_df.to_excel(writer, sheet_name="PowerPlants", index=False)
    units_df.to_excel(writer, sheet_name="GenUnits", index=False)
    distr_plants_df.to_excel(writer, sheet_name="DistributedPlants", index=False)
