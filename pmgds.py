import requests
import pandas as pd


class PMGDSDataFetcher:
    def __init__(self):
        self.url_agents = "https://api-infotecnica.coordinador.cl/v1/grupos"
        self.url_plants = "https://api-infotecnica.coordinador.cl/v1/centrales/"
        self.url_units = (
            "http://api-infotecnica.coordinador.cl/v1/unidades-generadoras/"
        )

    def fetch_json(self, url):
        """Download JSON with error handling."""
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.json()

    def fetch_all(self):
        """Download and load all datasets."""
        agents_df = pd.json_normalize(self.fetch_json(self.url_agents))
        plants_df = pd.json_normalize(self.fetch_json(self.url_plants))
        units_df = pd.json_normalize(self.fetch_json(self.url_units))
        return agents_df, plants_df, units_df

    def process_data(
        self, agents_df: pd.DataFrame, plants_df: pd.DataFrame, units_df: pd.DataFrame
    ):
        """Clean, rename, merge and filter all datasets."""

        # --- Rename columns ---
        plants_df = plants_df.rename(
            columns={
                "id": "PlantID",
                "nombre": "PlantName",
                "id_coordinado": "AgentID",
                "coordinado_nombre": "AgentName",
            }
        )
        units_df = units_df.rename(
            columns={"id": "UnitID", "id_central": "PlantID", "nombre": "UnitName"}
        )
        agents_df = agents_df.rename(columns={"id": "AgentID"})

        # --- Extract reuc_id ---
        agents_df["reuc_id"] = agents_df["descripcion"].str.split("_").str[-1]

        # --- Merge into plants ---
        plants_df = plants_df.merge(
            agents_df[["AgentID", "reuc_id"]],
            on="AgentID",
            how="left",
        )
        units_df = units_df.merge(
            plants_df[["PlantID", "AgentID", "reuc_id", "PlantName", "AgentName"]],
            on="PlantID",
            how="left",
        )

        # --- Filter PMGD ---
        distr_plants_df = plants_df[
            plants_df["PlantName"].str.contains("PMGD ", case=False, na=False)
        ]

        return agents_df, plants_df, units_df, distr_plants_df

    def save_to_excel(
        self,
        units_df: pd.DataFrame,
        file="output.xlsx",
    ):
        """Export all DataFrames to Excel with selected columns."""

        fields_to_save = [
            "UnitID",
            "UnitName",
            "PlantName",
            "AgentName",
            "reuc_id",
        ]

        with pd.ExcelWriter(file, engine="openpyxl") as writer:
            units_df[fields_to_save].to_excel(
                writer, sheet_name="GeneratingUnit", index=False
            )

        print(f"Excel file saved as {file}")


if __name__ == "__main__":
    # --------------------------
    # Running the full pipeline:
    # --------------------------

    fetcher = PMGDSDataFetcher()

    # Step 1: fetch raw data
    agents_df, plants_df, units_df = fetcher.fetch_all()

    # Step 2: clean, merge, filter
    agents_df, plants_df, units_df, distr_plants_df = fetcher.process_data(
        agents_df, plants_df, units_df
    )

    # Step 3: export to Excel
    fetcher.save_to_excel(units_df)
