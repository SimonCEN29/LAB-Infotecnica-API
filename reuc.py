import pandas as pd
from pathlib import Path


class REUCDataProcessor:

    def __init__(self, folder: Path = Path("input")):
        self.folder = folder

        self.agents_file_path = self.pick_latest("datos_empresas_*.xlsx")
        self.substitutions_file_path = self.pick_latest("datos_reuc_reemplazos_*.xlsx")

        if self.agents_file_path is None:
            raise FileNotFoundError("No matching 'datos_empresas_*.xlsx' file found.")

        if self.substitutions_file_path is None:
            raise FileNotFoundError(
                "No matching 'datos_reuc_reemplazos_*.xlsx' file found."
            )

    def pick_latest(self, pattern: str) -> Path | None:
        files = list(self.folder.glob(pattern))
        if not files:
            return None
        return max(files, key=lambda x: x.stat().st_mtime)

    def load_reuc_data(self) -> tuple[pd.DataFrame, pd.DataFrame]:

        # --- Load Agents ---
        try:
            agents_df = pd.read_excel(self.agents_file_path, sheet_name="Empresas")
        except ValueError as e:
            raise ValueError(
                f"Sheet 'Empresas' not found in {self.agents_file_path}"
            ) from e

        # --- Load Substitutions (first sheet) ---
        substitutions_df = pd.read_excel(self.substitutions_file_path)

        # Rename columns
        agents_df = agents_df.rename(
            columns={
                "id": "reuc_id",
                "Razón Social": "reuc_name",
                "Segmento": "reuc_category",
            }
        )[["reuc_id", "reuc_name", "reuc_category"]]

        substitutions_df = substitutions_df.rename(
            columns={
                "ID": "reuc_old_id",
                "Empresa": "reuc_old_name",
                "Rut": "reuc_old_rut",
                "ID Reemplazo": "reuc_new_id",
                "Reemplazada Por": "reuc_new_name",
                "Rut Reemplazante": "reuc_new_rut",
                "Inicio Reemplazo": "ReplacementStartDate",
                "Fin de Reemplazo": "ReplacementEndDate",
            }
        )[
            [
                "reuc_old_id",
                "reuc_old_name",
                "reuc_old_rut",
                "reuc_new_id",
                "reuc_new_name",
                "reuc_new_rut",
                "ReplacementStartDate",
                "ReplacementEndDate",
            ]
        ]

        return agents_df, substitutions_df

    def get_agents_after_substitution(
        self, agents_df: pd.DataFrame, substitutions_df: pd.DataFrame
    ):
        # Create a new DataFrame with agents with their substitutions,
        # only when datetime.today() is within the substitution period.
        from datetime import datetime

        updated_agents_df = agents_df.copy()

        for index, row in updated_agents_df.iterrows():
            reuc_id = row["reuc_id"]
            substitution_rows = substitutions_df[
                substitutions_df["reuc_old_id"] == reuc_id
            ]

            for _, sub_row in substitution_rows.iterrows():
                replacement_start = sub_row["ReplacementStartDate"]
                replacement_end = sub_row["ReplacementEndDate"]

                if (
                    datetime.today() >= replacement_start
                    and datetime.today() <= replacement_end
                ):
                    reuc_new_id = sub_row["reuc_new_id"]
                    reuc_new_name = sub_row["reuc_new_name"]

                    updated_agents_df.at[index, "reuc_id"] = reuc_new_id
                    updated_agents_df.at[index, "reuc_name"] = reuc_new_name

                    print(f"Agent {row['reuc_name']} is under REUC substitution.")

        return updated_agents_df

    def get_pmgd_agents(self, agents_df: pd.DataFrame) -> pd.DataFrame:
        # Filter agents that contain "PMGD", within the string,  in the field "reuc_category"
        pmgd_agents_df = agents_df[
            agents_df["reuc_category"].str.contains("PMGD", case=False, na=False)
        ].reset_index(drop=True)
        return pmgd_agents_df


if __name__ == "__main__":
    from datetime import datetime
    import os

    os.system("cls" if os.name == "nt" else "clear")

    processor = REUCDataProcessor(folder=Path("input"))

    agents_df, substitutions_df = processor.load_reuc_data()
    pmgd_agents_df = processor.get_pmgd_agents(agents_df)

    updated_agents_df = processor.get_agents_after_substitution(
        agents_df, substitutions_df
    )

    substitutions_file_path = Path(
        f"output/substitutions at {datetime.today().strftime('%Y-%m-%d')}.xlsx"
    )
    agents_file_path = Path(
        f"output/agents at {datetime.today().strftime('%Y-%m-%d')}.xlsx"
    )
    pmgd_agents_file_path = Path(
        f"output/pmgd_agents at {datetime.today().strftime('%Y-%m-%d')}.xlsx"
    )

    substitutions_df.to_excel(substitutions_file_path, index=False)
    agents_df.to_excel(agents_file_path, index=False)
    pmgd_agents_df.to_excel(pmgd_agents_file_path, index=False)
    print(f"\nSubstitutions saved to {substitutions_file_path}")
    print(f"Agents saved to {agents_file_path}")
    print(f"PMGD Agents saved to {pmgd_agents_file_path}\n")
