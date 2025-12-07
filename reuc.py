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


if __name__ == "__main__":
    processor = REUCDataProcessor(folder=Path("inputs"))

    agents_df, substitutions_df = processor.load_reuc_data()

    print(substitutions_df.head())
    print(agents_df.head())
