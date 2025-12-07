import pandas as pd
import pmgd
import reuc
from datetime import datetime

pmgd_fetcher = pmgd.PMGDSDataFetcher()

# Fetch raw data
agents_df, plants_df, units_df = pmgd_fetcher.fetch_all()

# Clean, merge, filter
distr_units_df = pmgd_fetcher.process_data(agents_df, plants_df, units_df)

# Load REUC data
reuc_processor = reuc.REUCDataProcessor()
reuc_agents_df, reuc_substitutions_df = reuc_processor.load_reuc_data()

# Ensure ID columns are of type string for proper merging
distr_units_df = distr_units_df.astype({"reuc_id": str})

reuc_agents_df = reuc_agents_df.astype({"reuc_id": str})
reuc_substitutions_df = reuc_substitutions_df.astype(
    {"reuc_old_id": str, "reuc_new_id": str}
)

# Merge substitution data
distr_units_df = distr_units_df.merge(
    reuc_substitutions_df[
        [
            "reuc_old_id",
            "reuc_new_id",
            "ReplacementStartDate",
            "ReplacementEndDate",
        ]
    ],
    how="left",
    left_on="reuc_id",
    right_on="reuc_old_id",
).drop(columns=["reuc_old_id"])

distr_units_df = distr_units_df.merge(
    reuc_agents_df[["reuc_id", "reuc_name"]],
    how="inner",
    left_on="reuc_id",
    right_on="reuc_id",
).drop(columns=["AgentName", "TechTypeName"])

for index, row in distr_units_df.iterrows():
    replacement_start = row["ReplacementStartDate"]
    replacement_end = row["ReplacementEndDate"]

    if datetime.today() >= replacement_start and datetime.today() <= replacement_end:
        reuc_new_id = row["reuc_new_id"]

        reuc_new_name = reuc_agents_df.loc[
            reuc_agents_df["reuc_id"] == reuc_new_id, "reuc_name"
        ]

        distr_units_df.at[index, "reuc_id"] = reuc_new_id
        distr_units_df.at[index, "reuc_name"] = reuc_new_name.values[0]

        print(f"Unit {row['GeneratingUnitID']} is under REUC substitution.")

distr_units_df["reuc_id"] = pd.to_numeric(
    distr_units_df["reuc_id"], errors="coerce"
).astype("int64")


distr_units_df.drop(
    columns=["reuc_new_id", "ReplacementStartDate", "ReplacementEndDate"]
).to_excel(
    f"output\\output at {datetime.now().strftime('%Y.%m.%d %H.%M.%S')}.xlsx",
    sheet_name="GeneratingUnit",
    index=False,
)
