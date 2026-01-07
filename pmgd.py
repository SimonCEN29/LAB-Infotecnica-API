from fetcher import InfoFetcher

fetcher = InfoFetcher()

units_df = fetcher.fetch_generating_units()
pmgd_units_df = units_df[units_df["nombre"].str.contains("PMGD", na=False)]
pmgd_units_df.to_excel("temp/pmgd_units.xlsx", index=False)
