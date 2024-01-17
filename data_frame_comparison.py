import pandas as pd


class DataFrameComparison:
    def __init__(self, old_df, new_df, lookup_col):
        self.merge_sheet = pd.merge(
            old_df,
            new_df,
            on=lookup_col,
            how="outer",
            indicator=True,
        )

        self.lookup_col = lookup_col

        column_list = [col_name for col_name in old_df]
        diffs = []

        for col_name in column_list:
            if col_name == lookup_col:
                continue
            changed_skus = self.merge_sheet.query(
                f'not `{col_name}_x` == `{col_name}_y` and _merge == "both"'
            )[lookup_col]
            if changed_skus.count == 0:
                continue
            diffs.extend(map(lambda lookup_val: (lookup_val, col_name), changed_skus))
        edited_df = pd.DataFrame(diffs, columns=[lookup_col, "Affected Columns"])
        grouped_by_sku = edited_df.groupby(lookup_col, as_index=False).apply(
            lambda df: str.join(",", df["Affected Columns"])
        )
        self.edited_rows = grouped_by_sku.set_axis(
            [lookup_col, "Affected Columns"], axis=1
        )

        self.new_rows = self.merge_sheet.query("_merge == 'right_only'")

        self.unchanged_rows = self.merge_sheet[
            (self.merge_sheet._merge == "both")
            & (~self.merge_sheet[lookup_col].isin(self.edited_rows[lookup_col]))
        ][lookup_col]

    def save_results(self, directory):
        if directory[-1] != "/":
            directory += "/"
        dfs_to_save = [
            (self.edited_rows, "edited"),
            (self.new_rows, "new"),
            (self.unchanged_rows, "unchanged"),
        ]
        for df, df_type in dfs_to_save:
            df.to_csv(f"{directory}{df_type}.csv")


if __name__ == "__main__":
    sheet_v1 = {
        "non_serialized": pd.read_excel(
            "data/flat-files/non_serialized_items_v1.xlsx", "Inventory Item SIK"
        ).fillna(0),
        "serialized": pd.read_excel(
            "data/flat-files/serialized_items_v1.xlsx", "Inventory Item SIK"
        ).fillna(0),
    }
    sheet_v2 = {
        "non_serialized": pd.read_excel(
            "data/flat-files/non_serialized_items_v2.xlsx", "Inventory Item SIK"
        ).fillna(0),
        "serialized": pd.read_excel(
            "data/flat-files/serialized_items_v2.xlsx", "Inventory Item SIK"
        ).fillna(0),
    }

    for key in sheet_v1:
        comp = DataFrameComparison(sheet_v1[key], sheet_v2[key], "Item Name/Number")
        comp.save_results(f"items-comparison-output/{key}")
