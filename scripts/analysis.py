import ast
import sys
from datetime import datetime
import pandas as pd


def analyze_schema_changes(df):
    df["columns"] = df["columns"].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) else (x or [])
    )
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["col_set"] = df["columns"].apply(lambda cols: set(cols))
    df = df.sort_values(["source", "datetime"]).reset_index(drop=True)

    changes = []

    for src, group in df.groupby("source"):
        prev_set = None
        for _, row in group.iterrows():
            dt = row["datetime"]
            cur_set = row["col_set"]
            if prev_set is not None and cur_set != prev_set:
                added = sorted(cur_set - prev_set)
                removed = sorted(prev_set - cur_set)
                changes.append({"source": src, "change_at": dt, "added": added, "removed": removed})
            prev_set = cur_set

    return pd.DataFrame(changes)


if __name__ == "__main__":
    try:
        df = pd.read_csv("schema_evolution_audit.csv")
        # df = pd.read_csv("schema_evolution_audit_test.csv")
    except FileNotFoundError:
        print("🚨 schema_evolution_audit_test.csv not found", file=sys.stderr)
        sys.exit(1)

    changes_df = analyze_schema_changes(df)

    if changes_df.empty:
        print("✅ No schema changes detected in the scanned range.")
    else:
        print("🔍 Schema changes detected:")
        print(changes_df.to_string(index=False))
