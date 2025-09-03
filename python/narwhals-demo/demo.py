# padas
import pandas as pd

pd_df = pd.read_csv("employees.csv")
pd_df.groupby("department").salary.mean().sort_values(ascending=False)

# polars
import polars as pl

pl_df = pl.scan_csv("employees.csv")

(
    pl_df.group_by("department")
    .agg(pl.col("salary").mean())
    .sort(by="salary", descending=True)
)

pl_df.collect()

# Duckdb
import duckdb

db_df = duckdb.query("SELECT * FROM read_csv('employees.csv')")
db_df.query(
    "df",
    "SELECT department, AVG(salary) as avg_salary FROM df GROUP BY department ORDER BY avg_salary desc",
)
import narwhals as nw
from narwhals.typing import IntoFrameT


def get_avg_by(frame: IntoFrameT, category: str, value: str) -> IntoFrameT:
    df = nw.from_native(frame).lazy()
    return (
        df.group_by(category)
        .agg(nw.col(value).mean())
        .sort(by=value, descending=True)
        .collect()
        .to_native()
    )


get_avg_by(pl_df, "department", "salary")
