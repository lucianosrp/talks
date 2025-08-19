import duckdb as db
import narwhals as nw
import pandas as pd
import polars as pl
from narwhals.typing import IntoDataFrameT

# Pandas DataFrame
pd_df = pd.read_csv("employees.csv")
pd_df.groupby("department").salary.mean().sort_values(ascending=False)

# Polars DataFrame
pl_df = pl.scan_csv("employees.csv")
pl_df.group_by("department").agg(pl.col("salary").mean()).sort(
    "salary", descending=True
).collect()

# Duck DB Relation
db_rel = db.read_csv("employees.csv")
db_rel.aggregate("department, mean(salary) as salary", "department").order(
    "salary desc"
)


# Narwhals dataframe-agnostic function
def get_avg(frame: IntoDataFrameT, by: str, agg: str) -> IntoDataFrameT:
    return (
        nw.from_native(frame)
        .group_by(by)
        .agg(nw.col(agg).mean())
        .sort(agg, descending=True)
        .to_native()
    )


# Send them !!! 💣
get_avg(pd_df, "department", "salary")
get_avg(pl_df, "department", "salary").collect()
get_avg(db_rel, "department", "salary")
