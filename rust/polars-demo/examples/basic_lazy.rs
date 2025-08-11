use polars::df;
use polars::prelude::*;

fn main() -> PolarsResult<()> {
    let df = df![
        "a" => [1, 2, 3],
        "b" => [None, Some("a"), Some("b")],
        "c" => ["foo","bar","baz"]
    ]?;

    let lf: LazyFrame = df.lazy().with_columns([
        (col("a") + lit(10)).alias("d"),
        col("c").str().to_uppercase().alias("upper_c"),
    ]);
    println!("{:?}", lf.collect());
    Ok(())
}
