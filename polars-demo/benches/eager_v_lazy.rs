use criterion::{criterion_group, criterion_main, Criterion};
use polars::prelude::*;
use polars_demo::load_data;

fn eager() -> Result<DataFrame, Box<dyn std::error::Error>> {
    let path = std::env::temp_dir().join("hk_buildings.parquet");
    load_data(&path)?;
    let file = std::fs::File::open(path)?;
    let mut df = ParquetReader::new(file).finish()?;
    let ambiguous = ChunkedArray::full("amb".into(), "raise", df.size());
    df.with_column(
        df.column("RECORDCREATIONDATE")?
            .str()?
            .as_datetime(
                Some("%FT%H:%M:%SZ"),
                TimeUnit::Milliseconds,
                true,
                false,
                None,
                &ambiguous,
            )?
            .year()
            .with_name("creation_year".into()),
    )?;

    let result = df
        .group_by(["creation_year"])?
        .select(["OBJECTID"])
        .count()?
        .sort(
            ["OBJECTID_count"],
            SortMultipleOptions::default().with_order_descending(true),
        )?;

    Ok(result)
}

fn lazy() -> Result<DataFrame, Box<dyn std::error::Error>> {
    let path = std::env::temp_dir().join("hk_buildings.parquet");
    load_data(&path)?;
    let mut lf: LazyFrame = LazyFrame::scan_parquet(path, ScanArgsParquet::default())?;
    println!("{:?}", lf.collect_schema());

    lf = lf.with_column(
        col("RECORDCREATIONDATE")
            .str()
            .strptime(
                DataType::Datetime(TimeUnit::Milliseconds, None),
                StrptimeOptions {
                    format: Some("%FT%H:%M:%SZ".into()),
                    strict: false,
                    exact: true,
                    cache: false,
                },
                lit("raise"),
            )
            .dt()
            .year()
            .alias("creation_year"),
    );

    let result = lf
        .group_by(["creation_year"])
        .agg([col("OBJECTID").count()])
        .sort(
            ["OBJECTID"],
            SortMultipleOptions::default().with_order_descending(true),
        )
        .collect()?;

    Ok(result)
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("eager", |b| b.iter(|| eager()));
    c.bench_function("lazy", |b| b.iter(|| lazy()));
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
);
criterion_main!(benches);
