use polars::prelude::*;
use polars_demo::{load_data, unnest_df};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::temp_dir().join("hk_buildings.json");
    load_data(&path)?;
    let file = std::fs::File::open(path)?;
    let mut df = JsonReader::new(file).finish()?.select(["features"])?;
    df = unnest_df(&df)?;
    println!("{:?}", df);
    println!("{:?}", df.column("RECORDCREATIONDATE")?);

    let ambiguous = ChunkedArray::full("amb".into(), "raise", df.size());

    df.with_column(
        df.column("RECORDCREATIONDATE")?
            .str()?
            .as_datetime(
                Some("%FT%H:%M:%SZ"),
                TimeUnit::Nanoseconds,
                true,
                false,
                None,
                &ambiguous,
            )?
            .year()
            .with_name("creation_year".into()),
    )?;

    println!("{:?}", df.column("creation_year")?);
    println!(
        "{:?}",
        df.group_by(["creation_year"])?
            .select(["OBJECTID"])
            .count()?
            .sort(
                ["OBJECTID_count"],
                SortMultipleOptions::default().with_order_descending(true)
            )
    );

    Ok(())
}
