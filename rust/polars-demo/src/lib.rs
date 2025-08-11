use polars::prelude::*;
use reqwest::blocking::Client;
use std::path::Path;
use std::time::Duration;

fn download_data(file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let url = "https://hub.arcgis.com/api/v3/datasets/2163df5803044dc3a8f6b6054092fc71_0/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1";
    let client = Client::new();
    let res = client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .header("Accept-Language", "en-US,en;q=0.5")
        .header("Connection", "keep-alive")
        .timeout(Duration::from_secs(60 * 5))
        .send()?
        .bytes()?;
    std::fs::write(file_path, res)?;
    Ok(())
}
pub fn load_data(file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    match file_path.extension().and_then(|e| e.to_str()) {
        Some("json") => load_data_json(file_path),
        Some("parquet") => load_data_parquet(file_path),
        _ => Err("Unsupported file extension".into()),
    }
}

pub fn load_data_json(file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if !file_path.exists() {
        println!("Downloading ...");
        download_data(file_path)?;
    }
    Ok(())
}

pub fn load_data_parquet(file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if !file_path.exists() {
        let json_path = file_path.with_extension("geojson");
        if json_path.exists() {
            let json_file = std::fs::File::open(&json_path)?;
            let mut df = JsonReader::new(json_file).finish()?;
            df = unnest_df(&df)?;
            let file = std::fs::File::create(file_path)?;
            ParquetWriter::new(file).finish(&mut df)?;
        } else {
            load_data_json(&json_path)?;
            load_data_parquet(file_path)?;
        }
    }
    Ok(())
}
pub fn unnest_df(df: &DataFrame) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let mut data: Vec<Column> = Vec::new();
    for value in df.column("features")?.list()? {
        if let Some(series) = value {
            let geometry = series.struct_()?.field_by_name("geometry")?.into_column();
            let fields_as_series = series
                .struct_()?
                .field_by_name("properties")?
                .struct_()?
                .fields_as_series();
            let properties = fields_as_series.iter().map(|s| s.clone().into_column());

            data.extend(properties);
            data.push(geometry)
        }
    }
    Ok(DataFrame::new(data)?)
}
pub fn unnest_lf(lf: LazyFrame) -> LazyFrame {
    lf.select([
        // unnest all fields from properties struct
        col("features")
            .explode()
            .struct_()
            .field_by_name("properties")
            .alias("properties"),
        // keep geometry as struct
        col("features")
            .explode()
            .struct_()
            .field_by_name("geometry")
            .alias("geometry"),
    ])
    // unnest the properties struct into individual columns
    .unnest(["properties"])
}
