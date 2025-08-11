use geo::{Centroid, Polygon};
use polars::prelude::*;
use polars_demo::load_data;

fn calculate_centroid(coords: Vec<Vec<f64>>) -> Option<(f64, f64)> {
    let polygon = Polygon::new(
        geo::LineString::from(
            coords
                .into_iter()
                .map(|coord| (coord[0], coord[1]))
                .collect::<Vec<_>>(),
        ),
        vec![],
    );
    polygon
        .centroid()
        .map(|centroid| (centroid.x(), centroid.y()))
}

fn get_xy_coords() -> Expr {
    col("geometry")
        .struct_()
        .field_by_name("coordinates")
        .list()
        .get(0.into(), true)
        .map(
            |l| {
                let ca = l.list()?;
                let out: Vec<Option<(f64, f64)>> = ca
                    .into_iter()
                    .map(|opt_coords| {
                        opt_coords.and_then(|coords| {
                            let coords_vec = coords
                                .list()
                                .unwrap()
                                .into_iter()
                                .filter_map(|opt_inner_coords| {
                                    opt_inner_coords.map(|inner_coords| {
                                        inner_coords
                                            .f64()
                                            .unwrap()
                                            .into_iter()
                                            .filter_map(|x| x)
                                            .collect::<Vec<_>>()
                                    })
                                })
                                .collect::<Vec<_>>();

                            calculate_centroid(coords_vec)
                        })
                    })
                    .collect();

                let x_series: Series = out.iter().map(|opt| opt.map(|p| p.0)).collect();
                let y_series: Series = out.iter().map(|opt| opt.map(|p| p.1)).collect();

                let x_series = x_series.with_name("x".into());
                let y_series = y_series.with_name("y".into());

                let struct_chunked = StructChunked::from_series(
                    "new".into(),
                    out.len(),
                    [x_series, y_series].iter(),
                )?;

                Ok(Some(struct_chunked.into_column()))
            },
            GetOutput::from_type(DataType::Struct(vec![
                Field::new("x".into(), DataType::Float64),
                Field::new("y".into(), DataType::Float64),
            ])),
        )
        .alias("coords")
}
fn make_buckets(col_name: &str, bin_width: f64) -> Expr {
    let col_expr = col(col_name);
    let idx = (col_expr / lit(bin_width)).floor().cast(DataType::Int32);
    ((idx.clone() * lit(bin_width)).cast(DataType::String)
        + lit("-")
        + ((idx + lit(1)) * lit(bin_width)).cast(DataType::String))
    .alias(format!("{}_bucket", col_name))
}

fn haversine(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6371.0; // Earth's radius in kilometers
    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();
    let a = (d_lat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (d_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    r * c
}

fn prep_data() -> Result<LazyFrame, Box<dyn std::error::Error>> {
    let path = std::env::temp_dir().join("hk_buildings.parquet");
    load_data(&path)?;
    let mut lf: LazyFrame = LazyFrame::scan_parquet(path, ScanArgsParquet::default())?;
    let soho_house = (114.1441448, 22.2878391);
    let coords = get_xy_coords();

    let creation_year = col("RECORDCREATIONDATE")
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
        .alias("creation_year");

    lf = lf.with_columns([coords, creation_year]).with_column(
        col("coords")
            .map(
                move |s| {
                    let ca = s.struct_()?;
                    let x_series = ca.field_by_name("x")?;
                    let y_series = ca.field_by_name("y")?;

                    let x_chunked = x_series.f64()?;
                    let y_chunked = y_series.f64()?;

                    let distances: Float64Chunked = x_chunked
                        .into_iter()
                        .zip(y_chunked.into_iter())
                        .map(|(opt_x, opt_y)| match (opt_x, opt_y) {
                            (Some(lon), Some(lat)) => {
                                Some(haversine(soho_house.1, soho_house.0, lat, lon))
                            }
                            _ => None,
                        })
                        .collect();

                    Ok(Some(distances.into_column()))
                },
                GetOutput::from_type(DataType::Float64),
            )
            .alias("distance_km"),
    );
    Ok(lf)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let lf = prep_data()?;

    // 1. Floor Area Analysis
    println!("\n=== Floor Area Distribution (in sq meters) ===");
    let floor_area = lf
        .clone()
        .filter(col("GROSSFLOORAREA").is_not_null())
        .with_column(make_buckets("GROSSFLOORAREA", 1000.0))
        .group_by([col("GROSSFLOORAREA_bucket")])
        .agg([col("OBJECTID").count().alias("count")])
        .sort(
            ["count"],
            SortMultipleOptions::default().with_order_descending(true),
        )
        .limit(10)
        .collect()?;
    println!("{}", floor_area);

    // 2. Height Analysis of Tall Buildings
    println!("\n=== Tallest Buildings (>100m) ===");
    let tall_buildings = lf
        .clone()
        .filter(col("TOPHEIGHT").gt(lit(100.0)))
        .filter(col("NUMABOVEGROUNDSTOREYS").gt(lit(50.0)))
        .select([
            col("OFFICIALBUILDINGNAMEEN"),
            col("TOPHEIGHT"),
            col("NUMABOVEGROUNDSTOREYS"),
        ])
        .sort(
            ["TOPHEIGHT"],
            SortMultipleOptions::default().with_order_descending(true),
        )
        .drop_nulls(Some(vec![
            col("OFFICIALBUILDINGNAMEEN"),
            col("NUMABOVEGROUNDSTOREYS"),
        ]))
        .limit(10)
        .collect()?;
    println!("{}", tall_buildings);

    // 3. building age and height correlation
    println!("\n=== Average Building Height by record creation period ===");
    let height_by_year = lf
        .clone()
        .filter(col("TOPHEIGHT").is_not_null())
        .group_by([col("creation_year")])
        .agg([
            col("TOPHEIGHT").mean().alias("avg_height"),
            col("OBJECTID").count().alias("building_count"),
        ])
        .filter(col("building_count").gt(lit(100))) // filter out years with few buildings
        .sort(
            ["creation_year"],
            SortMultipleOptions::default().with_order_descending(false),
        )
        .collect()?;
    println!("{}", height_by_year);

    // 4. District Density Analysis (using centroids)
    println!("\n=== Building Density Analysis ===");
    let density_analysis = lf
        .clone()
        .with_column(make_buckets("distance_km", 1.0))
        .group_by([col("distance_km_bucket")])
        .agg([
            col("OBJECTID").count().alias("building_count"),
            col("TOPHEIGHT").mean().alias("avg_height"),
            col("GROSSFLOORAREA").mean().alias("avg_floor_area"),
        ])
        .sort(
            ["distance_km_bucket"],
            SortMultipleOptions::default().with_order_descending(false),
        )
        .limit(10) // Show first 10km from reference point
        .collect()?;

    println!("{}", density_analysis);
    Ok(())
}
