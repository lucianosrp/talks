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

    lf = lf.with_column(coords).with_column(
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

fn make_buckets(col_name: &str, bin_width: f64) -> Expr {
    let col_expr = col(col_name);
    let idx = (col_expr / lit(bin_width)).floor().cast(DataType::Int32);
    ((idx.clone() * lit(bin_width)).cast(DataType::String)
        + lit("-")
        + ((idx + lit(1)) * lit(bin_width)).cast(DataType::String))
    .alias(format!("{}_bucket", col_name))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let lf = prep_data()?
        .filter(col("distance_km").lt(lit(10)))
        .group_by([make_buckets("GROSSFLOORAREA", 1000.0)])
        .agg([col("OBJECTID").count().alias("count")])
        .sort(
            ["count"],
            SortMultipleOptions::new().with_order_descending(true),
        )
        .drop_nulls(None);

    println!("{:?}", lf.collect());
    Ok(())
}
