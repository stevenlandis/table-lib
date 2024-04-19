use std::fs;
use std::time::Instant;

use table_lib::*;

fn main() {
    let file_contents = fs::read_to_string("../polars-testing/mock_data.json").unwrap();
    let table = Table::from_json_str(file_contents.as_str());
    println!("Table has len={}", table.get_n_rows());

    let t0 = Instant::now();
    let grouped_table = table.group_and_aggregate(
        &["text_col_0"],
        &[Aggregation {
            in_col_name: "float_col_0",
            out_col_name: "float_col_0",
            agg_type: AggregationType::Sum,
        }],
    );
    let e0 = t0.elapsed();
    println!("Took {:.2?} to group and agg", e0);
    println!("Grouped table has len={}", grouped_table.get_n_rows());

    fs::write("grouped.json", grouped_table.to_json_str()).unwrap();
}
