use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hasher},
    iter::zip,
};

use super::column::{Column, ColumnValues, Float64ColumnValues, TextColumnValues};

#[derive(Debug)]
pub struct Table {
    col_map: HashMap<String, usize>,
    columns: Vec<TableColumnWrapper>,
    n_rows: usize,
}

#[derive(Debug)]
struct TableColumnWrapper {
    name: String,
    column: Column,
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        if self.col_map.len() != other.col_map.len() {
            return false;
        }

        for (col_name, col_idx) in self.col_map.iter() {
            let other_col_idx_opt = other.col_map.get(col_name);
            match other_col_idx_opt {
                None => {
                    return false;
                }
                Some(other_col_idx) => {
                    let left_col = &self.columns[*col_idx];
                    let right_col = &other.columns[*other_col_idx];
                    if left_col.column != right_col.column {
                        return false;
                    }
                }
            }
        }

        return true;
    }
}

impl Table {
    pub fn get_n_rows(&self) -> usize {
        return self.n_rows;
    }

    pub fn from_json_str(json_str: &str) -> Table {
        let json_table: JsonTable = serde_json::from_str(json_str).unwrap();

        let mut col_map = HashMap::<String, usize>::new();
        let mut columns = Vec::<TableColumnWrapper>::new();
        let mut n_rows: usize = 0;

        for col in &json_table.columns {
            let parsed_col = match col._type.as_str() {
                "text" => {
                    let mut nulls = Vec::<bool>::new();
                    let mut values = Vec::<String>::new();
                    for value in &col.values {
                        match value {
                            None => {
                                nulls.push(true);
                                values.push("".to_string());
                            }
                            Some(val_str) => {
                                nulls.push(false);
                                values.push(val_str.clone());
                            }
                        }
                    }

                    Column {
                        nulls,
                        values: ColumnValues::Text(TextColumnValues { values }),
                    }
                }
                "float64" => {
                    let mut nulls = Vec::<bool>::new();
                    let mut values = Vec::<f64>::new();
                    for value in &col.values {
                        match value {
                            None => {
                                nulls.push(true);
                                values.push(0.0);
                            }
                            Some(val_str) => match val_str.parse::<f64>() {
                                Ok(val_float) => {
                                    nulls.push(false);
                                    values.push(val_float);
                                }
                                Err(_) => {
                                    nulls.push(true);
                                    values.push(0.0);
                                }
                            },
                        }
                    }

                    Column {
                        nulls,
                        values: ColumnValues::Float64(Float64ColumnValues { values }),
                    }
                }
                _ => {
                    panic!("Unable to parse column");
                }
            };
            n_rows = parsed_col.get_n_rows();
            col_map.insert(col.name.clone(), columns.len());
            columns.push(TableColumnWrapper {
                name: col.name.clone(),
                column: parsed_col,
            });
        }

        return Table {
            col_map,
            columns,
            n_rows,
        };
    }

    pub fn to_json_str(&self) -> String {
        let mut columns = Vec::<JsonColumn>::new();
        for table_col in self.columns.iter() {
            let col = &table_col.column;
            let json_col = match &col.values {
                ColumnValues::Text(text_col) => {
                    let mut values = Vec::<Option<String>>::new();
                    for (is_null, value) in zip(&col.nulls, &text_col.values) {
                        if *is_null {
                            values.push(None);
                        } else {
                            values.push(Some(value.clone()));
                        }
                    }

                    JsonColumn {
                        name: table_col.name.clone(),
                        _type: "text".to_string(),
                        values,
                    }
                }
                ColumnValues::Float64(float_col) => {
                    let mut values = Vec::<Option<String>>::new();

                    for (is_null, value) in zip(&col.nulls, &float_col.values) {
                        if *is_null {
                            values.push(None);
                        } else {
                            values.push(Some(value.to_string()));
                        }
                    }

                    JsonColumn {
                        name: table_col.name.clone(),
                        _type: "float64".to_string(),
                        values,
                    }
                }
            };

            columns.push(json_col);
        }

        let json_table = JsonTable { columns };

        return serde_json::to_string(&json_table).unwrap();
    }

    pub fn group_and_aggregate(&self, groups: &[&str], aggregations: &[Aggregation]) -> Table {
        let mut row_hashers = (0..self.get_n_rows())
            .map(|_| DefaultHasher::new())
            .collect::<Vec<_>>();

        for group in groups {
            let col = &self.columns[*self.col_map.get(*group).unwrap()].column;
            match &col.values {
                ColumnValues::Text(text_col) => {
                    for (hasher, (is_null, value)) in
                        zip(&mut row_hashers, zip(&col.nulls, &text_col.values))
                    {
                        if *is_null {
                            0.hash(hasher);
                        } else {
                            value.hash(hasher);
                        }
                    }
                }
                ColumnValues::Float64(float_col) => {
                    for (hasher, (is_null, value)) in
                        zip(&mut row_hashers, zip(&col.nulls, &float_col.values))
                    {
                        if *is_null {
                            0.hash(hasher);
                        } else {
                            (*value as u64).hash(hasher);
                        }
                    }
                }
            }
        }

        let row_hashes = row_hashers
            .iter()
            .map(|hasher| hasher.finish())
            .collect::<Vec<_>>();

        let group_idxs = groups
            .iter()
            .map(|group| *self.col_map.get(*group).unwrap())
            .collect::<Vec<_>>();

        struct GroupKey<'a> {
            table: &'a Table,
            group_idxs: &'a [usize],
            row_hash: u64,
            idx: usize,
        }

        impl Eq for GroupKey<'_> {}
        impl PartialEq for GroupKey<'_> {
            fn eq(&self, other: &Self) -> bool {
                for group_idx in self.group_idxs {
                    let col = &self.table.columns[*group_idx];

                    if !col.column.eq_at_indexes(self.idx, other.idx) {
                        return false;
                    }
                }

                return true;
            }
        }

        impl Hash for GroupKey<'_> {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.row_hash.hash(state);
            }
        }

        let mut index_groups = Vec::<Vec<usize>>::new();
        let mut group_to_idx_map = HashMap::<GroupKey, usize>::new();

        for idx in 0..self.get_n_rows() {
            let group_key = GroupKey {
                table: self,
                group_idxs: &group_idxs.as_slice(),
                row_hash: row_hashes[idx],
                idx,
            };
            let group_idx_opt = group_to_idx_map.get(&group_key);
            match group_idx_opt {
                None => {
                    group_to_idx_map.insert(group_key, index_groups.len());
                    index_groups.push(vec![idx]);
                }
                Some(group_idx) => {
                    index_groups[*group_idx].push(idx);
                }
            }
        }

        let mut new_columns = Vec::<TableColumnWrapper>::new();

        // add group columns
        for group_idx in group_idxs {
            let col = &self.columns[group_idx];
            let new_col = match &col.column.values {
                ColumnValues::Text(inner_col) => {
                    let mut new_nulls = Vec::<bool>::with_capacity(index_groups.len());
                    let mut new_values = Vec::<String>::with_capacity(index_groups.len());
                    for idx_group in &index_groups {
                        new_nulls.push(col.column.nulls[idx_group[0]]);
                        new_values.push(inner_col.values[idx_group[0]].clone());
                    }

                    Column {
                        nulls: new_nulls,
                        values: ColumnValues::Text(TextColumnValues { values: new_values }),
                    }
                }
                ColumnValues::Float64(inner_col) => {
                    let mut new_nulls = Vec::<bool>::with_capacity(index_groups.len());
                    let mut new_values = Vec::<f64>::with_capacity(index_groups.len());
                    for idx_group in &index_groups {
                        new_nulls.push(col.column.nulls[idx_group[0]]);
                        new_values.push(inner_col.values[idx_group[0]]);
                    }

                    Column {
                        nulls: new_nulls,
                        values: ColumnValues::Float64(Float64ColumnValues { values: new_values }),
                    }
                }
            };
            new_columns.push(TableColumnWrapper {
                name: col.name.clone(),
                column: new_col,
            });
        }

        // add aggregation column values
        for agg in aggregations {
            let in_col = &self.columns[self.col_map[agg.in_col_name]].column;
            let out_col: Column = match agg.agg_type {
                AggregationType::Sum => match &in_col.values {
                    ColumnValues::Float64(values) => {
                        let mut out_nulls = Vec::<bool>::with_capacity(index_groups.len());
                        let mut out_values = Vec::<f64>::with_capacity(index_groups.len());
                        for idx_group in &index_groups {
                            let mut has_non_null = false;
                            let mut val: f64 = 0.0;
                            for idx in idx_group {
                                if !in_col.nulls[*idx] {
                                    has_non_null = true;
                                    val += values.values[*idx];
                                }
                            }
                            out_nulls.push(!has_non_null);
                            out_values.push(val);
                        }

                        Column {
                            nulls: out_nulls,
                            values: ColumnValues::Float64(Float64ColumnValues {
                                values: out_values,
                            }),
                        }
                    }
                    _ => {
                        panic!("Unsupported SUM agg for this col type")
                    }
                },
                _ => {
                    panic!("Unsupported agg type");
                }
            };
            new_columns.push(TableColumnWrapper {
                name: agg.out_col_name.to_string(),
                column: out_col,
            });
        }
        let new_col_map = new_columns
            .iter()
            .enumerate()
            .map(|(idx, col)| (col.name.clone(), idx))
            .collect::<HashMap<String, usize>>();

        return Table {
            col_map: new_col_map,
            columns: new_columns,
            n_rows: index_groups.len(),
        };
    }
}

pub struct Aggregation<'a> {
    pub in_col_name: &'a str,
    pub out_col_name: &'a str,
    pub agg_type: AggregationType,
}

pub enum AggregationType {
    Sum,
    Min,
    Max,
}

#[derive(Serialize, Deserialize)]
struct JsonColumn {
    name: String,
    #[serde(rename = "type")]
    _type: String,
    values: Vec<Option<String>>,
}

#[derive(Serialize, Deserialize)]
struct JsonTable {
    columns: Vec<JsonColumn>,
}
