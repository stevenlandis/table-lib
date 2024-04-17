use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::rc::Rc;
use std::{collections::HashMap, hash::Hasher, iter::zip};

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
    column: Rc<Column>,
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
                column: Rc::new(parsed_col),
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
        let group_cols = groups
            .iter()
            .map(|group_col| &*self.columns[self.col_map[*group_col]].column)
            .collect::<Vec<_>>();

        let row_hashes = Column::get_col_hashes(self.get_n_rows(), group_cols.as_slice());

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
                column: Rc::new(new_col),
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
                column: Rc::new(out_col),
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

    pub fn select_and_rename(&self, fields: &[RenameCol]) -> Table {
        let mut new_columns = Vec::<TableColumnWrapper>::with_capacity(fields.len());
        let mut new_col_map = HashMap::<String, usize>::with_capacity(fields.len());

        for field in fields {
            new_col_map.insert(field.new_name.to_string(), new_columns.len());
            new_columns.push(TableColumnWrapper {
                name: field.new_name.to_string(),
                column: self.columns[self.col_map[field.old_name]].column.clone(),
            });
        }

        return Table {
            columns: new_columns,
            col_map: new_col_map,
            n_rows: self.n_rows,
        };
    }

    pub fn augment(
        &self,
        right: &Self,
        join_on: &[(&str, &str)],
        new_columns: &[RenameCol],
    ) -> Table {
        struct HashKey<'a> {
            row_hash: u64,
            idx: usize,
            columns: &'a [&'a TableColumnWrapper],
        }

        impl Eq for HashKey<'_> {}
        impl PartialEq for HashKey<'_> {
            fn eq(&self, other: &Self) -> bool {
                for (left_col, right_col) in zip(self.columns, other.columns) {
                    if !left_col
                        .column
                        .is_equal_at_index(&right_col.column, self.idx, other.idx)
                    {
                        return false;
                    }
                }
                return true;
            }
        }
        impl Hash for HashKey<'_> {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.row_hash.hash(state);
            }
        }

        let mut right_key_groups = HashMap::<HashKey, Vec<usize>>::new();

        let right_cols_for_hash = join_on
            .iter()
            .map(|(_, col_name)| &*right.columns[right.col_map[*col_name]].column)
            .collect::<Vec<_>>();
        let right_hashes =
            Column::get_col_hashes(right.get_n_rows(), right_cols_for_hash.as_slice());

        let right_cols = join_on
            .iter()
            .map(|(_, col_name)| &right.columns[right.col_map[*col_name]])
            .collect::<Vec<_>>();

        for idx in 0..right.get_n_rows() {
            let key = HashKey {
                row_hash: right_hashes[idx],
                idx,
                columns: right_cols.as_slice(),
            };

            if right_key_groups.contains_key(&key) {
                right_key_groups.get_mut(&key).unwrap().push(idx);
            } else {
                right_key_groups.insert(key, vec![idx]);
            }
        }

        let mut join_mapping = Vec::<(usize, Option<usize>)>::new();

        let left_cols_for_hash = join_on
            .iter()
            .map(|(col_name, _)| &*self.columns[self.col_map[*col_name]].column)
            .collect::<Vec<_>>();
        let left_hashes = Column::get_col_hashes(self.get_n_rows(), left_cols_for_hash.as_slice());

        let left_cols = join_on
            .iter()
            .map(|(col_name, _)| &self.columns[self.col_map[*col_name]])
            .collect::<Vec<_>>();

        for left_idx in 0..self.get_n_rows() {
            let key = HashKey {
                row_hash: left_hashes[left_idx],
                idx: left_idx,
                columns: left_cols.as_slice(),
            };

            match right_key_groups.get(&key) {
                None => {
                    join_mapping.push((left_idx, None));
                }
                Some(key_group) => {
                    for right_idx in key_group {
                        join_mapping.push((left_idx, Some(*right_idx)));
                    }
                }
            }
        }

        let mut new_cols = Vec::<TableColumnWrapper>::new();
        let mut new_col_map = HashMap::<String, usize>::new();

        /*
        Output Column Order:
            - all fields on left
            - selected fields from right
        */

        let left_idxs = join_mapping.iter().map(|(idx, _)| *idx).collect::<Vec<_>>();
        let right_idxs = join_mapping.iter().map(|(_, idx)| *idx).collect::<Vec<_>>();

        // add the left col values
        for col in &self.columns {
            new_col_map.insert(col.name.clone(), new_cols.len());
            new_cols.push(TableColumnWrapper {
                name: col.name.clone(),
                column: Rc::new(col.column.get_new_col_from_idx_map(left_idxs.as_slice())),
            });
        }

        // add the right col values
        for select in new_columns {
            let col = &right.columns[right.col_map[select.old_name]];

            new_col_map.insert(select.new_name.to_string(), new_cols.len());
            new_cols.push(TableColumnWrapper {
                name: select.new_name.to_string(),
                column: Rc::new(
                    col.column
                        .get_new_col_from_opt_idx_map(right_idxs.as_slice()),
                ),
            })
        }

        return Table {
            columns: new_cols,
            col_map: new_col_map,
            n_rows: join_mapping.len(),
        };
    }
}

pub struct RenameCol<'a> {
    pub old_name: &'a str,
    pub new_name: &'a str,
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
