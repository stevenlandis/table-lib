use serde::{Deserialize, Serialize};
use std::{collections::HashMap, iter::zip};

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

    pub fn to_json(&self) -> String {
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
