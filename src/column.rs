use std::hash::{Hash, Hasher};
use std::{hash::DefaultHasher, iter::zip};

#[derive(Debug)]
pub struct Column {
    pub nulls: Vec<bool>,
    pub values: ColumnValues,
}

impl Column {
    pub fn get_n_rows(&self) -> usize {
        return match &self.values {
            ColumnValues::Text(col) => col.values.len(),
            ColumnValues::Float64(col) => col.values.len(),
        };
    }

    pub fn eq_at_indexes(&self, left_idx: usize, right_idx: usize) -> bool {
        if self.nulls[left_idx] != self.nulls[right_idx] {
            return false;
        }
        if self.nulls[left_idx] {
            return true;
        }

        match &self.values {
            ColumnValues::Text(inner_col) => {
                return inner_col.values[left_idx] == inner_col.values[right_idx];
            }
            ColumnValues::Float64(inner_col) => {
                return inner_col.values[left_idx] == inner_col.values[right_idx];
            }
        }
    }

    pub fn is_equal_at_index(&self, other: &Self, left_idx: usize, right_idx: usize) -> bool {
        if self.nulls[left_idx] != other.nulls[right_idx] {
            return false;
        }
        if self.nulls[left_idx] {
            return true;
        }

        return match &self.values {
            ColumnValues::Text(inner_col) => match &other.values {
                ColumnValues::Text(other_inner_col) => {
                    inner_col.values[left_idx] == other_inner_col.values[right_idx]
                }
                _ => false,
            },
            ColumnValues::Float64(inner_col) => match &other.values {
                ColumnValues::Float64(other_inner_col) => {
                    inner_col.values[left_idx] == other_inner_col.values[right_idx]
                }
                _ => false,
            },
        };
    }

    pub fn get_new_col_from_idx_map(&self, idx_mapping: &[usize]) -> Column {
        Column {
            nulls: idx_mapping.iter().map(|idx| self.nulls[*idx]).collect(),
            values: match &self.values {
                ColumnValues::Text(inner_col) => ColumnValues::Text(TextColumnValues {
                    values: idx_mapping
                        .iter()
                        .map(|idx| inner_col.values[*idx].clone())
                        .collect(),
                }),
                ColumnValues::Float64(inner_col) => ColumnValues::Float64(Float64ColumnValues {
                    values: idx_mapping
                        .iter()
                        .map(|idx| inner_col.values[*idx])
                        .collect(),
                }),
            },
        }
    }

    pub fn get_new_col_from_opt_idx_map(&self, idx_mapping: &[Option<usize>]) -> Column {
        Column {
            nulls: idx_mapping
                .iter()
                .map(|idx_opt| match idx_opt {
                    None => true,
                    Some(idx) => self.nulls[*idx],
                })
                .collect(),
            values: match &self.values {
                ColumnValues::Text(inner_col) => ColumnValues::Text(TextColumnValues {
                    values: idx_mapping
                        .iter()
                        .map(|idx_opt| match idx_opt {
                            None => "".to_string(),
                            Some(idx) => inner_col.values[*idx].clone(),
                        })
                        .collect(),
                }),
                ColumnValues::Float64(inner_col) => ColumnValues::Float64(Float64ColumnValues {
                    values: idx_mapping
                        .iter()
                        .map(|idx_opt| match idx_opt {
                            None => 0.0,
                            Some(idx) => inner_col.values[*idx],
                        })
                        .collect(),
                }),
            },
        }
    }

    pub fn get_col_hashes(n_rows: usize, columns: &[&Column]) -> Vec<u64> {
        let mut row_hashers = (0..n_rows)
            .map(|_| DefaultHasher::new())
            .collect::<Vec<_>>();

        for col in columns {
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

        return row_hashes;
    }

    pub fn batch_equals(columns: &[&Column], equalities: &[(usize, usize)]) -> Vec<bool> {
        let mut is_equal = (0..equalities.len()).map(|_| true).collect::<Vec<_>>();

        for col in columns {
            match &col.values {
                ColumnValues::Text(inner_col) => {
                    for (idx, (left_idx, right_idx)) in equalities.iter().enumerate() {
                        if is_equal[idx]
                            && inner_col.values[*left_idx] != inner_col.values[*right_idx]
                        {
                            is_equal[idx] = false;
                        }
                    }
                }
                ColumnValues::Float64(inner_col) => {
                    for (idx, (left_idx, right_idx)) in equalities.iter().enumerate() {
                        if is_equal[idx]
                            && inner_col.values[*left_idx] != inner_col.values[*right_idx]
                        {
                            is_equal[idx] = false;
                        }
                    }
                }
            }
        }

        return is_equal;
    }

    pub fn batch_aggregate(
        column: &Column,
        aggregation_type: &AggregationType,
        groups: &[Group],
        row_indexes: &[usize],
    ) -> Column {
        return match aggregation_type {
            AggregationType::Sum => match &column.values {
                ColumnValues::Float64(values) => {
                    let mut out_nulls = Vec::<bool>::with_capacity(groups.len());
                    let mut out_values = Vec::<f64>::with_capacity(groups.len());
                    for group in groups {
                        let mut has_non_null = false;
                        let mut val: f64 = 0.0;
                        for group_row_idx in group.start_idx..(group.start_idx + group.len) {
                            let row_idx = row_indexes[group_row_idx];
                            if !column.nulls[row_idx] {
                                has_non_null = true;
                                val += values.values[row_idx];
                            }
                        }
                        out_nulls.push(!has_non_null);
                        out_values.push(val);
                    }

                    Column {
                        nulls: out_nulls,
                        values: ColumnValues::Float64(Float64ColumnValues { values: out_values }),
                    }
                }
                _ => {
                    panic!("Unsupported SUM agg for this col type");
                }
            },
            AggregationType::First => match &column.values {
                ColumnValues::Float64(values) => {
                    let mut out_nulls = Vec::<bool>::with_capacity(groups.len());
                    let mut out_values = Vec::<f64>::with_capacity(groups.len());
                    for group in groups {
                        let first_row_idx = row_indexes[group.start_idx];
                        out_nulls.push(column.nulls[first_row_idx]);
                        out_values.push(values.values[first_row_idx]);
                    }

                    Column {
                        nulls: out_nulls,
                        values: ColumnValues::Float64(Float64ColumnValues { values: out_values }),
                    }
                }
                _ => {
                    panic!("Unsupported FIRST agg for this col type");
                }
            },
        };
    }

    pub fn from_string_list(col_type: &str, values: &[Option<String>]) -> Column {
        return match col_type {
            "text" => {
                let mut new_nulls = Vec::<bool>::with_capacity(values.len());
                let mut new_values = Vec::<String>::with_capacity(values.len());
                for value in values {
                    match value {
                        None => {
                            new_nulls.push(true);
                            new_values.push("".to_string());
                        }
                        Some(val_str) => {
                            new_nulls.push(false);
                            new_values.push(val_str.clone());
                        }
                    }
                }

                Column {
                    nulls: new_nulls,
                    values: ColumnValues::Text(TextColumnValues { values: new_values }),
                }
            }
            "float64" => {
                let mut new_nulls = Vec::<bool>::with_capacity(values.len());
                let mut new_values = Vec::<f64>::with_capacity(values.len());
                for value in values {
                    match value {
                        None => {
                            new_nulls.push(true);
                            new_values.push(0.0);
                        }
                        Some(val_str) => match val_str.parse::<f64>() {
                            Ok(val_float) => {
                                new_nulls.push(false);
                                new_values.push(val_float);
                            }
                            Err(_) => {
                                new_nulls.push(true);
                                new_values.push(0.0);
                            }
                        },
                    }
                }

                Column {
                    nulls: new_nulls,
                    values: ColumnValues::Float64(Float64ColumnValues { values: new_values }),
                }
            }
            _ => {
                panic!("Unable to parse column");
            }
        };
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        if self.get_n_rows() != other.get_n_rows() {
            return false;
        }

        match &self.values {
            ColumnValues::Text(text_col) => {
                match &other.values {
                    ColumnValues::Text(other_text_col) => {
                        for ((left_null, left_val), (right_null, right_val)) in zip(
                            zip(&self.nulls, &text_col.values),
                            zip(&other.nulls, &other_text_col.values),
                        ) {
                            if left_null != right_null {
                                return false;
                            }
                            if !left_null && left_val != right_val {
                                return false;
                            }
                        }
                        return true;
                    }
                    _ => {
                        return false;
                    }
                };
            }
            ColumnValues::Float64(float_col) => match &other.values {
                ColumnValues::Float64(other_float_col) => {
                    for ((left_null, left_val), (right_null, right_val)) in zip(
                        zip(&self.nulls, &float_col.values),
                        zip(&other.nulls, &other_float_col.values),
                    ) {
                        if left_null != right_null {
                            return false;
                        }
                        if !left_null && left_val != right_val {
                            return false;
                        }
                    }
                    return true;
                }
                _ => {
                    return false;
                }
            },
        }
    }
}

#[derive(Debug)]
pub enum ColumnValues {
    Text(TextColumnValues),
    Float64(Float64ColumnValues),
}

#[derive(Debug)]
pub struct TextColumnValues {
    pub values: Vec<String>,
}

#[derive(Debug)]
pub struct Float64ColumnValues {
    pub values: Vec<f64>,
}

pub enum AggregationType {
    Sum,
    First,
}

pub struct Group {
    pub start_idx: usize,
    pub len: usize,
}
