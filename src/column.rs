use std::iter::zip;

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
