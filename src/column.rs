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
        match &self.values {
            ColumnValues::Text(text_col) => {
                if self.nulls[left_idx] != self.nulls[right_idx] {
                    return false;
                }
                if self.nulls[left_idx] && text_col.values[left_idx] != text_col.values[right_idx] {
                    return false;
                }
                return true;
            }
            ColumnValues::Float64(text_col) => {
                if self.nulls[left_idx] != self.nulls[right_idx] {
                    return false;
                }
                if self.nulls[left_idx] && text_col.values[left_idx] != text_col.values[right_idx] {
                    return false;
                }
                return true;
            }
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
