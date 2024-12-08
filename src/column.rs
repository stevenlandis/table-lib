use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::{hash::DefaultHasher, iter::zip};

use super::bit_vec::BitVec;

#[derive(Clone)]
pub struct Column {
    col: Rc<InnerColumn>,
}

impl core::fmt::Debug for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.col.fmt(f)
    }
}

impl Column {
    pub fn get_true_indexes(&self) -> Vec<usize> {
        self.col.get_true_indexes()
    }

    pub fn from_indexes(&self, indexes: &Vec<usize>) -> Column {
        Column {
            col: Rc::new(self.col.from_indexes(indexes)),
        }
    }

    pub fn len(&self) -> usize {
        self.col.len()
    }

    pub fn from_string_list(col_type: &str, values: &[Option<String>]) -> Column {
        Column {
            col: Rc::new(InnerColumn::from_string_list(col_type, values)),
        }
    }

    pub fn get_type_name(&self) -> String {
        self.col.get_type_name()
    }

    pub fn to_string_list(&self) -> Vec<Option<String>> {
        self.col.to_string_list()
    }

    pub fn get_col_hashes(n_rows: usize, columns: &[Column]) -> Vec<u64> {
        let mut row_hashers = (0..n_rows)
            .map(|_| DefaultHasher::new())
            .collect::<Vec<_>>();

        for col in columns {
            col.col.update_col_hashes(&mut row_hashers);
        }

        let row_hashes = row_hashers
            .iter()
            .map(|hasher| hasher.finish())
            .collect::<Vec<_>>();

        return row_hashes;
    }

    pub fn batch_equals(columns: &[Column], equalities: &[(usize, usize)]) -> Vec<bool> {
        let mut is_equal = (0..equalities.len()).map(|_| true).collect::<Vec<_>>();

        for col in columns {
            col.col.batch_equals(equalities, &mut is_equal);
        }

        return is_equal;
    }

    pub fn get_new_col_from_indexes(&self, indexes: &[usize]) -> Column {
        Column {
            col: Rc::new(self.col.get_new_col_from_indexes(indexes)),
        }
    }

    pub fn get_new_col_from_opt_indexes(&self, indexes: &[Option<usize>]) -> Column {
        Column {
            col: Rc::new(self.col.get_new_col_from_opt_indexes(indexes)),
        }
    }

    pub fn batch_aggregate(
        &self,
        aggregation_type: &AggregationType,
        groups: &[Group],
        row_indexes: &[usize],
    ) -> Column {
        Column {
            col: Rc::new(
                self.col
                    .batch_aggregate(aggregation_type, groups, row_indexes),
            ),
        }
    }

    pub fn is_equal_at_index(&self, other: &Self, left_idx: usize, right_idx: usize) -> bool {
        self.col.is_equal_at_index(&other.col, left_idx, right_idx)
    }

    pub fn add_f64(&self, val: f64) -> Column {
        Column {
            col: Rc::new(self.col.add_f64(val)),
        }
    }

    pub fn less_than(&self, other: &Column) -> Column {
        Column {
            col: Rc::new(self.col.less_than(&other.col)),
        }
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.col.eq(&other.col)
    }
}

impl std::ops::Add for Column {
    type Output = Column;

    fn add(self, rhs: Self) -> Self::Output {
        Column {
            col: Rc::new(self.col.add(&rhs.col)),
        }
    }
}

#[derive(Debug)]
struct InnerColumn {
    pub nulls: BitVec,
    pub values: ColumnValues,
}

impl InnerColumn {
    pub fn len(&self) -> usize {
        return match &self.values {
            ColumnValues::Text(col) => col.records.len(),
            ColumnValues::Float64(col) => col.values.len(),
            ColumnValues::Bool(col) => col.values.len(),
        };
    }

    pub fn get_type_name(&self) -> String {
        match &self.values {
            ColumnValues::Text(_) => "text".to_string(),
            ColumnValues::Float64(_) => "float64".to_string(),
            ColumnValues::Bool(_) => "bool".to_string(),
        }
    }

    // pub fn eq_at_indexes(&self, left_idx: usize, right_idx: usize) -> bool {
    //     if self.nulls.at(left_idx) != self.nulls.at(right_idx) {
    //         return false;
    //     }
    //     if self.nulls.at(left_idx) {
    //         return true;
    //     }

    //     match &self.values {
    //         ColumnValues::Text(inner_col) => {
    //             return inner_col.records[left_idx].start_idx
    //                 == inner_col.records[right_idx].start_idx;
    //         }
    //         ColumnValues::Float64(inner_col) => {
    //             return inner_col.values[left_idx] == inner_col.values[right_idx];
    //         }
    //         ColumnValues::Bool(inner_col) => {
    //             return inner_col.values.at(left_idx) == inner_col.values.at(right_idx);
    //         }
    //     }
    // }

    pub fn get_true_indexes(&self) -> Vec<usize> {
        let nulls = &self.nulls;
        let vals = match &self.values {
            ColumnValues::Bool(inner_col) => &inner_col.values,
            _ => todo!(),
        };

        zip(nulls, vals)
            .enumerate()
            .filter(|(_, (null, val))| !null && *val)
            .map(|(idx, _)| idx)
            .collect::<Vec<_>>()
    }

    pub fn from_indexes(&self, indexes: &Vec<usize>) -> InnerColumn {
        InnerColumn {
            nulls: indexes
                .iter()
                .map(|idx| self.nulls.at(*idx))
                .collect::<BitVec>(),
            values: match &self.values {
                ColumnValues::Text(text_col) => ColumnValues::Text(text_col.from_indexes(indexes)),
                ColumnValues::Float64(float_col) => ColumnValues::Float64(Float64ColumnValues {
                    values: indexes.iter().map(|idx| float_col.values[*idx]).collect(),
                }),
                ColumnValues::Bool(bool_col) => ColumnValues::Bool(BoolColumnValues {
                    values: bool_col.values.from_indexes(indexes),
                }),
            },
        }
    }

    pub fn is_equal_at_index(&self, other: &Self, left_idx: usize, right_idx: usize) -> bool {
        if self.nulls.at(left_idx) != other.nulls.at(right_idx) {
            return false;
        }
        if self.nulls.at(left_idx) {
            return true;
        }

        return match &self.values {
            ColumnValues::Text(inner_col) => match &other.values {
                ColumnValues::Text(other_inner_col) => {
                    inner_col.get_str_at_idx(left_idx) == other_inner_col.get_str_at_idx(right_idx)
                }
                _ => false,
            },
            ColumnValues::Float64(inner_col) => match &other.values {
                ColumnValues::Float64(other_inner_col) => {
                    inner_col.values[left_idx] == other_inner_col.values[right_idx]
                }
                _ => false,
            },
            ColumnValues::Bool(inner_col) => match &other.values {
                ColumnValues::Bool(other_inner_col) => {
                    inner_col.values.at(left_idx) == other_inner_col.values.at(right_idx)
                }
                _ => false,
            },
        };
    }

    pub fn get_new_col_from_indexes(&self, indexes: &[usize]) -> InnerColumn {
        match &self.values {
            ColumnValues::Text(inner_col) => {
                let mut builder = TextColBuilder::new(indexes.len());
                for idx in indexes {
                    if self.nulls.at(*idx) {
                        builder.add_null();
                    } else {
                        builder.add_value(inner_col.get_str_at_idx(*idx));
                    }
                }

                builder.to_col()
            }
            ColumnValues::Float64(inner_col) => InnerColumn {
                nulls: indexes.iter().map(|idx| self.nulls.at(*idx)).collect(),
                values: ColumnValues::Float64(Float64ColumnValues {
                    values: indexes.iter().map(|idx| inner_col.values[*idx]).collect(),
                }),
            },
            ColumnValues::Bool(inner_col) => InnerColumn {
                nulls: indexes.iter().map(|idx| self.nulls.at(*idx)).collect(),
                values: ColumnValues::Bool(BoolColumnValues {
                    values: indexes
                        .iter()
                        .map(|idx| inner_col.values.at(*idx))
                        .collect(),
                }),
            },
        }
    }

    pub fn get_new_col_from_opt_indexes(&self, indexes: &[Option<usize>]) -> InnerColumn {
        match &self.values {
            ColumnValues::Text(inner_col) => {
                let mut builder = TextColBuilder::new(indexes.len());
                for idx in indexes {
                    match idx {
                        None => {
                            builder.add_null();
                        }
                        Some(real_idx) => {
                            builder.add_value(inner_col.get_str_at_idx(*real_idx));
                        }
                    }
                }

                builder.to_col()
            }
            ColumnValues::Float64(inner_col) => InnerColumn {
                nulls: indexes
                    .iter()
                    .map(|idx_opt| match idx_opt {
                        None => true,
                        Some(idx) => self.nulls.at(*idx),
                    })
                    .collect(),
                values: ColumnValues::Float64(Float64ColumnValues {
                    values: indexes
                        .iter()
                        .map(|idx_opt| match idx_opt {
                            None => 0.0,
                            Some(idx) => inner_col.values[*idx],
                        })
                        .collect(),
                }),
            },
            ColumnValues::Bool(inner_col) => InnerColumn {
                nulls: indexes
                    .iter()
                    .map(|idx_opt| match idx_opt {
                        None => true,
                        Some(idx) => self.nulls.at(*idx),
                    })
                    .collect(),
                values: ColumnValues::Bool(BoolColumnValues {
                    values: indexes
                        .iter()
                        .map(|idx_opt| match idx_opt {
                            None => false,
                            Some(idx) => inner_col.values.at(*idx),
                        })
                        .collect(),
                }),
            },
        }
    }

    pub fn update_col_hashes(&self, row_hashers: &mut Vec<DefaultHasher>) {
        match &self.values {
            ColumnValues::Text(text_col) => {
                for (idx, (hasher, is_null)) in zip(row_hashers, &self.nulls).enumerate() {
                    if is_null {
                        0.hash(hasher);
                    } else {
                        text_col.get_str_at_idx(idx).hash(hasher);
                    }
                }
            }
            ColumnValues::Float64(float_col) => {
                for (hasher, (is_null, value)) in
                    zip(row_hashers, zip(&self.nulls, &float_col.values))
                {
                    if is_null {
                        0.hash(hasher);
                    } else {
                        (*value as u64).hash(hasher);
                    }
                }
            }
            ColumnValues::Bool(bool_col) => {
                for (hasher, (is_null, value)) in
                    zip(row_hashers, zip(&self.nulls, &bool_col.values))
                {
                    if is_null {
                        0.hash(hasher);
                    } else {
                        value.hash(hasher);
                    }
                }
            }
        }
    }

    pub fn batch_equals(&self, equalities: &[(usize, usize)], is_equal: &mut Vec<bool>) {
        match &self.values {
            ColumnValues::Text(inner_col) => {
                for (idx, (left_idx, right_idx)) in equalities.iter().enumerate() {
                    if is_equal[idx]
                        && inner_col.records[*left_idx].start_idx
                            != inner_col.records[*right_idx].start_idx
                    {
                        is_equal[idx] = false;
                    }
                }
            }
            ColumnValues::Float64(inner_col) => {
                for (idx, (left_idx, right_idx)) in equalities.iter().enumerate() {
                    if is_equal[idx] && inner_col.values[*left_idx] != inner_col.values[*right_idx]
                    {
                        is_equal[idx] = false;
                    }
                }
            }
            ColumnValues::Bool(inner_col) => {
                for (idx, (left_idx, right_idx)) in equalities.iter().enumerate() {
                    if is_equal[idx]
                        && inner_col.values.at(*left_idx) != inner_col.values.at(*right_idx)
                    {
                        is_equal[idx] = false;
                    }
                }
            }
        }
    }

    pub fn batch_aggregate(
        &self,
        aggregation_type: &AggregationType,
        groups: &[Group],
        row_indexes: &[usize],
    ) -> InnerColumn {
        return match aggregation_type {
            AggregationType::Sum => match &self.values {
                ColumnValues::Float64(values) => {
                    let mut out_nulls = BitVec::new();
                    let mut out_values = Vec::<f64>::with_capacity(groups.len());
                    for group in groups {
                        let mut has_non_null = false;
                        let mut val: f64 = 0.0;
                        for group_row_idx in group.start_idx..(group.start_idx + group.len) {
                            let row_idx = row_indexes[group_row_idx];
                            if !self.nulls.at(row_idx) {
                                has_non_null = true;
                                val += values.values[row_idx];
                            }
                        }
                        out_nulls.push(!has_non_null);
                        out_values.push(val);
                    }

                    InnerColumn {
                        nulls: out_nulls,
                        values: ColumnValues::Float64(Float64ColumnValues { values: out_values }),
                    }
                }
                _ => {
                    panic!("Unsupported SUM agg for this col type");
                }
            },
            AggregationType::First => match &self.values {
                ColumnValues::Float64(values) => {
                    let mut out_nulls = BitVec::new();
                    let mut out_values = Vec::<f64>::with_capacity(groups.len());
                    for group in groups {
                        let first_row_idx = row_indexes[group.start_idx];
                        out_nulls.push(self.nulls.at(first_row_idx));
                        out_values.push(values.values[first_row_idx]);
                    }

                    InnerColumn {
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

    pub fn from_string_list(col_type: &str, values: &[Option<String>]) -> InnerColumn {
        return match col_type {
            "text" => {
                let mut builder = TextColBuilder::new(values.len());

                for value in values {
                    match value {
                        None => {
                            builder.add_null();
                        }
                        Some(val_str) => {
                            builder.add_value(val_str);
                        }
                    }
                }

                builder.to_col()
            }
            "float64" => {
                let mut new_nulls = BitVec::new();
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

                InnerColumn {
                    nulls: new_nulls,
                    values: ColumnValues::Float64(Float64ColumnValues { values: new_values }),
                }
            }
            "bool" => {
                let mut new_nulls = BitVec::new();
                let mut new_values = BitVec::new();
                for value in values {
                    match value {
                        None => {
                            new_nulls.push(true);
                            new_values.push(false);
                        }
                        Some(val_str) => match val_str.as_str() {
                            "false" => {
                                new_nulls.push(false);
                                new_values.push(false);
                            }
                            "true" => {
                                new_nulls.push(false);
                                new_values.push(true);
                            }
                            _ => {
                                new_nulls.push(true);
                                new_values.push(false);
                            }
                        },
                    }
                }

                InnerColumn {
                    nulls: new_nulls,
                    values: ColumnValues::Bool(BoolColumnValues { values: new_values }),
                }
            }
            _ => {
                panic!("Unable to parse column");
            }
        };
    }

    pub fn to_string_list(&self) -> Vec<Option<String>> {
        return match &self.values {
            ColumnValues::Text(inner_col) => {
                let mut values = Vec::<Option<String>>::new();
                for (idx, is_null) in self.nulls.iter().enumerate() {
                    if is_null {
                        values.push(None);
                    } else {
                        values.push(Some(inner_col.get_str_at_idx(idx).to_string()));
                    }
                }

                values
            }
            ColumnValues::Float64(inner_col) => {
                let mut values = Vec::<Option<String>>::new();

                for (is_null, value) in zip(&self.nulls, &inner_col.values) {
                    if is_null {
                        values.push(None);
                    } else {
                        values.push(Some(value.to_string()));
                    }
                }

                values
            }
            ColumnValues::Bool(inner_col) => {
                let mut values = Vec::<Option<String>>::new();

                for (is_null, value) in zip(&self.nulls, &inner_col.values) {
                    if is_null {
                        values.push(None);
                    } else {
                        values.push(Some(if value {
                            "true".to_string()
                        } else {
                            "false".to_string()
                        }));
                    }
                }

                values
            }
        };
    }

    pub fn add(&self, other: &InnerColumn) -> InnerColumn {
        return match &self.values {
            ColumnValues::Float64(inner_col) => match &other.values {
                ColumnValues::Float64(other_inner_col) => InnerColumn {
                    nulls: zip(&self.nulls, &other.nulls)
                        .map(|(left, right)| left || right)
                        .collect(),
                    values: ColumnValues::Float64(Float64ColumnValues {
                        values: zip(&inner_col.values, &other_inner_col.values)
                            .map(|(left, right)| left + right)
                            .collect(),
                    }),
                },
                _ => {
                    panic!("unsupported");
                }
            },
            _ => {
                panic!("unsupported");
            }
        };
    }

    pub fn add_f64(&self, val: f64) -> InnerColumn {
        match &self.values {
            ColumnValues::Float64(inner_col) => InnerColumn {
                nulls: self.nulls.clone(),
                values: ColumnValues::Float64(Float64ColumnValues {
                    values: inner_col
                        .values
                        .iter()
                        .map(|col_val| col_val + val)
                        .collect(),
                }),
            },
            _ => todo!(),
        }
    }

    pub fn less_than(&self, other: &InnerColumn) -> InnerColumn {
        match &self.values {
            ColumnValues::Text(inner_col) => match &other.values {
                ColumnValues::Text(other_col) => {
                    let mut result_nulls = BitVec::with_capacity(self.len());
                    let mut result_vals = BitVec::with_capacity(self.len());
                    for idx in 0..self.len() {
                        let left_null = self.nulls.at(idx);
                        let right_null = other.nulls.at(idx);

                        if left_null || right_null {
                            result_nulls.push(true);
                            result_vals.push(false);
                        } else {
                            let left_val = inner_col.get_str_at_idx(idx);
                            let right_val = other_col.get_str_at_idx(idx);
                            result_nulls.push(false);
                            result_vals.push(left_val < right_val);
                        }
                    }

                    InnerColumn {
                        nulls: result_nulls,
                        values: ColumnValues::Bool(BoolColumnValues {
                            values: result_vals,
                        }),
                    }
                }
                _ => todo!(),
            },
            ColumnValues::Float64(inner_col) => match &other.values {
                ColumnValues::Float64(other_col) => {
                    let mut result_nulls = BitVec::with_capacity(self.len());
                    let mut result_vals = BitVec::with_capacity(self.len());
                    for idx in 0..self.len() {
                        let left_null = self.nulls.at(idx);
                        let right_null = other.nulls.at(idx);

                        if left_null || right_null {
                            result_nulls.push(true);
                            result_vals.push(false);
                        } else {
                            let left_val = inner_col.values[idx];
                            let right_val = other_col.values[idx];
                            result_nulls.push(false);
                            result_vals.push(left_val < right_val);
                        }
                    }

                    InnerColumn {
                        nulls: result_nulls,
                        values: ColumnValues::Bool(BoolColumnValues {
                            values: result_vals,
                        }),
                    }
                }
                _ => todo!(),
            },
            _ => todo!(),
        }
    }
}

impl PartialEq for InnerColumn {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        match &self.values {
            ColumnValues::Text(text_col) => {
                match &other.values {
                    ColumnValues::Text(other_text_col) => {
                        for (idx, (left_null, right_null)) in
                            zip(&self.nulls, &other.nulls).enumerate()
                        {
                            if left_null != right_null {
                                return false;
                            }
                            if !left_null
                                && text_col.get_str_at_idx(idx)
                                    != other_text_col.get_str_at_idx(idx)
                            {
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
            ColumnValues::Bool(bool_col) => match &other.values {
                ColumnValues::Bool(other_bool_col) => {
                    for ((left_null, left_val), (right_null, right_val)) in zip(
                        zip(&self.nulls, &bool_col.values),
                        zip(&other.nulls, &other_bool_col.values),
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
                _ => return false,
            },
        }
    }
}

#[derive(Debug)]
pub enum ColumnValues {
    Text(TextColumnValues),
    Float64(Float64ColumnValues),
    Bool(BoolColumnValues),
}

#[derive(Debug, Copy, Clone)]
pub struct TextColRecord {
    pub start_idx: usize,
    pub len: usize,
}

#[derive(Debug)]
pub struct TextColumnValues {
    pub base_data: Vec<u8>,
    pub records: Vec<TextColRecord>,
}

impl TextColumnValues {
    fn get_str_at_idx(&self, idx: usize) -> &str {
        let record = &self.records[idx];
        unsafe {
            return std::str::from_utf8_unchecked(
                &self.base_data[record.start_idx..record.start_idx + record.len],
            );
        }
    }

    fn from_indexes(&self, indexes: &Vec<usize>) -> TextColumnValues {
        let mut base_data = Vec::<u8>::new();
        let mut records = Vec::<TextColRecord>::with_capacity(indexes.len());

        for idx in indexes {
            let record = &self.records[*idx];
            records.push(TextColRecord {
                start_idx: base_data.len(),
                len: record.len,
            });
            base_data.extend_from_slice(
                &self.base_data[record.start_idx..record.start_idx + record.len],
            );
        }

        TextColumnValues { base_data, records }
    }
}

#[derive(Debug)]
pub struct Float64ColumnValues {
    pub values: Vec<f64>,
}

#[derive(Debug)]
pub struct BoolColumnValues {
    pub values: BitVec,
}

pub enum AggregationType {
    Sum,
    First,
}

pub struct Group {
    pub start_idx: usize,
    pub len: usize,
}

struct TextColBuilder<'a> {
    nulls: BitVec,
    base_data: Vec<u8>,
    records: Vec<TextColRecord>,
    val_map: HashMap<&'a str, TextColRecord>,
}

impl<'a> TextColBuilder<'a> {
    fn new(len: usize) -> Self {
        TextColBuilder {
            nulls: BitVec::new(),
            base_data: Vec::new(),
            records: Vec::with_capacity(len),
            val_map: HashMap::new(),
        }
    }

    fn add_value<'b: 'a>(&mut self, value: &'b str) {
        self.nulls.push(false);
        match self.val_map.get(value) {
            None => {
                let bytes = value.as_bytes();
                let record = TextColRecord {
                    start_idx: self.base_data.len(),
                    len: bytes.len(),
                };
                self.records.push(record);
                self.base_data.extend(bytes);
                self.val_map.insert(value, record.clone());
            }
            Some(val_record) => {
                self.records.push(*val_record);
            }
        }
    }

    fn add_null(&mut self) {
        self.nulls.push(true);
        self.records.push(TextColRecord {
            start_idx: 0,
            len: 0,
        });
    }

    fn to_col(self) -> InnerColumn {
        return InnerColumn {
            nulls: self.nulls,
            values: ColumnValues::Text(TextColumnValues {
                base_data: self.base_data,
                records: self.records,
            }),
        };
    }
}
