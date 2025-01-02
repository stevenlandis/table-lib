use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::rc::Rc;
use std::{hash::DefaultHasher, iter::zip};

use crate::partition::{Partition, PartitionBuilder};
use crate::str_vec::StringVec;

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
    pub fn from_repeated_f64(val: f64, len: usize) -> Column {
        Column {
            col: Rc::new(InnerColumn {
                nulls: BitVec::from_repeated_value(false, len),
                values: ColumnValues::Float64(Float64ColumnValues {
                    values: vec![val; len],
                }),
            }),
        }
    }

    pub fn from_repeated_bool(val: bool, len: usize) -> Column {
        Column {
            col: Rc::new(InnerColumn {
                nulls: BitVec::from_repeated_value(false, len),
                values: ColumnValues::Bool(BoolColumnValues {
                    values: BitVec::from_repeated_value(val, len),
                }),
            }),
        }
    }

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

    pub fn get_type(&self) -> ColType {
        self.col.get_type()
    }

    pub fn to_string_list(&self) -> Vec<Option<String>> {
        self.col.to_string_list()
    }

    pub fn get_col_hashes(n_rows: usize, columns: &[Column]) -> Vec<u64> {
        let mut row_hashers = (0..n_rows)
            .map(|_| DefaultHasher::new())
            .collect::<Vec<_>>();

        for col in columns {
            col.col
                .update_col_hashes(row_hashers.iter_mut().enumerate());
        }

        let row_hashes = row_hashers
            .iter()
            .map(|hasher| hasher.finish())
            .collect::<Vec<_>>();

        return row_hashes;
    }

    pub fn hash_at_index<H: Hasher>(&self, state: &mut H, idx: usize) {
        return self.col.hash_at_index(state, idx);
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

    pub fn aggregate(
        &self,
        aggregation_type: &AggregationType,
        groups: &[Group],
        row_indexes: &[usize],
    ) -> Column {
        Column {
            col: Rc::new(self.col.aggregate(aggregation_type, groups, row_indexes)),
        }
    }

    pub fn aggregate_partition(
        &self,
        partition: &Partition,
        aggregation_type: &AggregationType,
    ) -> Column {
        Column {
            col: Rc::new(self.col.aggregate_partition(partition, aggregation_type)),
        }
    }

    pub fn aggregate_count(partition: &Partition) -> Column {
        // Since COUNT() doesn't need a column, this aggregation is a
        // static method
        let counts = partition
            .iter()
            .map(|span| span.len() as f64)
            .collect::<Vec<_>>();

        Column {
            col: Rc::new(InnerColumn {
                nulls: BitVec::from_repeated_value(false, counts.len()),
                values: ColumnValues::Float64(Float64ColumnValues { values: counts }),
            }),
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

    pub fn repeat_scalar_col(&self, len: usize) -> Column {
        assert_eq!(self.len(), 1);

        Column {
            col: Rc::new(InnerColumn {
                nulls: BitVec::from_repeated_value(self.col.nulls.at(0), len),
                values: match &self.col.values {
                    ColumnValues::Bool(inner_col) => ColumnValues::Bool(BoolColumnValues {
                        values: BitVec::from_repeated_value(inner_col.values.at(0), len),
                    }),
                    ColumnValues::Float64(inner_col) => {
                        ColumnValues::Float64(Float64ColumnValues {
                            values: vec![inner_col.values[0]; len],
                        })
                    }
                    ColumnValues::Text(inner_col) => ColumnValues::Text(
                        TextColumnValues::from_repeated_value(inner_col.get_str_at_idx(0), len),
                    ),
                },
            }),
        }
    }

    pub fn repeat_with_partition(
        &self,
        partition: &Partition,
        target_partition: &Partition,
    ) -> Column {
        // Intended for cases like
        // from tbl group by foo get bar + sum(bar)
        // to repeat sum(bar) so it matches bar
        assert_eq!(partition.n_spans(), target_partition.n_spans());
        let mut indexes = Vec::<usize>::new();
        for (span, target_span) in std::iter::zip(partition, target_partition) {
            assert_eq!(span.len(), 1);
            let idx = span.start;
            for _ in target_span {
                indexes.push(idx);
            }
        }

        self.from_indexes(&indexes)
    }

    pub fn spread_scalar(&self, from_partition: &Partition, to_partition: &Partition) -> Column {
        assert_eq!(self.len(), from_partition.n_spans());
        assert_eq!(from_partition.n_rows(), to_partition.n_rows());

        let mut indexes = Vec::<usize>::with_capacity(to_partition.n_spans());

        let mut to_span_idx: usize = 0;
        let to_spans = to_partition.spans();
        for (from_span_idx, from_span) in from_partition.spans().iter().enumerate() {
            let target_len = from_span.len;
            let mut len: usize = 0;
            while len < target_len {
                len += to_spans[to_span_idx].len;
                to_span_idx += 1;

                indexes.push(from_span_idx);
            }
            assert_eq!(len, target_len);
        }

        assert_eq!(indexes.len(), to_partition.n_spans());

        self.from_indexes(&indexes)
    }

    pub fn group_by(group_cols: &[Column], partition: &Partition) -> (Partition, Vec<usize>) {
        let inner_group_cols = group_cols
            .iter()
            .map(|col| col.col.as_ref())
            .collect::<Vec<_>>();

        InnerColumn::group_by(&inner_group_cols, partition)
    }

    pub fn from_str_vec(nulls: BitVec, values: StringVec) -> Column {
        Column {
            col: Rc::new(InnerColumn {
                nulls,
                values: ColumnValues::Text(TextColumnValues { values }),
            }),
        }
    }

    pub fn serialize(&self) -> StringVec {
        let nulls = &self.col.nulls;
        let mut result = StringVec::new();
        match &self.col.values {
            ColumnValues::Text(values) => {
                for (is_null, val) in zip(nulls, &values.values) {
                    if is_null {
                        result.push("");
                    } else {
                        result.push(val);
                    }
                }
            }
            ColumnValues::Float64(values) => {
                let mut writer = result.get_writer();
                for (is_null, val) in zip(nulls, &values.values) {
                    if is_null {
                        writer.write_str("null");
                    } else {
                        write!(writer, "{}", val).unwrap();
                    }
                    writer.finish_value();
                }
            }
            ColumnValues::Bool(values) => {
                let mut writer = result.get_writer();
                for (is_null, val) in zip(nulls, &values.values) {
                    if is_null {
                        writer.write_str("null");
                    } else {
                        write!(writer, "{}", val).unwrap();
                    }
                    writer.finish_value();
                }
            }
        }

        result
    }

    fn cmp_indexes(&self, idx0: usize, idx1: usize) -> std::cmp::Ordering {
        match &self.col.values {
            ColumnValues::Text(values) => {
                values.get_str_at_idx(idx0).cmp(values.get_str_at_idx(idx1))
            }
            ColumnValues::Float64(values) => values.values[idx0].total_cmp(&values.values[idx1]),
            ColumnValues::Bool(values) => values.values.at(idx0).cmp(&values.values.at(idx1)),
        }
    }

    pub fn get_sorted_indexes(
        cols: &[Column],
        directions: &[SortOrderDirection],
        n_rows: usize,
        partition: &Partition,
    ) -> Vec<usize> {
        assert!(cols.len() >= 1);

        struct SortKey<'a> {
            row_idx: usize,
            cols: &'a [Column],
            directions: &'a [SortOrderDirection],
        }

        impl<'a> SortKey<'a> {
            fn base_cmp(&self, other: &Self) -> std::cmp::Ordering {
                for (col, direction) in zip(self.cols, self.directions) {
                    let order = col.cmp_indexes(self.row_idx, other.row_idx);
                    match order {
                        std::cmp::Ordering::Equal => {}
                        std::cmp::Ordering::Less => {
                            return match direction {
                                SortOrderDirection::Ascending => std::cmp::Ordering::Less,
                                SortOrderDirection::Descending => std::cmp::Ordering::Greater,
                            }
                        }
                        std::cmp::Ordering::Greater => {
                            return match direction {
                                SortOrderDirection::Ascending => std::cmp::Ordering::Greater,
                                SortOrderDirection::Descending => std::cmp::Ordering::Less,
                            }
                        }
                    }
                }

                std::cmp::Ordering::Equal
            }
        }

        impl<'a> PartialEq for SortKey<'a> {
            fn eq(&self, other: &Self) -> bool {
                self.base_cmp(other) == std::cmp::Ordering::Equal
            }
        }

        impl<'a> Eq for SortKey<'a> {}

        impl<'a> PartialOrd for SortKey<'a> {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.base_cmp(other))
            }
        }

        impl<'a> Ord for SortKey<'a> {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.base_cmp(other)
            }
        }

        let mut result = Vec::<usize>::with_capacity(n_rows);

        // sort each partition separately
        for span in partition {
            result.extend(span.clone());

            let slice_to_sort = &mut result[span];
            slice_to_sort.sort_by_key(|row_idx| SortKey {
                row_idx: *row_idx,
                cols,
                directions,
            });
        }

        result
    }

    pub fn limit(&self, limit: usize, partition: &Partition) -> Column {
        let mut nulls = BitVec::new();
        let values = match &self.col.values {
            ColumnValues::Text(inner_col) => {
                let mut values = StringVec::new();
                for span in partition {
                    for row_idx in span.take(limit) {
                        nulls.push(self.col.nulls.at(row_idx));
                        values.push(inner_col.get_str_at_idx(row_idx));
                    }
                }

                ColumnValues::Text(TextColumnValues { values })
            }
            ColumnValues::Float64(inner_col) => {
                let mut values = Vec::<f64>::new();
                for span in partition {
                    for row_idx in span.take(limit) {
                        nulls.push(self.col.nulls.at(row_idx));
                        values.push(inner_col.values[row_idx]);
                    }
                }

                ColumnValues::Float64(Float64ColumnValues { values })
            }
            ColumnValues::Bool(inner_col) => {
                let mut values = BitVec::new();
                for span in partition {
                    for row_idx in span.take(limit) {
                        nulls.push(self.col.nulls.at(row_idx));
                        values.push(inner_col.values.at(row_idx));
                    }
                }

                ColumnValues::Bool(BoolColumnValues { values })
            }
        };

        Column {
            col: Rc::new(InnerColumn { nulls, values }),
        }
    }

    pub fn infer_col_type(&self) -> Column {
        match &self.col.values {
            ColumnValues::Text(values) => {
                let mut guess: Option<ColType> = None;

                for (is_null, value) in zip(&self.col.nulls, &values.values) {
                    if is_null {
                        continue;
                    }

                    let this_guess = if value == "true" || value == "false" {
                        ColType::Bool
                    } else if value.parse::<f64>().is_ok() {
                        ColType::Float64
                    } else {
                        ColType::Text
                    };

                    match guess {
                        None => {
                            guess = Some(this_guess);
                        }
                        Some(inner_guess) => match this_guess {
                            ColType::Text => {
                                guess = Some(ColType::Text);
                            }
                            ColType::Bool => match inner_guess {
                                ColType::Bool => {}
                                _ => {
                                    guess = Some(ColType::Text);
                                }
                            },
                            ColType::Float64 => match inner_guess {
                                ColType::Float64 => {}
                                _ => {
                                    guess = Some(ColType::Text);
                                }
                            },
                        },
                    }
                }

                let guess = guess.unwrap_or(ColType::Text);

                match guess {
                    ColType::Text => self.clone(),
                    ColType::Bool => {
                        let mut out_values = BitVec::with_capacity(self.len());
                        for (is_null, value) in zip(&self.col.nulls, &values.values) {
                            if is_null {
                                out_values.push(false);
                            } else {
                                out_values.push(match value {
                                    "false" => false,
                                    "true" => true,
                                    _ => panic!(),
                                });
                            }
                        }
                        Column {
                            col: Rc::new(InnerColumn {
                                nulls: self.col.nulls.clone(),
                                values: ColumnValues::Bool(BoolColumnValues { values: out_values }),
                            }),
                        }
                    }
                    ColType::Float64 => {
                        let mut out_values = Vec::<f64>::with_capacity(self.len());
                        for (is_null, value) in zip(&self.col.nulls, &values.values) {
                            if is_null {
                                out_values.push(0.0);
                            } else {
                                out_values.push(value.parse::<f64>().unwrap());
                            }
                        }
                        Column {
                            col: Rc::new(InnerColumn {
                                nulls: self.col.nulls.clone(),
                                values: ColumnValues::Float64(Float64ColumnValues {
                                    values: out_values,
                                }),
                            }),
                        }
                    }
                }
            }
            _ => self.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum SortOrderDirection {
    Ascending,
    Descending,
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.col.eq(&other.col)
    }
}

impl std::ops::Add for &Column {
    type Output = Column;

    fn add(self, rhs: Self) -> Self::Output {
        Column {
            col: Rc::new(self.col.add(&rhs.col)),
        }
    }
}

impl std::ops::Div for &Column {
    type Output = Column;

    fn div(self, rhs: Self) -> Self::Output {
        Column {
            col: Rc::new(self.col.div(&rhs.col)),
        }
    }
}

#[derive(Debug)]
struct InnerColumn {
    pub nulls: BitVec,
    pub values: ColumnValues,
}

#[derive(Clone, Copy, Debug)]
pub enum ColType {
    Text,
    Bool,
    Float64,
}

impl ColType {
    pub fn to_string(&self) -> String {
        match self {
            ColType::Text => "text".to_string(),
            ColType::Float64 => "float64".to_string(),
            ColType::Bool => "bool".to_string(),
        }
    }
}

impl InnerColumn {
    pub fn len(&self) -> usize {
        return match &self.values {
            ColumnValues::Text(col) => col.values.len(),
            ColumnValues::Float64(col) => col.values.len(),
            ColumnValues::Bool(col) => col.values.len(),
        };
    }

    pub fn get_type(&self) -> ColType {
        match &self.values {
            ColumnValues::Text(_) => ColType::Text,
            ColumnValues::Float64(_) => ColType::Float64,
            ColumnValues::Bool(_) => ColType::Bool,
        }
    }

    pub fn get_type_name(&self) -> String {
        self.get_type().to_string()
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

    pub fn update_col_hashes<'a, H: Hasher + 'a>(
        &self,
        row_hashers: impl IntoIterator<Item = (usize, &'a mut H)>,
    ) {
        match &self.values {
            ColumnValues::Text(text_col) => {
                for (idx, hasher) in row_hashers {
                    if self.nulls.at(idx) {
                        0.hash(hasher);
                    } else {
                        text_col.get_str_at_idx(idx).hash(hasher);
                    }
                }
            }
            ColumnValues::Float64(float_col) => {
                for (idx, hasher) in row_hashers {
                    if self.nulls.at(idx) {
                        0.hash(hasher);
                    } else {
                        (float_col.values[idx] as u64).hash(hasher);
                    }
                }
            }
            ColumnValues::Bool(bool_col) => {
                for (idx, hasher) in row_hashers {
                    if self.nulls.at(idx) {
                        0.hash(hasher);
                    } else {
                        bool_col.values.at(idx).hash(hasher);
                    }
                }
            }
        }
    }

    pub fn hash_at_index<H: Hasher>(&self, state: &mut H, idx: usize) {
        let slice = [(idx, state)];
        self.update_col_hashes(slice);
    }

    fn eq_at_indexes(&self, idx0: usize, idx1: usize) -> bool {
        if self.nulls.at(idx0) || self.nulls.at(idx1) {
            return self.nulls.at(idx0) && self.nulls.at(idx1);
        }

        match &self.values {
            ColumnValues::Text(inner_col) => {
                inner_col.get_str_at_idx(idx0) == inner_col.get_str_at_idx(idx1)
            }
            ColumnValues::Float64(inner_col) => inner_col.values[idx0] == inner_col.values[idx1],
            ColumnValues::Bool(inner_col) => inner_col.values.at(idx0) == inner_col.values.at(idx1),
        }
    }

    pub fn batch_equals(&self, equalities: &[(usize, usize)], is_equal: &mut Vec<bool>) {
        // DEPRECATE THIS
        match &self.values {
            ColumnValues::Text(inner_col) => {
                for (idx, (left_idx, right_idx)) in equalities.iter().enumerate() {
                    if is_equal[idx] && inner_col.values[*left_idx] != inner_col.values[*right_idx]
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

    pub fn aggregate_partition(
        &self,
        partition: &Partition,
        aggregation_type: &AggregationType,
    ) -> InnerColumn {
        return match aggregation_type {
            AggregationType::Sum => match &self.values {
                ColumnValues::Float64(values) => {
                    let mut out_nulls = BitVec::new();
                    let mut out_values = Vec::<f64>::with_capacity(partition.n_spans());
                    for span in partition {
                        let mut val: f64 = 0.0;
                        for row_idx in span {
                            if !self.nulls.at(row_idx) {
                                val += values.values[row_idx];
                            }
                        }
                        out_nulls.push(false);
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
                    let mut out_values = Vec::<f64>::with_capacity(partition.n_spans());
                    for span in partition {
                        let first_row_idx = span.into_iter().nth(0).unwrap();
                        out_nulls.push(self.nulls.at(first_row_idx));
                        out_values.push(values.values[first_row_idx]);
                    }

                    InnerColumn {
                        nulls: out_nulls,
                        values: ColumnValues::Float64(Float64ColumnValues { values: out_values }),
                    }
                }
                ColumnValues::Text(values) => {
                    let mut builder = TextColBuilder::new(partition.n_spans());

                    for span in partition {
                        let first_row_idx = span.into_iter().nth(0).unwrap();
                        if self.nulls.at(first_row_idx) {
                            builder.add_null();
                        } else {
                            builder.add_value(values.get_str_at_idx(first_row_idx));
                        }
                    }

                    builder.to_col()
                }
                _ => {
                    panic!("Unsupported FIRST agg for this col type");
                }
            },
        };
    }

    pub fn aggregate(
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
        assert_eq!(self.len(), other.len());
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

    pub fn div(&self, other: &InnerColumn) -> InnerColumn {
        assert_eq!(self.len(), other.len());
        match &self.values {
            ColumnValues::Float64(inner_col) => match &other.values {
                ColumnValues::Float64(other_inner_col) => {
                    let mut nulls = BitVec::with_capacity(self.len());
                    let mut values = Vec::<f64>::with_capacity(self.len());

                    for ((left_null, left_val), (right_null, right_val)) in zip(
                        zip(&self.nulls, inner_col.values.iter().cloned()),
                        zip(&other.nulls, other_inner_col.values.iter().cloned()),
                    ) {
                        let is_null = left_null || right_null || right_val == 0.0;
                        let value = if is_null { 0.0 } else { left_val / right_val };
                        nulls.push(is_null);
                        values.push(value);
                    }

                    InnerColumn {
                        nulls,
                        values: ColumnValues::Float64(Float64ColumnValues { values }),
                    }
                }
                _ => {
                    panic!("unsupported");
                }
            },
            _ => {
                panic!("unsupported");
            }
        }
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

    fn group_by(group_cols: &[&InnerColumn], partition: &Partition) -> (Partition, Vec<usize>) {
        struct HashKey<'a> {
            row_idx: usize,
            columns: &'a [&'a InnerColumn],
        }
        impl Eq for HashKey<'_> {}
        impl PartialEq for HashKey<'_> {
            fn eq(&self, other: &Self) -> bool {
                return self
                    .columns
                    .iter()
                    .all(|col| col.eq_at_indexes(self.row_idx, other.row_idx));
            }
        }
        impl Hash for HashKey<'_> {
            fn hash<H: Hasher>(&self, state: &mut H) {
                for col in self.columns {
                    col.hash_at_index(state, self.row_idx);
                }
            }
        }

        let mut part_builder = PartitionBuilder::new();
        let mut row_indexes = Vec::<usize>::new();

        for span in partition {
            // TODO: Move out of loop to minimize allocations
            let mut hash_to_group_idx = HashMap::<HashKey, usize>::new();
            let mut group_first_indexes = Vec::<usize>::new();
            let mut group_last_indexes = Vec::<usize>::new();
            // let mut external_indexes = Vec::<usize>::new();
            let mut next_indexes = vec![usize::MAX; span.len()];

            for (inner_idx, idx) in span.clone().enumerate() {
                // external_indexes.push(*outer_idx);
                match hash_to_group_idx.entry(HashKey {
                    row_idx: idx,
                    columns: group_cols,
                }) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let group_idx = group_first_indexes.len();
                        entry.insert(group_idx);
                        group_first_indexes.push(inner_idx);
                        group_last_indexes.push(inner_idx);
                    }
                    std::collections::hash_map::Entry::Occupied(entry) => {
                        let group_idx = *entry.get();
                        let last_idx = &mut group_last_indexes[group_idx];
                        next_indexes[*last_idx] = inner_idx;
                        *last_idx = inner_idx;
                    }
                }
            }

            for start_idx in group_first_indexes.iter().cloned() {
                let mut idx = start_idx;
                let mut span_len: usize = 0;
                while idx != usize::MAX {
                    row_indexes.push(span.start + idx);
                    idx = next_indexes[idx];
                    span_len += 1;
                }
                part_builder.add_span(span_len);
            }
            // part_lens.push(group_first_indexes.len());
        }

        (part_builder.to_partition(), row_indexes)
        // (part_builder.to_partition(), part_lens)
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
    values: StringVec,
}

impl TextColumnValues {
    fn get_str_at_idx(&self, idx: usize) -> &str {
        &self.values[idx]
    }

    fn from_indexes(&self, indexes: &Vec<usize>) -> TextColumnValues {
        let mut values = StringVec::new();

        for idx in indexes {
            values.push(&self.values[*idx]);
        }

        TextColumnValues { values }
    }

    fn from_repeated_value(value: &str, len: usize) -> TextColumnValues {
        let mut values = StringVec::new();

        for _ in 0..len {
            values.push(value);
        }

        TextColumnValues { values }
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

#[derive(Debug, PartialEq, Hash, Clone, Copy)]
pub enum AggregationType {
    Sum,
    First,
}

pub struct Group {
    pub start_idx: usize,
    pub len: usize,
}

struct TextColBuilder {
    nulls: BitVec,
    values: StringVec,
}

impl TextColBuilder {
    fn new(_len: usize) -> Self {
        TextColBuilder {
            nulls: BitVec::new(),
            values: StringVec::new(),
        }
    }

    fn add_value(&mut self, value: &str) {
        self.nulls.push(false);
        self.values.push(value);
    }

    fn add_null(&mut self) {
        self.nulls.push(true);
        self.values.push("");
    }

    fn to_col(self) -> InnerColumn {
        return InnerColumn {
            nulls: self.nulls,
            values: ColumnValues::Text(TextColumnValues {
                values: self.values,
            }),
        };
    }
}
