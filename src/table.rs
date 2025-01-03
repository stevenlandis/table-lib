use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::{collections::HashMap, hash::Hasher, iter::zip};

use crate::bit_vec::BitVec;
use crate::str_vec::StringVec;

use super::column::{AggregationType, Column, Group};

#[derive(Debug, Clone)]
pub struct Table {
    col_map: HashMap<String, usize>,
    columns: Vec<TableColumnWrapper>,
    n_rows: usize,
}

#[derive(Debug, Clone)]
pub struct TableColumnWrapper {
    pub name: String,
    pub column: Column,
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
    pub fn from_columns(columns_iter: impl IntoIterator<Item = TableColumnWrapper>) -> Self {
        let mut col_map = HashMap::<String, usize>::new();
        let mut columns = Vec::<TableColumnWrapper>::new();
        let mut n_rows: usize = 0;

        let mut idx: usize = 0;
        for col in columns_iter {
            if idx == 0 {
                n_rows = col.column.len();
            } else {
                assert_eq!(n_rows, col.column.len());
            }

            assert!(!col_map.contains_key(&col.name));
            col_map.insert(col.name.clone(), columns.len());
            columns.push(col);

            idx += 1;
        }

        Table {
            col_map,
            columns,
            n_rows,
        }
    }

    pub fn get_n_rows(&self) -> usize {
        return self.n_rows;
    }

    pub fn get_n_cols(&self) -> usize {
        self.columns.len()
    }

    pub fn from_json_str(json_str: &str) -> Table {
        let json_table: JsonTable = serde_json::from_str(json_str).unwrap();

        let mut col_map = HashMap::<String, usize>::new();
        let mut columns = Vec::<TableColumnWrapper>::new();
        let mut n_rows: usize = 0;

        for col in &json_table.columns {
            let parsed_col = Column::from_string_list(&col._type, col.values.as_slice());
            n_rows = parsed_col.len();
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

            let json_col = JsonColumn {
                name: table_col.name.clone(),
                _type: col.get_type_name(),
                values: col.to_string_list(),
            };

            columns.push(json_col);
        }

        let json_table = JsonTable { columns };

        return serde_json::to_string(&json_table).unwrap();
    }

    pub fn group(&self, group_fields: &[&str], indexes: &[usize]) -> GroupResult {
        let mut row_indexes = Vec::<usize>::new();
        let mut groups = Vec::<Group>::new();

        self.inner_group(group_fields, indexes, &mut row_indexes, &mut groups);

        return GroupResult {
            groups,
            row_indexes,
        };
    }

    fn inner_group(
        &self,
        group_fields: &[&str],
        indexes: &[usize],
        row_indexes: &mut Vec<usize>,
        groups: &mut Vec<Group>,
    ) {
        let group_cols = group_fields
            .iter()
            .map(|col_name| self.get_column(col_name))
            .collect::<Vec<_>>();

        struct HashKey<'a> {
            row_idx: usize,
            columns: &'a [Column],
        }
        impl Eq for HashKey<'_> {}
        impl PartialEq for HashKey<'_> {
            fn eq(&self, other: &Self) -> bool {
                let results = Column::batch_equals(self.columns, &[(self.row_idx, other.row_idx)]);
                return results[0];
            }
        }
        impl Hash for HashKey<'_> {
            fn hash<H: Hasher>(&self, state: &mut H) {
                for col in self.columns {
                    col.hash_at_index(state, self.row_idx);
                }
            }
        }

        let mut hash_to_group_idx = HashMap::<HashKey, usize>::new();

        let mut group_first_indexes = Vec::<usize>::new();
        let mut group_last_indexes = Vec::<usize>::new();
        // let mut external_indexes = Vec::<usize>::new();
        let mut next_indexes = Vec::<usize>::new();

        for (inner_idx, outer_idx) in indexes.into_iter().enumerate() {
            // external_indexes.push(*outer_idx);
            match hash_to_group_idx.entry(HashKey {
                row_idx: *outer_idx,
                columns: &group_cols,
            }) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let group_idx = group_first_indexes.len();
                    entry.insert(group_idx);
                    group_first_indexes.push(inner_idx);
                    group_last_indexes.push(inner_idx);
                    next_indexes.push(usize::MAX);
                }
                std::collections::hash_map::Entry::Occupied(entry) => {
                    let group_idx = *entry.get();
                    let last_idx = &mut group_last_indexes[group_idx];
                    next_indexes[*last_idx] = inner_idx;
                    *last_idx = inner_idx;
                }
            }
        }

        for start_idx in group_first_indexes {
            let mut idx = start_idx;
            let mut len: usize = 0;
            while idx != usize::MAX {
                row_indexes.push(indexes[idx]);
                idx = next_indexes[idx];
                len += 1;
            }
            groups.push(Group { start_idx, len });
        }
    }

    pub fn group_grouping(&self, group_fields: &[&str], group_result: &GroupResult) -> GroupResult {
        let mut groups = Vec::<Group>::new();
        let mut row_indexes = Vec::<usize>::new();

        for group in group_result {
            self.inner_group(group_fields, group, &mut row_indexes, &mut groups);
        }

        GroupResult {
            groups,
            row_indexes,
        }
    }

    pub fn group_and_aggregate(&self, groups: &[&str], aggregations: &[Aggregation]) -> Table {
        let group_cols = groups
            .iter()
            .map(|group_col| self.get_column(group_col))
            .collect::<Vec<_>>();

        let row_hashes = Column::get_col_hashes(self.get_n_rows(), group_cols.as_slice());

        let mut hash_to_group_idx = HashMap::<u64, usize>::new();
        struct HashGroup {
            start_group_row_idx: usize,
            end_group_row_idx: usize,
        }
        let mut hash_groups = Vec::<HashGroup>::new();
        struct HashGroupRow {
            row_idx: usize,
            next_row_idx: usize,
        }
        let mut hash_group_rows = Vec::<HashGroupRow>::with_capacity(self.get_n_rows());

        for (idx, hash) in row_hashes.iter().enumerate() {
            match hash_to_group_idx.get(hash) {
                None => {
                    hash_to_group_idx.insert(*hash, hash_groups.len());
                    hash_groups.push(HashGroup {
                        start_group_row_idx: hash_group_rows.len(),
                        end_group_row_idx: hash_group_rows.len(),
                    });
                }
                Some(group_idx) => {
                    let group_info = &mut hash_groups[*group_idx];
                    hash_group_rows[group_info.end_group_row_idx].next_row_idx =
                        hash_group_rows.len();
                    group_info.end_group_row_idx = hash_group_rows.len();
                }
            }
            hash_group_rows.push(HashGroupRow {
                row_idx: idx,
                next_row_idx: usize::MAX,
            });
        }

        let mut equality_checks = Vec::<(usize, usize)>::new();

        // Pass 1: create equality checks
        for hash_group in &hash_groups {
            if hash_group.start_group_row_idx == hash_group.end_group_row_idx {
                // ignore hash groups with just one row
                continue;
            }

            let mut idx = hash_group.start_group_row_idx;
            let first_row_group = &hash_group_rows[idx];
            let first_row_idx = first_row_group.row_idx;
            idx = first_row_group.next_row_idx;
            loop {
                let group_row = &hash_group_rows[idx];

                equality_checks.push((first_row_idx, group_row.row_idx));

                if idx == hash_group.end_group_row_idx {
                    break;
                }
                idx = group_row.next_row_idx;
            }
        }

        let equalities = Column::batch_equals(&group_cols, &equality_checks);

        // get a sorted list of row indexes that have hash collisions.
        let mut false_equality_row_indexes = Vec::<usize>::new();
        for (is_equal, (_, row_idx)) in zip(&equalities, &equality_checks) {
            if !is_equal {
                false_equality_row_indexes.push(*row_idx);
            }
        }
        false_equality_row_indexes.sort();

        // Use naive alg to group collisions since there shouldn't
        // be many :)
        struct CollisionKey<'a> {
            row_idx: usize,
            row_hash: u64,
            columns: &'a [Column],
        }
        impl Eq for CollisionKey<'_> {}
        impl PartialEq for CollisionKey<'_> {
            fn eq(&self, other: &Self) -> bool {
                let results = Column::batch_equals(self.columns, &[(self.row_idx, other.row_idx)]);
                return results[0];
            }
        }
        impl Hash for CollisionKey<'_> {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.row_hash.hash(state);
            }
        }
        let mut collision_key_to_group_idx = HashMap::<CollisionKey, usize>::new();
        struct CollisionGroup {
            start_row_idx: usize,
            row_indexes: Vec<usize>,
        }
        let mut collision_groups = Vec::<CollisionGroup>::new();
        for idx in false_equality_row_indexes {
            let key = CollisionKey {
                row_idx: idx,
                row_hash: row_hashes[idx],
                columns: group_cols.as_slice(),
            };
            match collision_key_to_group_idx.get(&key) {
                None => {
                    collision_key_to_group_idx.insert(key, collision_groups.len());
                    collision_groups.push(CollisionGroup {
                        start_row_idx: idx,
                        row_indexes: vec![idx],
                    });
                }
                Some(group_idx) => {
                    collision_groups[*group_idx].row_indexes.push(idx);
                }
            }
        }

        // Now that both the "happy-path" and "collision" rows are grouped,
        // do one last pass to combine
        let mut final_group_row_indexes = Vec::<usize>::with_capacity(self.get_n_rows());
        let mut final_groups =
            Vec::<Group>::with_capacity(hash_groups.len() + collision_groups.len());

        {
            let mut equality_idx: usize = 0;
            let mut group_idx: usize = 0;
            let mut collision_group_idx: usize = 0;
            loop {
                let hash_group_exists = group_idx < hash_groups.len();
                let collision_group_exists = collision_group_idx < collision_groups.len();
                if !hash_group_exists && !collision_group_exists {
                    break;
                }
                if !collision_group_exists
                    || collision_groups[collision_group_idx].start_row_idx
                        > hash_group_rows[hash_groups[group_idx].start_group_row_idx].row_idx
                {
                    // process the normal group
                    let mut final_group = Group {
                        start_idx: final_group_row_indexes.len(),
                        len: 1,
                    };
                    let hash_group = &hash_groups[group_idx];
                    let mut idx = hash_group.start_group_row_idx;
                    let first_row_group = &hash_group_rows[idx];
                    let first_row_idx = first_row_group.row_idx;
                    final_group_row_indexes.push(first_row_idx);
                    idx = first_row_group.next_row_idx;
                    loop {
                        if idx == usize::MAX {
                            break;
                        }

                        let group_row = &hash_group_rows[idx];

                        if equalities[equality_idx] {
                            // belongs to group
                            final_group_row_indexes.push(group_row.row_idx);
                            final_group.len += 1;
                        }
                        equality_idx += 1;

                        idx = group_row.next_row_idx;
                    }
                    final_groups.push(final_group);

                    group_idx += 1;
                } else {
                    // process the collision group
                    let collision_group = &collision_groups[collision_group_idx];
                    final_groups.push(Group {
                        start_idx: final_group_row_indexes.len(),
                        len: collision_group.row_indexes.len(),
                    });
                    final_group_row_indexes.extend(&collision_group.row_indexes);

                    collision_group_idx += 1;
                }
            }
        }

        let group_idxs = groups
            .iter()
            .map(|group| *self.col_map.get(*group).unwrap())
            .collect::<Vec<_>>();

        let mut new_columns = Vec::<TableColumnWrapper>::new();

        let first_group_row_indexes = final_groups
            .iter()
            .map(|group| final_group_row_indexes[group.start_idx])
            .collect::<Vec<_>>();

        // add group columns
        for col_group_idx in group_idxs {
            let col = &self.columns[col_group_idx];
            let new_col = col
                .column
                .get_new_col_from_indexes(first_group_row_indexes.as_slice());

            new_columns.push(TableColumnWrapper {
                name: col.name.clone(),
                column: new_col,
            });
        }

        // add aggregation column values
        for agg in aggregations {
            let in_col = &self.columns[self.col_map[agg.in_col_name]].column;
            let out_col = in_col.aggregate(
                &agg.agg_type,
                final_groups.as_slice(),
                final_group_row_indexes.as_slice(),
            );
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
            n_rows: final_groups.len(),
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
            .map(|(_, col_name)| right.get_column(col_name))
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
            .map(|(col_name, _)| self.get_column(col_name))
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
                column: col.column.get_new_col_from_indexes(left_idxs.as_slice()),
            });
        }

        // add the right col values
        for select in new_columns {
            let col = &right.columns[right.col_map[select.old_name]];

            new_col_map.insert(select.new_name.to_string(), new_cols.len());
            new_cols.push(TableColumnWrapper {
                name: select.new_name.to_string(),
                column: col
                    .column
                    .get_new_col_from_opt_indexes(right_idxs.as_slice()),
            })
        }

        return Table {
            columns: new_cols,
            col_map: new_col_map,
            n_rows: join_mapping.len(),
        };
    }

    pub fn get_col_idx(&self, col_name: &str) -> usize {
        self.col_map[col_name]
    }

    pub fn get_column(&self, col_name: &str) -> Column {
        return self.columns[self.col_map[col_name]].column.clone();
    }

    pub fn get_column_at_idx(&self, idx: usize) -> &TableColumnWrapper {
        &self.columns[idx]
    }

    pub fn col_iter<'a>(&'a self) -> std::slice::Iter<'a, TableColumnWrapper> {
        self.columns.iter()
    }

    pub fn col_iter_owned(self) -> Vec<TableColumnWrapper> {
        self.columns
    }

    pub fn where_col_is_true(&self, col: &Column) -> Table {
        let true_indexes = col.get_true_indexes();

        return Table {
            col_map: self.col_map.clone(),
            columns: self
                .columns
                .iter()
                .map(|col| TableColumnWrapper {
                    name: col.name.clone(),
                    column: col.column.from_indexes(&true_indexes),
                })
                .collect::<Vec<_>>(),
            n_rows: true_indexes.len(),
        };
    }

    pub fn with_column(&self, col_name: &str, column: Column) -> Table {
        assert_eq!(self.get_n_rows(), column.len());
        assert!(!self.col_map.contains_key(col_name));
        let mut new_columns = self.columns.clone();
        let mut new_col_map = self.col_map.clone();

        new_col_map.insert(col_name.to_string(), new_columns.len());
        new_columns.push(TableColumnWrapper {
            name: col_name.to_string(),
            column: column.clone(),
        });

        return Table {
            col_map: new_col_map,
            columns: new_columns,
            n_rows: self.n_rows,
        };
    }

    pub fn repeat_scalar_table(&self, len: usize) -> Table {
        assert_eq!(self.get_n_rows(), 1);
        Table {
            col_map: self.col_map.clone(),
            columns: self
                .columns
                .iter()
                .map(|col| TableColumnWrapper {
                    name: col.name.clone(),
                    column: col.column.repeat_scalar_col(len),
                })
                .collect(),
            n_rows: len,
        }
    }

    pub fn from_csv_reader(reader: impl std::io::Read) -> Table {
        let mut reader = csv::Reader::from_reader(reader);

        let mut col_names = StringVec::new();
        let mut col_values = Vec::<StringVec>::new();
        let mut col_nulls = Vec::<BitVec>::new();

        match reader.headers() {
            Err(_) => panic!("csv file needs a header row"),
            Ok(record) => {
                for elem in record.iter() {
                    col_names.push(elem);
                    col_values.push(StringVec::new());
                    col_nulls.push(BitVec::new());
                }
            }
        }

        for record in reader.records() {
            match record {
                Err(_) => panic!("unable to read csv"),
                Ok(record) => {
                    let mut count: usize = 0;
                    for (col_idx, elem) in record.iter().enumerate() {
                        col_values[col_idx].push(elem);
                        col_nulls[col_idx].push(elem == "");
                        count += 1;
                    }

                    assert_eq!(count, col_names.len());
                }
            }
        }

        Table::from_columns(
            std::iter::zip(&col_names, std::iter::zip(col_nulls, col_values)).map(
                |(col_name, (col_nul, col_val))| TableColumnWrapper {
                    name: col_name.to_string(),
                    column: Column::from_str_vec(col_nul, col_val).infer_col_type(),
                },
            ),
        )
    }

    pub fn from_csv(file_name: &str) -> Table {
        let file_reader = std::fs::File::open(file_name).unwrap();

        Self::from_csv_reader(file_reader)
    }
}

impl std::fmt::Display for Table {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        /*
        -------------------
        | col_0 | col_1   |
        | text  | float64 |
        -------------------
        | stuff | 1.23    |
        -------------------
        */

        fn wstr(out: &mut std::fmt::Formatter, str: &str) -> std::fmt::Result {
            out.write_str(str)
        }

        fn wpad(out: &mut std::fmt::Formatter, str: &str, width: usize) -> std::fmt::Result {
            wstr(out, str)?;
            for _ in 0..(width - str.len()) {
                wstr(out, " ")?;
            }

            Ok(())
        }

        let n_rows = self.get_n_rows();
        let n_cols = self.get_n_cols();
        let mut col_names = StringVec::new();
        let mut col_types = StringVec::new();
        let mut out_cols = Vec::<StringVec>::new();
        for col in self.col_iter() {
            let mut writer = col_names.get_writer();
            writer.write_str("'");
            writer.write_str(&col.name);
            writer.write_str("'");

            col_types.push(&col.column.get_type_name());
            out_cols.push(col.column.serialize());
        }

        let mut col_widths = Vec::<usize>::new();
        for col_idx in 0..n_cols {
            let mut width = col_names[col_idx].len().max(col_types[col_idx].len());
            for val in &out_cols[col_idx] {
                width = width.max(val.len());
            }
            col_widths.push(width);
        }

        // write headers
        for col_idx in 0..n_cols {
            if col_idx == 0 {
                wstr(out, "-")?;
            }
            let width = col_widths[col_idx];
            for _ in 0..(width + 2) {
                wstr(out, "-")?;
            }
            wstr(out, "-")?;
        }
        wstr(out, "\n")?;
        for col_idx in 0..n_cols {
            if col_idx == 0 {
                wstr(out, "| ")?;
            } else {
                wstr(out, " ")?;
            }
            let width = col_widths[col_idx];
            wpad(out, &col_names[col_idx], width)?;
            wstr(out, " |")?;
        }
        wstr(out, "\n")?;
        for col_idx in 0..n_cols {
            if col_idx == 0 {
                wstr(out, "| ")?;
            } else {
                wstr(out, " ")?;
            }
            let width = col_widths[col_idx];
            wpad(out, &col_types[col_idx], width)?;
            wstr(out, " |")?;
        }
        wstr(out, "\n")?;
        for col_idx in 0..n_cols {
            if col_idx == 0 {
                wstr(out, "-")?;
            }
            let width = col_widths[col_idx];
            for _ in 0..(width + 2) {
                wstr(out, "-")?;
            }
            wstr(out, "-")?;
        }
        wstr(out, "\n")?;

        let row_limit = 10;
        let (n_leading_rows, n_trailing_rows) = if n_rows <= row_limit {
            (n_rows, 0)
        } else {
            (row_limit >> 1, row_limit >> 1)
        };

        for row_idx in 0..n_leading_rows {
            for col_idx in 0..n_cols {
                if col_idx == 0 {
                    wstr(out, "| ")?;
                } else {
                    wstr(out, " ")?;
                }
                let width = col_widths[col_idx];
                wpad(out, &out_cols[col_idx][row_idx], width)?;
                wstr(out, " |")?;
            }

            wstr(out, "\n")?;
        }

        if n_trailing_rows > 0 {
            wstr(out, "|")?;
            let total_width = col_widths.iter().fold(0, |acc, width| acc + width + 3) - 1;
            for _ in 0..total_width {
                wstr(out, ".")?;
            }
            wstr(out, "|\n")?;
        }

        for row_idx in (n_rows - n_trailing_rows)..n_rows {
            for col_idx in 0..n_cols {
                if col_idx == 0 {
                    wstr(out, "| ")?;
                } else {
                    wstr(out, " ")?;
                }
                let width = col_widths[col_idx];
                wpad(out, &out_cols[col_idx][row_idx], width)?;
                wstr(out, " |")?;
            }

            wstr(out, "\n")?;
        }

        for col_idx in 0..n_cols {
            if col_idx == 0 {
                wstr(out, "-")?;
            }
            let width = col_widths[col_idx];
            for _ in 0..(width + 2) {
                wstr(out, "-")?;
            }
            wstr(out, "-")?;
        }
        wstr(out, "\n")?;

        Ok(())
    }
}

pub struct GroupResult {
    groups: Vec<Group>,
    row_indexes: Vec<usize>,
}

impl<'a> IntoIterator for &'a GroupResult {
    type Item = &'a [usize];

    type IntoIter = GroupResultIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        GroupResultIter {
            group_result: self,
            idx: 0,
        }
    }
}

pub struct GroupResultIter<'a> {
    group_result: &'a GroupResult,
    idx: usize,
}
impl<'a> Iterator for GroupResultIter<'a> {
    type Item = &'a [usize];

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx > self.group_result.groups.len() {
            None
        } else {
            let group = &self.group_result.groups[self.idx];
            let result =
                &self.group_result.row_indexes[group.start_idx..group.start_idx + group.len];
            Some(result)
        }
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
