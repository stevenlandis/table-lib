use core::panic;
use std::collections::HashMap;

use crate::{
    ast_node::{AstNode, AstNodeType},
    parser::{ParseError, Parser},
    partition::Partition,
    AggregationType, Column, SortOrderDirection, Table, TableColumnWrapper,
};

pub struct TableCollection {
    table_map: HashMap<String, Table>,
}

impl TableCollection {
    pub fn new() -> Self {
        TableCollection {
            table_map: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, name: &str, table: Table) {
        assert!(!self.table_map.contains_key(name));
        self.table_map.insert(name.to_string(), table);
    }

    fn get_table(&mut self, name: &str) -> &Table {
        if name.ends_with(".csv") && !self.table_map.contains_key(name) {
            self.table_map
                .insert(name.to_string(), Table::from_csv(name));
        }

        &self.table_map[name]
    }

    pub fn query(&mut self, query: &str) -> Result<Table, ParseError> {
        let mut parser = Parser::new(query);
        let ast = match parser.external_parse_expr() {
            Err(err) => return Err(err),
            Ok(ast) => ast,
        };

        let mut calc_ctx = CalcNodeCtx::new(self);
        let root_id = calc_ctx.register_ast_node(
            &RegisterAstNodeCtx {
                parent_id: usize::MAX,
            },
            &ast,
        );
        let root_id = calc_ctx.normalize(root_id);

        // calc_ctx._print_debug();

        let col_ids = calc_ctx
            .get_calc_node_cols(root_id)
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        let col_names = col_ids
            .iter()
            .cloned()
            .map(|id| calc_ctx.get_calc_node_name(id, 0).to_string())
            .collect::<Vec<_>>();

        let result = calc_ctx.eval_calc_node(root_id);

        let result = match result {
            CalcResult::Table(table) => table,
            _ => panic!(),
        };

        let table = Table::from_columns(std::iter::zip(col_names, &result.cols).map(
            |(name, column)| TableColumnWrapper {
                name,
                column: column.clone(),
            },
        ));

        // calc_ctx.print_debug();

        Ok(table)
    }
}

#[derive(Clone)]
enum CalcResult {
    Table(CalcResultTable),
    Partition(Partition),
    Column(Column),
    RowIndexes(Vec<usize>),
}

#[derive(Clone)]
struct CalcResultTable {
    cols: Vec<Column>,
    partition: Partition,
}

type PartitionLevel = usize;
type PartitionId = usize;

struct CalcNodeCtx<'a> {
    table_collection: &'a mut TableCollection,
    calc_nodes: Vec<CalcNode>,
    registered_tables: HashMap<String, CalcNodeId>,
    name_cache2: HashMap<(CalcNodeId, usize), String>,
    node_cols_cache: HashMap<CalcNodeId, Vec<CalcNodeId>>,
    table_col_node_cache: HashMap<(String, String), CalcNodeId>,
    result_cache: HashMap<CalcNodeId, CalcResult>,
    part_level_count: usize,
    part_level_parent_map: HashMap<PartitionLevel, PartitionLevel>,
    calc_node_to_part_level: HashMap<CalcNodeId, PartitionLevel>,
    partition_id_count: usize,
    partition_id_to_partition: HashMap<PartitionId, Partition>,
    calc_node_to_partition_id: HashMap<CalcNodeId, PartitionId>,
    calc_node_to_is_scalar: HashMap<CalcNodeId, bool>,
}

const ROOT_PARTITION_LEVEL: usize = 0;
const SCALAR_PARTITION_ID: usize = 0;

#[derive(Clone)]
struct RegisterAstNodeCtx {
    parent_id: CalcNodeId,
}

impl<'a> CalcNodeCtx<'a> {
    fn new(table_collection: &'a mut TableCollection) -> Self {
        CalcNodeCtx {
            table_collection,
            calc_nodes: Vec::new(),
            registered_tables: HashMap::new(),
            name_cache2: HashMap::new(),
            node_cols_cache: HashMap::new(),
            table_col_node_cache: HashMap::new(),
            result_cache: HashMap::new(),
            part_level_count: ROOT_PARTITION_LEVEL + 1,
            part_level_parent_map: HashMap::new(),
            calc_node_to_part_level: HashMap::new(),
            partition_id_count: SCALAR_PARTITION_ID + 1,
            calc_node_to_partition_id: HashMap::new(),
            calc_node_to_is_scalar: HashMap::new(),
            partition_id_to_partition: HashMap::new(),
        }
    }

    fn add_calc_node(&mut self, node: CalcNode) -> CalcNodeId {
        let idx = self.calc_nodes.len();
        self.calc_nodes.push(node);
        idx
    }

    fn get_calc_node(&self, id: CalcNodeId) -> &CalcNode {
        if id == usize::MAX {
            panic!("Undefined parent. Maybe missing a \"from\" clause in the query?");
        }
        &self.calc_nodes[id]
    }

    fn get_parent_part_level(&self, part_level: PartitionLevel) -> Option<PartitionLevel> {
        match self.part_level_parent_map.get(&part_level) {
            None => None,
            Some(parent) => Some(*parent),
        }
    }

    fn make_part_level(&mut self, from_level: PartitionLevel) -> PartitionLevel {
        let new_level = self.part_level_count;
        self.part_level_count += 1;
        self.part_level_parent_map
            .insert(new_level.clone(), from_level);
        new_level
    }

    fn get_part_level(&mut self, calc_node_id: CalcNodeId) -> PartitionLevel {
        match self.calc_node_to_part_level.get(&calc_node_id) {
            None => {
                let level = match self.get_calc_node(calc_node_id) {
                    CalcNode::FieldSelect {
                        source_id,
                        col_idx: _,
                    } => self.get_part_level(*source_id),
                    CalcNode::Add(left, right) => {
                        let left = *left;
                        let right = *right;
                        let left_level = self.get_part_level(left);
                        let right_level = self.get_part_level(right);
                        assert_eq!(left_level, right_level);
                        left_level
                    }
                    CalcNode::Integer(_) => ROOT_PARTITION_LEVEL,
                    CalcNode::Float64(_) => ROOT_PARTITION_LEVEL,
                    CalcNode::ColSpread(_, part_level) => *part_level,
                    CalcNode::Table { name: _ } => ROOT_PARTITION_LEVEL,
                    CalcNode::SpreadScalarCol(source_id, _) => self.get_part_level(*source_id),
                    CalcNode::Selects { col_ids } => {
                        assert!(col_ids.len() >= 1);
                        let col_ids = col_ids.clone();
                        let level = self.get_part_level(col_ids[0]);
                        for col_id in &col_ids[1..] {
                            assert_eq!(level, self.get_part_level(*col_id));
                        }
                        level
                    }
                    CalcNode::RePartition(info) => self.get_part_level(info.partition),
                    CalcNode::GroupByPartition { source_id } => self.get_new_partition_level(),
                    CalcNode::FcnCall { name, args } => match name.as_str() {
                        "first" => self.get_part_level(args[0]),
                        "sum" => self.get_part_level(args[0]),
                        "avg" => self.get_part_level(args[0]),
                        _ => todo!("unimplemented for fcn {}", name),
                    },
                    CalcNode::GetUngroupPartition(info) => self.get_part_level(info.source_id),
                    CalcNode::Alias(col_id, _) => self.get_part_level(*col_id),
                    CalcNode::OrderColumn(info) => self.get_part_level(info.col_id),
                    _ => {
                        // self._print_debug();
                        todo!("Unimplemented for {:?}", self.get_calc_node(calc_node_id))
                    }
                };
                self.calc_node_to_part_level.insert(calc_node_id, level);
            }
            Some(result) => return *result,
        }

        self.calc_node_to_part_level[&calc_node_id].clone()
    }

    fn is_parent_of(&mut self, parent_level: PartitionLevel, child_level: PartitionLevel) -> bool {
        let mut level = child_level;
        loop {
            if level == parent_level {
                return true;
            }

            match self.get_parent_part_level(level) {
                None => break,
                Some(parent) => {
                    level = parent;
                }
            }
        }

        return false;
    }

    fn spread_col(&mut self, col_id: CalcNodeId, new_part_level: PartitionLevel) -> CalcNodeId {
        self.add_calc_node(CalcNode::ColSpread(col_id, new_part_level))
    }

    fn is_scalar(&mut self, node_id: CalcNodeId) -> bool {
        match self.calc_node_to_is_scalar.get(&node_id) {
            None => {
                let result: bool = match self.get_calc_node(node_id) {
                    CalcNode::FieldSelect {
                        source_id,
                        col_idx: _,
                    } => self.is_scalar(*source_id),
                    CalcNode::Table { name: _ } => false,
                    CalcNode::Add(left, right) => {
                        let left = *left;
                        let right = *right;
                        let left_is_scalar = self.is_scalar(left);
                        let right_is_scalar = self.is_scalar(right);

                        // if left_is_scalar != right_is_scalar {
                        //     self._print_debug();
                        // }

                        assert_eq!(left_is_scalar, right_is_scalar);
                        left_is_scalar
                    }
                    CalcNode::ColSpread(source_id, _) => self.is_scalar(*source_id),
                    CalcNode::Integer(_) => true,
                    CalcNode::Float64(_) => true,
                    CalcNode::SpreadScalarCol(_, _) => false,
                    CalcNode::Selects { col_ids } => {
                        let col_ids = col_ids.clone();
                        let is_scalar = self.is_scalar(col_ids[0]);
                        for col_id in &col_ids[1..] {
                            // if is_scalar != self.is_scalar(*col_id) {
                            //     self._print_debug();
                            // }

                            assert_eq!(is_scalar, self.is_scalar(*col_id));
                        }
                        is_scalar
                    }
                    CalcNode::RePartition(info) => self.is_scalar(info.col_id),
                    CalcNode::FcnCall { name, args } => match name.as_str() {
                        "first" => true,
                        "sum" => true,
                        "avg" => true,
                        _ => todo!("unimplemented for fcn {}", name),
                    },
                    CalcNode::Alias(col_id, _) => self.is_scalar(*col_id),
                    CalcNode::OrderColumn(info) => self.is_scalar(info.col_id),
                    _ => todo!("Unimplemented for {:?}", self.get_calc_node(node_id)),
                };
                self.calc_node_to_is_scalar.insert(node_id, result);
                result
            }
            Some(result) => *result,
        }
    }

    fn normalize(&mut self, id: CalcNodeId) -> CalcNodeId {
        let col_ids = self
            .get_calc_node_cols(id)
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        let col_ids = self.normalize_cols(col_ids);

        self.add_calc_node(CalcNode::Selects { col_ids })
    }

    /// Normalize all columns to be at the same partition and scalar level.
    /// All columns after this step will have the same partition and the same number of rows.
    fn normalize_cols(&mut self, mut col_ids: Vec<CalcNodeId>) -> Vec<CalcNodeId> {
        let mut lowest_part_level: Option<PartitionLevel> = None;
        for col_id in &col_ids {
            let part_level = self.get_part_level(*col_id);
            match &mut lowest_part_level {
                None => lowest_part_level = Some(part_level),
                Some(lowest_part_level) => {
                    if self.is_parent_of(*lowest_part_level, part_level) {
                        *lowest_part_level = part_level;
                    } else if !self.is_parent_of(part_level, *lowest_part_level) {
                        panic!("All partition levels must be in the same heirarchy");
                    }
                }
            }
        }

        let lowest_part_level = lowest_part_level.unwrap();

        for col_id in &mut col_ids {
            // starting from lowest, get parent part level until reach the current column's part level
            let part_level = self.get_part_level(*col_id);
            let mut part_levels = vec![lowest_part_level];

            loop {
                let last_part_level = *part_levels.last().unwrap();
                if last_part_level == part_level {
                    break;
                }
                part_levels.push(self.get_parent_part_level(last_part_level).unwrap())
            }

            for idx in (0..part_levels.len() - 1).rev() {
                *col_id = self.spread_col(*col_id, part_levels[idx]);
            }
        }

        // now spread scalars if needed (and assert that each partition level is the same)
        let mut final_partition: Option<PartitionId> = None;
        for col_id in &col_ids {
            let is_scalar = self.is_scalar(*col_id);
            if !is_scalar {
                let partition = self.get_partition_id(*col_id);
                match &mut final_partition {
                    None => {
                        final_partition = Some(partition);
                    }
                    Some(final_partition) => {
                        assert_eq!(partition, *final_partition);
                    }
                }
            }
        }

        match final_partition {
            None => {} // all columns are scalar so do nothing
            Some(final_partition) => {
                // spread all scalar columns to match final_partition
                for col_id in &mut col_ids {
                    let is_scalar = self.is_scalar(*col_id);
                    if is_scalar {
                        *col_id =
                            self.add_calc_node(CalcNode::SpreadScalarCol(*col_id, final_partition));
                    }
                }
            }
        }

        col_ids
    }

    fn get_partition_id(&mut self, node_id: CalcNodeId) -> PartitionId {
        match self.calc_node_to_partition_id.get(&node_id) {
            None => {
                let result: PartitionId = match self.get_calc_node(node_id) {
                    CalcNode::FieldSelect {
                        source_id,
                        col_idx: _,
                    } => self.get_partition_id(*source_id),
                    CalcNode::Add(left, right) => {
                        let left = *left;
                        let right = *right;
                        let left_is_scalar = self.get_partition_id(left);
                        let right_is_scalar = self.get_partition_id(right);
                        assert_eq!(left_is_scalar, right_is_scalar);
                        left_is_scalar
                    }
                    CalcNode::SpreadScalarCol(_, partition_id) => *partition_id,
                    CalcNode::Selects { col_ids } => {
                        let col_ids = col_ids.clone();
                        let part_id = self.get_partition_id(col_ids[0]);
                        for col_id in &col_ids {
                            assert_eq!(part_id, self.get_partition_id(*col_id));
                        }
                        part_id
                    }
                    CalcNode::RePartition(info) => info.partition,
                    CalcNode::Alias(col_id, _) => self.get_partition_id(*col_id),
                    CalcNode::OrderColumn(info) => info.partition_id,
                    _ => todo!("Unimplemented for {:?}", self.get_calc_node(node_id)),
                };
                self.calc_node_to_partition_id.insert(node_id, result);
                result
            }
            Some(result) => *result,
        }
    }

    fn get_calc_node_name(&mut self, id: CalcNodeId, col_idx: usize) -> &str {
        let key = (id, col_idx);
        match self.name_cache2.get(&key) {
            None => {
                let name = match self.get_calc_node(id) {
                    CalcNode::FieldSelect {
                        source_id: table_id,
                        col_idx,
                    } => {
                        let table_id = *table_id;
                        let col_idx = *col_idx;
                        self.get_calc_node_name(table_id, col_idx).to_string()
                    }
                    CalcNode::FcnCall { name, args } => {
                        let mut result = String::new();
                        result.push_str(&name);
                        result.push_str("(");
                        let args = args.clone();
                        for (idx, arg) in args.iter().enumerate() {
                            if idx > 0 {
                                result.push_str(", ");
                            }
                            result.push_str(self.get_calc_node_name(*arg, 0));
                        }
                        result.push_str(")");

                        result
                    }
                    CalcNode::Table { name } => {
                        let name = name.clone();
                        let table = self.table_collection.get_table(&name);
                        table.get_column_at_idx(col_idx).name.clone()
                    }
                    CalcNode::Where {
                        source_id,
                        where_col_id: _,
                    } => self.get_calc_node_name(*source_id, col_idx).to_string(),
                    CalcNode::GroupBy {
                        source_id: _,
                        partition_id: _,
                        get_id,
                    } => self.get_calc_node_name(*get_id, col_idx).to_string(),
                    CalcNode::GroupByPartition { source_id } => {
                        self.get_calc_node_name(*source_id, col_idx).to_string()
                    }
                    CalcNode::Selects { col_ids } => {
                        self.get_calc_node_name(col_ids[col_idx], 0).to_string()
                    }
                    CalcNode::Add(left, right) => {
                        let left = *left;
                        let right = *right;

                        let mut result = String::new();
                        result.push_str("(");
                        result.push_str(self.get_calc_node_name(left, 0));
                        result.push_str(" + ");
                        result.push_str(self.get_calc_node_name(right, 0));
                        result.push_str(")");

                        result
                    }
                    CalcNode::GroupByGroupFields { group_by_id } => {
                        self.get_calc_node_name(*group_by_id, col_idx).to_string()
                    }
                    CalcNode::Integer(val) => val.to_string(),
                    CalcNode::Float64(val) => val.to_string(),
                    CalcNode::Alias(_, name) => name.clone(),
                    // CalcNode::OrderBy {
                    //     source_id,
                    //     orders_id: _,
                    //     directions: _,
                    // } => self.get_calc_node_name(*source_id, col_idx).to_string(),
                    CalcNode::Limit(source_id, _) => {
                        self.get_calc_node_name(*source_id, col_idx).to_string()
                    }
                    CalcNode::SpreadScalarCol(source_id, _) => {
                        self.get_calc_node_name(*source_id, col_idx).to_string()
                    }
                    CalcNode::RePartition(info) => {
                        self.get_calc_node_name(info.col_id, col_idx).to_string()
                    }
                    CalcNode::OrderColumn(info) => {
                        self.get_calc_node_name(info.col_id, col_idx).to_string()
                    }
                    _ => todo!("unimplemented for {:?}", self.get_calc_node(id)),
                };
                self.name_cache2.insert(key, name);
            }
            _ => {}
        };

        &self.name_cache2[&key]
    }

    fn get_table_col_node_id(&mut self, table_id: CalcNodeId, col_name: &str) -> CalcNodeId {
        let table_name = match self.get_calc_node(table_id) {
            CalcNode::Table { name } => name.clone(),
            _ => panic!(),
        };

        let key = (table_name.clone(), col_name.to_string());
        match self.table_col_node_cache.get(&key) {
            None => {
                let table = self.table_collection.get_table(&table_name).clone();
                let new_id = self.add_calc_node(CalcNode::FieldSelect {
                    source_id: table_id,
                    col_idx: table.get_col_idx(col_name),
                });
                self.table_col_node_cache.insert(key.clone(), new_id);
            }
            _ => {}
        };

        self.table_col_node_cache[&key]
    }

    fn get_calc_node_cols(&mut self, id: CalcNodeId) -> &[CalcNodeId] {
        match self.node_cols_cache.get(&id) {
            None => {
                let mut cols = Vec::<CalcNodeId>::new();
                match self.get_calc_node(id) {
                    CalcNode::Table { name } => {
                        let name = name.clone();
                        let table = self.table_collection.get_table(&name).clone();
                        for col in table.col_iter() {
                            cols.push(self.get_table_col_node_id(id, &col.name));
                        }
                    }
                    CalcNode::FieldSelect {
                        source_id: _,
                        col_idx: _,
                    } => {
                        cols.push(id);
                    }
                    CalcNode::Selects {
                        col_ids: select_cols,
                    } => {
                        cols.extend(select_cols);
                    }
                    CalcNode::FcnCall { name: _, args: _ } => {
                        cols.push(id);
                    }
                    CalcNode::GroupBy {
                        source_id: _,
                        partition_id: _,
                        get_id,
                    } => {
                        let get_id = *get_id;
                        cols.extend(self.get_calc_node_cols(get_id));
                    }
                    CalcNode::Where {
                        source_id,
                        where_col_id: _,
                    } => {
                        cols.extend(self.get_calc_node_cols(*source_id));
                    }
                    CalcNode::GroupByPartition { source_id } => {
                        let source_cols = self
                            .get_calc_node_cols(*source_id)
                            .iter()
                            .cloned()
                            .collect::<Vec<_>>();

                        cols.extend(source_cols.iter().cloned().map(|col_id| {
                            self.add_calc_node(CalcNode::RePartition(CalcNodeRePartition {
                                col_id,
                                partition: id,
                            }))
                        }))
                        // let n_cols = self.get_calc_node_cols(*source_id).len();
                        // cols.extend((0..n_cols).map(|col_idx| {
                        //     self.add_calc_node(CalcNode::FieldSelect {
                        //         source_id: id,
                        //         col_idx,
                        //     })
                        // }));
                    }
                    // CalcNode::GroupByGroupFields { group_by_id } => {
                    //     let n_cols = self.get_calc_node_cols(*group_by_id).len();
                    //     cols.extend((0..n_cols).map(|col_idx| {
                    //         self.add_calc_node(CalcNode::FieldSelect {
                    //             source_id: id,
                    //             col_idx,
                    //         })
                    //     }));
                    // }
                    CalcNode::Add(_, _) => {
                        cols.push(id);
                    }
                    CalcNode::Integer(_) => {
                        cols.push(id);
                    }
                    CalcNode::Float64(_) => {
                        cols.push(id);
                    }
                    CalcNode::Alias(_, _) => {
                        cols.push(id);
                    }
                    // CalcNode::OrderBy {
                    //     source_id,
                    //     orders_id: _,
                    //     directions: _,
                    // } => {
                    //     cols.extend(
                    //         self.get_calc_node_cols(*source_id)
                    //             .iter()
                    //             .cloned()
                    //             .collect::<Vec<_>>()
                    //             .iter()
                    //             .cloned()
                    //             .map(|col_id| {
                    //                 self.add_calc_node(CalcNode::OrderColumn {
                    //                     col_id,
                    //                     partition_id: id,
                    //                 })
                    //             }),
                    //     );
                    // }
                    CalcNode::Limit(source_id, _) => {
                        cols.extend(
                            self.get_calc_node_cols(*source_id)
                                .iter()
                                .cloned()
                                .collect::<Vec<_>>()
                                .iter()
                                .cloned()
                                .map(|col_id| {
                                    self.add_calc_node(CalcNode::LimitColumn {
                                        col_id,
                                        limit_id: id,
                                    })
                                }),
                        );
                    }
                    CalcNode::RePartition(_) => {
                        cols.push(id);
                    }
                    CalcNode::OrderColumn(_) => {
                        cols.push(id);
                    }
                    _ => todo!("unimplemented for {:?}", self.get_calc_node(id)),
                };

                self.node_cols_cache.insert(id, cols);
            }
            _ => {}
        };

        self.node_cols_cache[&id].as_slice()
    }

    fn get_calc_node_col_idx(&mut self, id: CalcNodeId, col_name: &str) -> usize {
        let col_ids = self
            .get_calc_node_cols(id)
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        for (col_idx, col_id) in col_ids.iter().cloned().enumerate().rev() {
            if self.get_calc_node_name(col_id, 0) == col_name {
                return col_idx;
            }
        }

        panic!("Unable to find col name {:?}", col_name);
    }

    // fn get_calc_node_col(&mut self, id: CalcNodeId, col_name: &str) -> CalcNodeId {
    //     match self.col_name_cache.get(&id) {
    //         None => {
    //             let mut col_map = HashMap::<String, CalcNodeId>::new();
    //             match self.get_calc_node(id) {
    //                 CalcNode2::Table { name } => {
    //                     let table = self.table_collection.get_table(name);
    //                     for col in table.col_iter() {
    //                         col_map
    //                             .insert(col.name.clone(), self.get_table_col_node_id(id, col_name));
    //                     }
    //                 }
    //                 _ => todo!(),
    //             };
    //             self.col_name_cache.insert(id, col_map);
    //         }
    //         _ => {}
    //     };

    //     self.col_name_cache[&id][col_name]
    // }

    fn register_select_list(&mut self, ctx: &RegisterAstNodeCtx, node: &AstNode) -> CalcNodeId {
        let mut col_ids = Vec::<CalcNodeId>::new();
        for select in node.iter_list() {
            let select_id = self.register_ast_node(ctx, select);
            col_ids.extend(self.get_calc_node_cols(select_id));
        }

        self.add_calc_node(CalcNode::Selects { col_ids })
    }

    fn get_ast_col_ids(&mut self, ctx: &RegisterAstNodeCtx, node: &AstNode) -> Vec<CalcNodeId> {
        let mut col_node_ids = Vec::<CalcNodeId>::new();
        for select in node.iter_list() {
            let select_id = self.register_ast_node(ctx, select);
            col_node_ids.extend(self.get_calc_node_cols(select_id));
        }

        col_node_ids
    }

    fn repartition(&mut self, col_id: CalcNodeId, partition_node_id: CalcNodeId) -> CalcNodeId {
        self.add_calc_node(CalcNode::RePartition(CalcNodeRePartition {
            col_id,
            partition: partition_node_id,
        }))
    }

    fn register_ast_node(&mut self, ctx: &RegisterAstNodeCtx, node: &AstNode) -> CalcNodeId {
        match node.get_type() {
            AstNodeType::StmtList(stmts) => {
                let mut ctx = ctx.clone();
                for stmt in stmts.iter_list() {
                    ctx = RegisterAstNodeCtx {
                        parent_id: self.register_ast_node(&ctx, stmt),
                    };
                }
                ctx.parent_id
            }
            AstNodeType::FromStmt(from_node) => match from_node.get_type() {
                AstNodeType::Identifier(table_name) => self.get_table_calc_node(&table_name),
                _ => todo!(),
            },
            AstNodeType::SelectStmt(selects) => {
                let select_node_ids = self.get_ast_col_ids(ctx, selects);

                self.add_calc_node(CalcNode::Selects {
                    col_ids: select_node_ids,
                })
            }
            AstNodeType::Identifier(col_name) => {
                let col_idx = self.get_calc_node_col_idx(ctx.parent_id, col_name);

                self.get_calc_node_cols(ctx.parent_id)[col_idx]
            }
            AstNodeType::WhereStmt(condition) => {
                let condition_id = self.register_ast_node(ctx, condition);
                self.add_calc_node(CalcNode::Where {
                    source_id: ctx.parent_id,
                    where_col_id: condition_id,
                })
            }
            AstNodeType::GroupBy { group_by, get_expr } => {
                // This block decomposes the group_by statment into a series of
                // underlying statements
                let norm_input = self.normalize(ctx.parent_id);
                let group_by_id = self.register_select_list(
                    &RegisterAstNodeCtx {
                        parent_id: norm_input,
                    },
                    group_by,
                );
                let norm_group_expr = self.normalize(group_by_id);

                assert_eq!(
                    self.get_partition_id(norm_input),
                    self.get_partition_id(norm_group_expr)
                );

                let partition_node = self.add_calc_node(CalcNode::GroupByPartition {
                    source_id: norm_group_expr,
                });

                let group_col_ids = self
                    .get_calc_node_cols(norm_group_expr)
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>();

                let group_col_ids = group_col_ids
                    .iter()
                    .map(|col_id| {
                        let col_id = self.repartition(*col_id, partition_node);
                        let first_id = self.add_calc_node(CalcNode::FcnCall {
                            name: "first".to_string(),
                            args: vec![col_id],
                        });
                        let col_name = self.get_calc_node_name(col_id, 0).to_string();
                        self.add_calc_node(CalcNode::Alias(first_id, col_name))
                    })
                    .collect::<Vec<_>>();

                let select_col_ids = self
                    .get_calc_node_cols(norm_input)
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>();

                let select_col_ids = select_col_ids
                    .iter()
                    .map(|col_id| self.repartition(*col_id, partition_node))
                    .collect::<Vec<_>>();

                let all_select_col_ids = select_col_ids
                    .iter()
                    .chain(group_col_ids.iter())
                    .cloned()
                    .collect::<Vec<_>>();
                let base_select_node = self.add_calc_node(CalcNode::Selects {
                    col_ids: all_select_col_ids,
                });

                let select_stmt = self.register_select_list(
                    &RegisterAstNodeCtx {
                        parent_id: base_select_node,
                    },
                    get_expr,
                );

                let select_stmt = self.normalize(select_stmt);

                let ungroup_partition = self.add_calc_node(CalcNode::GetUngroupPartition(
                    CalcNodeGetUngroupPartition {
                        source_id: norm_input,
                        group_partition_id: partition_node,
                        result_id: select_stmt,
                    },
                ));

                let select_stmt_col_ids = self
                    .get_calc_node_cols(select_stmt)
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>();

                let ungrouped_col_ids = select_stmt_col_ids
                    .iter()
                    .map(|col_id| self.repartition(*col_id, ungroup_partition))
                    .collect::<Vec<_>>();

                self.add_calc_node(CalcNode::Selects {
                    col_ids: ungrouped_col_ids,
                })
            }
            AstNodeType::FcnCall { name, args } => {
                let fcn_name = match name.get_type() {
                    AstNodeType::Identifier(fcn_name) => fcn_name.as_str(),
                    _ => todo!(),
                };

                let mut arg_ids = Vec::<CalcNodeId>::new();
                match args {
                    None => {}
                    Some(args) => {
                        for arg_node in args.iter_list() {
                            arg_ids.push(self.register_ast_node(ctx, arg_node));
                        }
                    }
                }

                self.add_calc_node(CalcNode::FcnCall {
                    name: fcn_name.to_string(),
                    args: arg_ids,
                })
            }
            AstNodeType::Add(left, right) => {
                let left_id = self.register_ast_node(ctx, left);
                let right_id = self.register_ast_node(ctx, right);
                let normalized_cols = self.normalize_cols(vec![left_id, right_id]);
                self.add_calc_node(CalcNode::Add(normalized_cols[0], normalized_cols[1]))
            }
            AstNodeType::Integer(val) => self.add_calc_node(CalcNode::Integer(*val)),
            AstNodeType::Float64(val) => self.add_calc_node(CalcNode::Float64(*val)),
            AstNodeType::Alias { expr, alias } => {
                let expr_id = self.register_ast_node(ctx, expr);
                let alias_name = match alias.get_type() {
                    AstNodeType::Identifier(name) => name.clone(),
                    _ => panic!(),
                };

                self.add_calc_node(CalcNode::Alias(expr_id, alias_name))
            }
            AstNodeType::OrderBy(orders) => {
                let mut order_col_ids = Vec::<usize>::new();
                let mut directions = Vec::<SortOrderDirection>::new();
                for node in orders.iter_list() {
                    match node.get_type() {
                        AstNodeType::SortFieldWithDirection(sort_field, direction) => {
                            order_col_ids.push(self.register_ast_node(ctx, sort_field));
                            directions.push(*direction);
                        }
                        _ => {
                            order_col_ids.push(self.register_ast_node(ctx, node));
                            directions.push(SortOrderDirection::Ascending);
                        }
                    }
                }

                let order_cols_id = self.add_calc_node(CalcNode::Selects {
                    col_ids: order_col_ids,
                });
                let order_cols_id = self.normalize(order_cols_id);

                let row_indexes_id =
                    self.add_calc_node(CalcNode::OrderByRowIndexes(OrderByRowIndexes {
                        source_id: ctx.parent_id,
                        order_cols_id,
                        directions,
                    }));

                let partition_id = self.add_calc_node(CalcNode::OrderByPartition {
                    row_indexes: row_indexes_id,
                });

                let input_col_ids = self
                    .get_calc_node_cols(ctx.parent_id)
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>();

                let output_col_ids = input_col_ids
                    .iter()
                    .cloned()
                    .map(|col_id| {
                        self.add_calc_node(CalcNode::OrderColumn(OrderColumn {
                            col_id,
                            row_indexes_id,
                            partition_id,
                        }))
                    })
                    .collect::<Vec<_>>();
                self.add_calc_node(CalcNode::Selects {
                    col_ids: output_col_ids,
                })
            }
            AstNodeType::Limit(limit) => self.add_calc_node(CalcNode::Limit(ctx.parent_id, *limit)),
            _ => todo!("Unknown type {:?}", node),
        }
    }

    fn get_new_partition_level(&mut self) -> usize {
        let result = self.part_level_count;
        self.part_level_count += 1;
        result
    }

    fn set_new_partition_level(&mut self, node_id: CalcNodeId, parent_level: PartitionLevel) {
        let level = self.get_new_partition_level();
        self.calc_node_to_part_level.insert(node_id, level);
        self.part_level_parent_map.insert(level, parent_level);
    }

    fn get_new_partition_id(&mut self) -> usize {
        let result = self.partition_id_count;
        self.partition_id_count += 1;
        result
    }

    fn set_new_partition_id(&mut self, node_id: CalcNodeId) {
        // let partition = self.get_new_partition_id();
        self.calc_node_to_partition_id.insert(node_id, node_id);
    }

    fn get_table_calc_node(&mut self, name: &str) -> CalcNodeId {
        match self.registered_tables.get(name) {
            None => {
                let node_id = self.add_calc_node(CalcNode::Table {
                    name: name.to_string(),
                });
                self.set_new_partition_id(node_id);
                self.registered_tables.insert(name.to_string(), node_id);
            }
            _ => {}
        }

        self.registered_tables[name]
    }

    fn eval_calc_node(&mut self, calc_node_id: CalcNodeId) -> &CalcResult {
        match self.result_cache.get(&calc_node_id) {
            None => {
                let result = match self.get_calc_node(calc_node_id) {
                    CalcNode::Table { name } => {
                        let name = name.clone();
                        let table = self.table_collection.get_table(&name);
                        let len = table.get_n_rows();
                        let cols = table
                            .col_iter()
                            .map(|col| col.column.clone())
                            .collect::<Vec<_>>();

                        CalcResult::Table(CalcResultTable {
                            cols,
                            partition: Partition::new_single_partition(len),
                        })
                    }
                    CalcNode::FieldSelect {
                        source_id: table_id,
                        col_idx,
                    } => {
                        let table_id = *table_id;
                        let col_idx = *col_idx;
                        let result = self.eval_calc_node(table_id);
                        let result = match result {
                            CalcResult::Table(table) => table,
                            _ => panic!(),
                        };

                        CalcResult::Table(CalcResultTable {
                            cols: vec![result.cols[col_idx].clone()],
                            partition: result.partition.clone(),
                        })
                    }
                    CalcNode::Where {
                        source_id: table_id,
                        where_col_id,
                    } => {
                        let table_id = *table_id;
                        let where_col_id = *where_col_id;

                        let condition_result = self.eval_calc_node(where_col_id);
                        let condition_result = match condition_result {
                            CalcResult::Table(table) => table,
                            _ => panic!(),
                        };
                        assert_eq!(condition_result.cols.len(), 1);
                        let condition_col = &condition_result.cols[0];
                        let true_indexes = condition_col.get_true_indexes();

                        let in_result = match self.eval_calc_node(table_id) {
                            CalcResult::Table(table) => table,
                            _ => panic!(),
                        };

                        let cols = in_result
                            .cols
                            .iter()
                            .map(|col| col.from_indexes(&true_indexes))
                            .collect::<Vec<_>>();
                        let partition = in_result.partition.filter_indexes(&true_indexes);

                        CalcResult::Table(CalcResultTable { cols, partition })
                    }
                    CalcNode::FcnCall { name, args } => match name.as_str() {
                        "sum" => {
                            assert_eq!(args.len(), 1);
                            let val = match self.eval_calc_node(args[0]) {
                                CalcResult::Table(table) => table,
                                _ => panic!(),
                            };

                            let cols = val
                                .cols
                                .iter()
                                .map(|col| {
                                    col.aggregate_partition(&val.partition, &AggregationType::Sum)
                                })
                                .collect::<Vec<_>>();

                            CalcResult::Table(CalcResultTable {
                                cols,
                                partition: val.partition.get_single_value_partition(),
                            })
                        }
                        "first" => {
                            assert_eq!(args.len(), 1);
                            let val = match self.eval_calc_node(args[0]) {
                                CalcResult::Table(table) => table,
                                _ => panic!(),
                            };

                            let cols = val
                                .cols
                                .iter()
                                .map(|col| {
                                    col.aggregate_partition(&val.partition, &AggregationType::First)
                                })
                                .collect::<Vec<_>>();

                            CalcResult::Table(CalcResultTable {
                                cols,
                                partition: val.partition.get_single_value_partition(),
                            })
                        }
                        "avg" => {
                            assert_eq!(args.len(), 1);
                            let val = match self.eval_calc_node(args[0]) {
                                CalcResult::Table(table) => table,
                                _ => panic!(),
                            };

                            let counts = Column::aggregate_count(&val.partition);

                            let cols = val
                                .cols
                                .iter()
                                .map(|col| {
                                    let sum = col
                                        .aggregate_partition(&val.partition, &AggregationType::Sum);
                                    let avg = &sum / &counts;
                                    avg
                                })
                                .collect::<Vec<_>>();

                            CalcResult::Table(CalcResultTable {
                                cols,
                                partition: val.partition.get_single_value_partition(),
                            })
                        }
                        _ => todo!("Fcn is not implemented: {}", name),
                    },
                    CalcNode::Selects { col_ids } => {
                        let col_ids = col_ids.clone();
                        let mut cols = Vec::<Column>::new();
                        let mut partition: Option<Partition> = None;
                        for col_id in col_ids {
                            let col_result = match self.eval_calc_node(col_id) {
                                CalcResult::Table(table) => table,
                                _ => panic!(),
                            };

                            match partition {
                                None => {
                                    partition = Some(col_result.partition.clone());
                                }
                                _ => {}
                            }
                            cols.extend(col_result.cols.clone());
                        }

                        let partition = partition.unwrap();

                        CalcResult::Table(CalcResultTable { cols, partition })
                    }
                    CalcNode::GroupByPartition { source_id } => {
                        let input = self.eval_table(*source_id);
                        let partition = Column::group_by(&input.cols, &input.partition);

                        CalcResult::Partition(partition)
                    }
                    CalcNode::Add(left_id, right_id) => {
                        let left_id = *left_id;
                        let right_id = *right_id;

                        let left_result = match self.eval_calc_node(left_id) {
                            CalcResult::Table(table) => table,
                            _ => panic!(),
                        };
                        assert_eq!(left_result.cols.len(), 1);
                        let left_col = left_result.cols[0].clone();
                        let left_partition = left_result.partition.clone();

                        let right_result = match self.eval_calc_node(right_id) {
                            CalcResult::Table(table) => table,
                            _ => panic!(),
                        };
                        assert_eq!(right_result.cols.len(), 1);
                        let right_col = right_result.cols[0].clone();

                        CalcResult::Table(CalcResultTable {
                            cols: vec![&left_col + &right_col],
                            partition: left_partition,
                        })
                    }
                    CalcNode::Integer(val) => CalcResult::Table(CalcResultTable {
                        cols: vec![Column::from_repeated_f64(*val as f64, 1)],
                        partition: Partition::new_single_partition(1),
                    }),
                    CalcNode::Float64(val) => CalcResult::Table(CalcResultTable {
                        cols: vec![Column::from_repeated_f64(*val, 1)],
                        partition: Partition::new_single_partition(1),
                    }),
                    CalcNode::Alias(col_id, _) => self.eval_calc_node(*col_id).clone(),
                    // CalcNode::OrderBy {
                    //     source_id,
                    //     orders_id,
                    //     directions,
                    // } => {
                    //     let source_id = *source_id;
                    //     let orders_id = *orders_id;
                    //     let directions = directions.clone();
                    //     let source = match self.eval_calc_node(source_id) {
                    //         CalcResult::Table(table) => table.clone(),
                    //         _ => panic!(),
                    //     };

                    //     let orders_result = match self.eval_calc_node(orders_id) {
                    //         CalcResult::Table(table) => table.clone(),
                    //         _ => panic!(),
                    //     };

                    //     let sort_indexes = Column::get_sorted_indexes(
                    //         &orders_result.cols,
                    //         &directions,
                    //         source.cols[0].len(),
                    //         &source.partition,
                    //     );
                    //     let result_cols = source
                    //         .cols
                    //         .iter()
                    //         .map(|col| col.from_indexes(&sort_indexes))
                    //         .collect::<Vec<_>>();

                    //     CalcResult::Table(CalcResultTable {
                    //         cols: result_cols,
                    //         partition: source.partition.reset_row_indexes(),
                    //     })
                    // }
                    CalcNode::Limit(source_id, limit) => {
                        let source_id = *source_id;
                        let limit = *limit;
                        let source_result = match self.eval_calc_node(source_id) {
                            CalcResult::Table(table) => table,
                            _ => panic!(),
                        };

                        let result_cols = source_result
                            .cols
                            .iter()
                            .map(|col| col.limit(limit, &source_result.partition))
                            .collect::<Vec<_>>();

                        CalcResult::Table(CalcResultTable {
                            cols: result_cols,
                            partition: source_result.partition.limit(limit),
                        })
                    }
                    CalcNode::SpreadScalarCol(source_id, target_partition_id) => {
                        let source_id = *source_id;
                        let target_partition_id = *target_partition_id;
                        let source_result = match self.eval_calc_node(source_id) {
                            CalcResult::Table(table) => table.clone(),
                            _ => todo!(),
                        };
                        assert_eq!(source_result.cols.len(), 1);

                        let target_partition = self.eval_partition(target_partition_id).clone();

                        let new_col = source_result.cols[0]
                            .repeat_with_partition(&source_result.partition, &target_partition);

                        CalcResult::Table(CalcResultTable {
                            cols: vec![new_col],
                            partition: target_partition,
                        })
                    }
                    CalcNode::RePartition(info) => {
                        let col_id = info.col_id;
                        let partition_id = info.partition;
                        let result = self.eval_table(col_id);
                        assert_eq!(result.cols.len(), 1);
                        let col = result.cols[0].clone();

                        let partition = self.eval_partition(partition_id).clone();

                        CalcResult::Table(CalcResultTable {
                            cols: vec![col],
                            partition,
                        })
                    }
                    CalcNode::GetUngroupPartition(info) => {
                        let info = *info;
                        let source_part = self.eval_partition(info.source_id).clone();
                        let group_part = self.eval_partition(info.group_partition_id).clone();
                        let result_part = self.eval_partition(info.result_id);

                        CalcResult::Partition(Partition::undo_group_by(
                            &source_part,
                            &group_part,
                            result_part,
                        ))
                    }
                    CalcNode::OrderColumn(info) => {
                        let info = *info;
                        let source_col = self.eval_column(info.col_id).clone();
                        let row_indexes = self.eval_row_indexes(info.row_indexes_id);

                        // TODO: Switch to CalcResult::Column
                        // CalcResult::Column(source_col.from_indexes(row_indexes))
                        CalcResult::Table(CalcResultTable {
                            cols: vec![source_col.from_indexes(row_indexes)],
                            partition: self.eval_partition(info.col_id).reset_row_indexes(),
                        })
                    }
                    CalcNode::OrderByRowIndexes(info) => {
                        let info = info.clone();
                        let sort_result = self.eval_table(info.order_cols_id);

                        let sort_indexes = Column::get_sorted_indexes(
                            &sort_result.cols,
                            &info.directions,
                            sort_result.cols[0].len(),
                            &sort_result.partition,
                        );

                        CalcResult::RowIndexes(sort_indexes)
                    }
                    _ => todo!("unimplemented for {:?}", self.get_calc_node(calc_node_id)),
                };
                self.result_cache.insert(calc_node_id, result);
            }
            _ => {}
        }

        &self.result_cache[&calc_node_id]
    }

    fn eval_row_indexes(&mut self, node_id: CalcNodeId) -> &Vec<usize> {
        match self.eval_calc_node(node_id) {
            CalcResult::RowIndexes(indexes) => indexes,
            _ => panic!(),
        }
    }

    fn eval_column(&mut self, node_id: CalcNodeId) -> &Column {
        match self.eval_calc_node(node_id) {
            CalcResult::Column(column) => column,
            CalcResult::Table(table) => {
                assert_eq!(table.cols.len(), 1);
                &table.cols[0]
            }
            _ => panic!(),
        }
    }

    fn eval_partition(&mut self, partition_id: PartitionId) -> &Partition {
        let result = self.eval_calc_node(partition_id);
        match result {
            CalcResult::Table(table) => &table.partition,
            CalcResult::Partition(partition) => partition,
            _ => panic!(),
        }
    }

    fn eval_table(&mut self, col_id: CalcNodeId) -> &CalcResultTable {
        match self.eval_calc_node(col_id) {
            CalcResult::Table(table) => table,
            _ => panic!(),
        }
    }

    fn _print_debug(&mut self) {
        for calc_node_id in 0..self.calc_nodes.len() {
            let calc_node = &self.calc_nodes[calc_node_id];
            println!("{}: {:?}", calc_node_id, calc_node);
            // let col_ids = self
            //     .get_calc_node_cols(calc_node_id)
            //     .iter()
            //     .cloned()
            //     .collect::<Vec<_>>();

            // for (col_idx, col_id) in col_ids.iter().cloned().enumerate() {
            //     println!(
            //         "  - {}: {}",
            //         col_id,
            //         self.get_calc_node_name2(calc_node_id, col_idx)
            //     );
            // }
        }
    }
}

#[derive(Debug)]
enum CalcNode {
    FieldSelect {
        source_id: CalcNodeId,
        col_idx: usize,
    },
    Where {
        source_id: CalcNodeId,
        where_col_id: CalcNodeId,
    },
    Table {
        name: String,
    },
    Selects {
        col_ids: Vec<CalcNodeId>,
    },
    NormalizeCols(CalcNodeId),
    RePartition(CalcNodeRePartition),
    GroupByGroupFields {
        // A table of group by fields with grouped partition
        group_by_id: CalcNodeId,
    },
    GroupByPartition {
        // A partition calculated from all columns on a table
        source_id: CalcNodeId,
    },
    GroupBy {
        // root calc node for groups
        source_id: CalcNodeId,
        partition_id: CalcNodeId,
        get_id: CalcNodeId,
    },
    FcnCall {
        name: String,
        args: Vec<CalcNodeId>,
    },
    Add(CalcNodeId, CalcNodeId),
    Integer(u64),
    Float64(f64),
    Alias(CalcNodeId, String),
    OrderByRowIndexes(OrderByRowIndexes),
    OrderByPartition {
        row_indexes: CalcNodeId,
    },
    OrderColumn(OrderColumn),
    Limit(CalcNodeId, usize),
    ColSpread(CalcNodeId, PartitionLevel),
    SpreadScalarCol(CalcNodeId, PartitionId),
    GetUngroupPartition(CalcNodeGetUngroupPartition),
    LimitColumn {
        col_id: CalcNodeId,
        limit_id: CalcNodeId,
    },
}

#[derive(Debug, Clone)]
struct OrderByRowIndexes {
    source_id: CalcNodeId,
    order_cols_id: CalcNodeId,
    directions: Vec<SortOrderDirection>,
}

#[derive(Debug, Clone, Copy)]
struct OrderColumn {
    col_id: CalcNodeId,
    row_indexes_id: CalcNodeId,
    partition_id: PartitionId,
}

#[derive(Debug)]
struct CalcNodeRePartition {
    col_id: CalcNodeId,
    partition: CalcNodeId,
}

#[derive(Debug, Clone, Copy)]
struct CalcNodeGetUngroupPartition {
    source_id: CalcNodeId,
    group_partition_id: CalcNodeId,
    result_id: CalcNodeId,
}

type CalcNodeId = usize;
