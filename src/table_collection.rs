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

        let col_ids = calc_ctx
            .get_calc_node_cols(root_id)
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        let table = Table::from_columns(col_ids.iter().cloned().map(|col_id| TableColumnWrapper {
            name: calc_ctx.get_calc_node_name(col_id).to_string(),
            column: calc_ctx.eval_column(col_id).clone(),
        }));

        Ok(table)
    }
}

#[derive(Clone)]
enum CalcResult {
    Partition(Partition),
    Column(Column),
    RowIndexes(Vec<usize>),
    GroupByPartition(Partition, Vec<usize>),
}

type PartitionLevel = usize;
type PartitionId = usize;

struct CalcNodeCtx<'a> {
    table_collection: &'a mut TableCollection,
    calc_nodes: Vec<CalcNode>,
    calc_node_to_id_map: HashMap<CalcNode, usize>,
    registered_tables: HashMap<String, CalcNodeId>,
    name_cache: HashMap<CalcNodeId, String>,
    node_cols_cache: HashMap<CalcNodeId, Vec<CalcNodeId>>,
    result_cache: HashMap<CalcNodeId, CalcResult>,
    part_level_count: usize,
    part_level_parent_map: HashMap<PartitionLevel, PartitionLevel>,
    calc_node_to_part_level: HashMap<CalcNodeId, PartitionLevel>,
    calc_node_to_partition_id: HashMap<CalcNodeId, PartitionId>,
    calc_node_to_is_scalar: HashMap<CalcNodeId, bool>,
    partition_level_to_source_partition_id: HashMap<PartitionLevel, PartitionId>,
}

const ROOT_PARTITION_LEVEL: usize = 0;

#[derive(Clone)]
struct RegisterAstNodeCtx {
    parent_id: CalcNodeId,
}

impl<'a> CalcNodeCtx<'a> {
    fn new(table_collection: &'a mut TableCollection) -> Self {
        CalcNodeCtx {
            table_collection,
            calc_nodes: Vec::new(),
            calc_node_to_id_map: HashMap::new(),
            registered_tables: HashMap::new(),
            name_cache: HashMap::new(),
            node_cols_cache: HashMap::new(),
            result_cache: HashMap::new(),
            part_level_count: ROOT_PARTITION_LEVEL + 1,
            part_level_parent_map: HashMap::new(),
            calc_node_to_part_level: HashMap::new(),
            calc_node_to_partition_id: HashMap::new(),
            calc_node_to_is_scalar: HashMap::new(),
            partition_level_to_source_partition_id: HashMap::new(),
        }
    }

    fn add_calc_node(&mut self, node: CalcNode) -> CalcNodeId {
        match self.calc_node_to_id_map.get(&node) {
            None => {
                assert_eq!(self.calc_node_to_id_map.len(), self.calc_nodes.len());
                let idx = self.calc_node_to_id_map.len();
                self.calc_node_to_id_map.insert(node.clone(), idx);
                self.calc_nodes.push(node);
                idx
            }
            Some(idx) => *idx,
        }
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

    fn get_partition_level(&mut self, calc_node_id: CalcNodeId) -> PartitionLevel {
        match self.calc_node_to_part_level.get(&calc_node_id) {
            None => {
                let level = match self.get_calc_node(calc_node_id) {
                    CalcNode::TableCol(info) => self.get_partition_level(info.partition_id),
                    CalcNode::Add(left, right) => {
                        let left = *left;
                        let right = *right;
                        let left_level = self.get_partition_level(left);
                        let right_level = self.get_partition_level(right);
                        assert_eq!(left_level, right_level);
                        left_level
                    }
                    CalcNode::Literal(_) => ROOT_PARTITION_LEVEL,
                    CalcNode::ColSpread(_, part_level) => *part_level,
                    CalcNode::TablePartition(_) => ROOT_PARTITION_LEVEL,
                    CalcNode::ScalarPartition => ROOT_PARTITION_LEVEL,
                    CalcNode::SpreadScalarCol(source_id, _) => self.get_partition_level(*source_id),
                    CalcNode::Selects { col_ids } => {
                        assert!(col_ids.len() >= 1);
                        let col_ids = col_ids.clone();
                        let level = self.get_partition_level(col_ids[0]);
                        for col_id in &col_ids[1..] {
                            assert_eq!(level, self.get_partition_level(*col_id));
                        }
                        level
                    }
                    CalcNode::RePartition(info) => self.get_partition_level(info.partition),
                    CalcNode::GroupByPartition { source_id: _ } => self.get_new_partition_level(),
                    CalcNode::GetUngroupPartition(info) => self.get_partition_level(info.source_id),
                    CalcNode::Alias(col_id, _) => self.get_partition_level(*col_id),
                    CalcNode::OrderColumn(info) => self.get_partition_level(info.col_id),
                    CalcNode::ReOrderAndRePartition(info) => {
                        self.get_partition_level(info.partition_id)
                    }
                    CalcNode::Aggregation {
                        col_id: _,
                        typ: _,
                        partition_id,
                    } => self.get_partition_level(*partition_id),
                    CalcNode::AggregatedPartition(partition_level) => *partition_level,
                    CalcNode::Count(info) => self.get_partition_level(info.out_partition_id),
                    CalcNode::Divide(left, right) => {
                        let left = *left;
                        let right = *right;
                        let left_level = self.get_partition_level(left);
                        let right_level = self.get_partition_level(right);
                        assert_eq!(left_level, right_level);
                        left_level
                    }
                    CalcNode::PartitionFromRowIndexes(partition_id, _) => {
                        self.get_partition_level(*partition_id)
                    }
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
                    CalcNode::TableCol(info) => self.is_scalar(info.partition_id),
                    CalcNode::TablePartition(_) => false,
                    CalcNode::Add(left, right) => {
                        let left = *left;
                        let right = *right;
                        let left_is_scalar = self.is_scalar(left);
                        let right_is_scalar = self.is_scalar(right);

                        assert_eq!(left_is_scalar, right_is_scalar);
                        left_is_scalar
                    }
                    CalcNode::ColSpread(source_id, _) => self.is_scalar(*source_id),
                    CalcNode::Literal(_) => true,
                    CalcNode::SpreadScalarCol(_, _) => false,
                    CalcNode::ScalarPartition => true,
                    CalcNode::Selects { col_ids } => {
                        let col_ids = col_ids.clone();
                        let is_scalar = self.is_scalar(col_ids[0]);
                        for col_id in &col_ids[1..] {
                            assert_eq!(is_scalar, self.is_scalar(*col_id));
                        }
                        is_scalar
                    }
                    CalcNode::RePartition(info) => self.is_scalar(info.col_id),
                    CalcNode::Alias(col_id, _) => self.is_scalar(*col_id),
                    CalcNode::OrderColumn(info) => self.is_scalar(info.col_id),
                    CalcNode::ReOrderAndRePartition(info) => self.is_scalar(info.col_id),
                    CalcNode::Aggregation {
                        col_id: _,
                        typ: _,
                        partition_id: _,
                    } => true,
                    CalcNode::GroupByPartition { source_id: _ } => false,
                    CalcNode::Count(_) => true,
                    CalcNode::AggregatedPartition(_) => true,
                    CalcNode::Divide(left, right) => {
                        let left = *left;
                        let right = *right;
                        let left_scalar = self.is_scalar(left);
                        let right_scalar = self.is_scalar(right);
                        assert_eq!(left_scalar, right_scalar);
                        left_scalar
                    }
                    CalcNode::GetUngroupPartition(info) => self.is_scalar(info.result_id),
                    CalcNode::PartitionFromRowIndexes(partition_id, _) => {
                        self.is_scalar(*partition_id)
                    }
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

        // double check that normalization worked
        let partition_id = self.get_partition_id(col_ids[0]);
        let partition_level = self.get_partition_level(col_ids[0]);
        for col_id in &col_ids {
            assert_eq!(partition_id, self.get_partition_id(*col_id));
            assert_eq!(partition_level, self.get_partition_level(*col_id));
        }

        self.add_calc_node(CalcNode::Selects { col_ids })
    }

    fn normalize_col_to(&mut self, col_id: CalcNodeId, to_partition_id: PartitionId) -> CalcNodeId {
        let mut col_id = col_id;
        let to_part_level = self.get_partition_level(to_partition_id);

        // starting from lowest, get parent part level until reach the current column's part level
        let part_level = self.get_partition_level(col_id);
        let mut part_levels = vec![to_part_level];

        loop {
            let last_part_level = *part_levels.last().unwrap();
            if last_part_level == part_level {
                break;
            }
            part_levels.push(self.get_parent_part_level(last_part_level).unwrap())
        }

        for idx in (0..part_levels.len() - 1).rev() {
            col_id = self.spread_col(col_id, part_levels[idx]);
        }

        let to_scalar = self.is_scalar(to_partition_id);
        if !to_scalar {
            if self.is_scalar(col_id) {
                col_id = self.add_calc_node(CalcNode::SpreadScalarCol(col_id, to_partition_id))
            }
        }

        col_id
    }

    /// Normalize all columns to be at the same partition and scalar level.
    /// All columns after this step will have the same partition and the same number of rows.
    fn normalize_cols(&mut self, mut col_ids: Vec<CalcNodeId>) -> Vec<CalcNodeId> {
        struct LowestInfo {
            part_level: PartitionLevel,
            part_id: PartitionId,
            is_scalar: bool,
        }
        let mut lowest: Option<LowestInfo> = None;
        // let mut lowest_part_level: Option<PartitionLevel> = None;
        // let mut lowest_part_id: Option<PartitionId> = None;
        for col_id in &col_ids {
            let part_level = self.get_partition_level(*col_id);
            let part_id = self.get_partition_id(*col_id);
            let is_scalar = self.is_scalar(*col_id);
            match &mut lowest {
                None => {
                    lowest = Some(LowestInfo {
                        part_level,
                        part_id,
                        is_scalar,
                    });
                }
                Some(lowest) => {
                    if lowest.part_level == part_level {
                        if lowest.is_scalar && !is_scalar {
                            *lowest = LowestInfo {
                                part_level,
                                part_id,
                                is_scalar,
                            };
                        }
                    } else if self.is_parent_of(lowest.part_level, part_level) {
                        *lowest = LowestInfo {
                            part_level,
                            part_id,
                            is_scalar,
                        };
                    } else if !self.is_parent_of(part_level, lowest.part_level) {
                        panic!("All partition levels must be in the same heirarchy");
                    }
                }
            }
        }

        let lowest = lowest.unwrap();

        if lowest.part_id == 8 {
            println!("Normalie to {}", lowest.part_id);
        }

        for col_id in &mut col_ids {
            *col_id = self.normalize_col_to(*col_id, lowest.part_id);
        }

        col_ids
    }

    fn get_partition_id(&mut self, node_id: CalcNodeId) -> PartitionId {
        match self.calc_node_to_partition_id.get(&node_id) {
            None => {
                let result: PartitionId = match self.get_calc_node(node_id) {
                    CalcNode::TableCol(info) => info.partition_id,
                    CalcNode::TablePartition(_) => node_id,
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
                            if part_id != self.get_partition_id(*col_id) {
                                self._print_debug();
                            }

                            assert_eq!(part_id, self.get_partition_id(*col_id));
                        }
                        part_id
                    }
                    CalcNode::RePartition(info) => info.partition,
                    CalcNode::Alias(col_id, _) => self.get_partition_id(*col_id),
                    CalcNode::OrderColumn(info) => info.partition_id,
                    CalcNode::ReOrderAndRePartition(info) => info.partition_id,
                    CalcNode::Aggregation {
                        col_id: _,
                        typ: _,
                        partition_id,
                    } => *partition_id,
                    CalcNode::Count(info) => info.out_partition_id,
                    CalcNode::Divide(left, right) => {
                        let left = *left;
                        let right = *right;
                        let left_id = self.get_partition_id(left);
                        let right_id = self.get_partition_id(right);

                        assert_eq!(left_id, right_id);
                        left_id
                    }
                    CalcNode::Literal(_) => self.add_calc_node(CalcNode::ScalarPartition),
                    CalcNode::GroupByPartition { source_id: _ } => node_id,
                    _ => todo!("Unimplemented for {:?}", self.get_calc_node(node_id)),
                };
                self.calc_node_to_partition_id.insert(node_id, result);
                result
            }
            Some(result) => *result,
        }
    }

    fn get_calc_node_name(&mut self, col_id: CalcNodeId) -> &str {
        match self.name_cache.get(&col_id) {
            None => {
                let name = match self.get_calc_node(col_id) {
                    CalcNode::TableCol(info) => info.col_name.clone(),
                    CalcNode::GroupByPartition { source_id } => {
                        self.get_calc_node_name(*source_id).to_string()
                    }
                    CalcNode::Add(left, right) => {
                        let left = *left;
                        let right = *right;

                        let mut result = String::new();
                        result.push_str("(");
                        result.push_str(self.get_calc_node_name(left));
                        result.push_str(" + ");
                        result.push_str(self.get_calc_node_name(right));
                        result.push_str(")");

                        result
                    }
                    CalcNode::Literal(literal) => match literal {
                        CalcNodeLiteral::Integer(val) => val.to_string(),
                        CalcNodeLiteral::Float64(val) => val.val.to_string(),
                        CalcNodeLiteral::Bool(val) => match val {
                            false => "false",
                            true => "true",
                        }
                        .to_string(),
                    },
                    CalcNode::Alias(_, name) => name.clone(),
                    CalcNode::SpreadScalarCol(source_id, _) => {
                        self.get_calc_node_name(*source_id).to_string()
                    }
                    CalcNode::RePartition(info) => self.get_calc_node_name(info.col_id).to_string(),
                    CalcNode::OrderColumn(info) => self.get_calc_node_name(info.col_id).to_string(),
                    CalcNode::ReOrderAndRePartition(info) => {
                        self.get_calc_node_name(info.col_id).to_string()
                    }
                    CalcNode::Aggregation {
                        col_id,
                        typ,
                        partition_id: _,
                    } => {
                        let mut result = String::new();

                        result.push_str(match typ {
                            AggregationType::First => "first",
                            AggregationType::Sum => "sum",
                        });
                        result.push_str("(");
                        result.push_str(self.get_calc_node_name(*col_id));
                        result.push_str(")");

                        result
                    }
                    _ => todo!("unimplemented for {:?}", self.get_calc_node(col_id)),
                };
                self.name_cache.insert(col_id, name);
            }
            _ => {}
        };

        &self.name_cache[&col_id]
    }

    fn get_calc_node_cols(&mut self, id: CalcNodeId) -> &[CalcNodeId] {
        match self.node_cols_cache.get(&id) {
            None => {
                let mut cols = Vec::<CalcNodeId>::new();
                match self.get_calc_node(id) {
                    CalcNode::TableCol(_) => {
                        cols.push(id);
                    }
                    CalcNode::Selects {
                        col_ids: select_cols,
                    } => {
                        cols.extend(select_cols);
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
                    }
                    CalcNode::Add(_, _) => {
                        cols.push(id);
                    }
                    CalcNode::Literal(_) => {
                        cols.push(id);
                    }
                    CalcNode::Alias(_, _) => {
                        cols.push(id);
                    }
                    CalcNode::RePartition(_) => {
                        cols.push(id);
                    }
                    CalcNode::OrderColumn(_) => {
                        cols.push(id);
                    }
                    CalcNode::ReOrderAndRePartition(_) => {
                        cols.push(id);
                    }
                    CalcNode::Aggregation {
                        col_id: _,
                        typ: _,
                        partition_id: _,
                    } => {
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
            if self.get_calc_node_name(col_id) == col_name {
                return col_idx;
            }
        }

        panic!("Unable to find col name {:?}", col_name);
    }

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
                let norm_in = self.normalize(ctx.parent_id);
                let in_partition_id = self.get_partition_id(norm_in);

                let condition_id = self.register_ast_node(ctx, condition);
                let condition_id = self.normalize_col_to(condition_id, in_partition_id);

                let row_indexes_id = self.add_calc_node(CalcNode::WhereRowIndexes(condition_id));

                let partition_id = self.add_calc_node(CalcNode::PartitionFromRowIndexes(
                    in_partition_id,
                    row_indexes_id,
                ));

                let col_ids = self
                    .get_calc_node_cols(norm_in)
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>();

                let result_col_ids = col_ids
                    .iter()
                    .cloned()
                    .map(|col_id| {
                        self.add_calc_node(CalcNode::ReOrderAndRePartition(ReOrderAndRePartition {
                            col_id,
                            partition_id,
                            row_indexes_id,
                        }))
                    })
                    .collect::<Vec<_>>();

                self.add_calc_node(CalcNode::Selects {
                    col_ids: result_col_ids,
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
                    .cloned()
                    .map(|col_id| {
                        let col_id = self.add_calc_node(CalcNode::ReOrderAndRePartition(
                            ReOrderAndRePartition {
                                col_id,
                                partition_id: partition_node,
                                row_indexes_id: partition_node,
                            },
                        ));
                        let first_id = self.add_aggregation(col_id, AggregationType::First);
                        let col_name = self.get_calc_node_name(col_id).to_string();
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
                    .cloned()
                    .map(|col_id| {
                        self.add_calc_node(CalcNode::ReOrderAndRePartition(ReOrderAndRePartition {
                            col_id,
                            partition_id: partition_node,
                            row_indexes_id: partition_node,
                        }))
                    })
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

                match fcn_name {
                    "sum" => {
                        assert_eq!(arg_ids.len(), 1);
                        let col_id = arg_ids[0];
                        self.add_aggregation(col_id, AggregationType::Sum)
                    }
                    "first" => {
                        assert_eq!(arg_ids.len(), 1);
                        let col_id = arg_ids[0];
                        self.add_aggregation(col_id, AggregationType::First)
                    }
                    "avg" => {
                        assert_eq!(arg_ids.len(), 1);
                        let col_id = arg_ids[0];
                        let in_part_id = self.get_partition_id(col_id);

                        let sum_id = self.add_aggregation(col_id, AggregationType::Sum);
                        let count_id = self.add_count(in_part_id);

                        self.add_division(sum_id, count_id)
                    }
                    _ => todo!("Support function \"{}\"", fcn_name),
                }
            }
            AstNodeType::Add(left, right) => {
                let left_id = self.register_ast_node(ctx, left);
                let right_id = self.register_ast_node(ctx, right);
                let normalized_cols = self.normalize_cols(vec![left_id, right_id]);
                self.add_calc_node(CalcNode::Add(normalized_cols[0], normalized_cols[1]))
            }
            AstNodeType::Integer(val) => {
                self.add_calc_node(CalcNode::Literal(CalcNodeLiteral::Integer(*val)))
            }
            AstNodeType::Float64(val) => self.add_calc_node(CalcNode::Literal(
                CalcNodeLiteral::Float64(CalcNodeFloat64 { val: *val }),
            )),
            AstNodeType::Bool(val) => {
                self.add_calc_node(CalcNode::Literal(CalcNodeLiteral::Bool(*val)))
            }
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

                let partition_id = self.get_partition_id(ctx.parent_id);

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
            AstNodeType::Limit(limit) => {
                let in_col_ids = self
                    .get_calc_node_cols(ctx.parent_id)
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>();
                let in_partition_id = self.get_partition_id(ctx.parent_id);
                let row_indexes_id =
                    self.add_calc_node(CalcNode::LimitRowIndexes(in_partition_id, *limit));

                let out_partition_id = self.add_calc_node(CalcNode::PartitionFromRowIndexes(
                    in_partition_id,
                    row_indexes_id,
                ));

                let out_col_ids = in_col_ids
                    .iter()
                    .cloned()
                    .map(|col_id| {
                        self.add_calc_node(CalcNode::OrderColumn(OrderColumn {
                            col_id,
                            row_indexes_id,
                            partition_id: out_partition_id,
                        }))
                    })
                    .collect::<Vec<_>>();

                self.add_calc_node(CalcNode::Selects {
                    col_ids: out_col_ids,
                })
            }
            _ => todo!("Unknown type {:?}", node),
        }
    }

    fn add_division(&mut self, left_id: CalcNodeId, right_id: CalcNodeId) -> CalcNodeId {
        let normalized_cols = self.normalize_cols(vec![left_id, right_id]);
        self.add_calc_node(CalcNode::Divide(normalized_cols[0], normalized_cols[1]))
    }

    fn add_count(&mut self, calc_node_id: PartitionId) -> CalcNodeId {
        let in_partition_id = self.get_partition_id(calc_node_id);
        let out_partition_id = self.add_aggregated_partition(calc_node_id);
        self.add_calc_node(CalcNode::Count(CalcNodeCount {
            in_partition_id,
            out_partition_id,
        }))
    }

    fn add_aggregated_partition(&mut self, col_id: CalcNodeId) -> CalcNodeId {
        let in_partition_id = self.get_partition_id(col_id);
        let in_partition_level = self.get_partition_level(col_id);
        let out_part_id = self.add_calc_node(CalcNode::AggregatedPartition(in_partition_level));

        if !self
            .partition_level_to_source_partition_id
            .contains_key(&in_partition_level)
        {
            self.partition_level_to_source_partition_id
                .insert(in_partition_level, in_partition_id);
        }

        out_part_id
    }

    fn add_aggregation(&mut self, col_id: CalcNodeId, agg_type: AggregationType) -> CalcNodeId {
        let out_part_id = self.add_aggregated_partition(col_id);
        self.add_calc_node(CalcNode::Aggregation {
            col_id,
            typ: agg_type,
            partition_id: out_part_id,
        })
    }

    fn get_new_partition_level(&mut self) -> usize {
        let result = self.part_level_count;
        self.part_level_count += 1;
        result
    }

    fn get_table_calc_node(&mut self, name: &str) -> CalcNodeId {
        match self.registered_tables.get(name) {
            None => {
                let table = self.table_collection.get_table(name);
                let col_names = table
                    .col_iter()
                    .map(|col| col.name.clone())
                    .collect::<Vec<_>>();

                let partition_id = self.add_calc_node(CalcNode::TablePartition(name.to_string()));

                let col_ids = col_names
                    .into_iter()
                    .map(|col| {
                        self.add_calc_node(CalcNode::TableCol(CalcNodeTableCol {
                            table_name: name.to_string(),
                            col_name: col,
                            partition_id,
                        }))
                    })
                    .collect::<Vec<_>>();

                let node_id = self.add_calc_node(CalcNode::Selects { col_ids });

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
                    CalcNode::TablePartition(table_name) => {
                        let table_name = table_name.to_string();
                        let table = self.table_collection.get_table(&table_name);
                        let len = table.get_n_rows();
                        CalcResult::Partition(Partition::new_single_partition(len))
                    }
                    CalcNode::TableCol(info) => {
                        let info = info.clone();
                        let table = self.table_collection.get_table(&info.table_name);
                        let col = table.get_column(&info.col_name);
                        CalcResult::Column(col)
                    }
                    CalcNode::GroupByPartition { source_id } => {
                        let source_id = *source_id;
                        let col_ids = self
                            .get_calc_node_cols(source_id)
                            .iter()
                            .cloned()
                            .collect::<Vec<_>>();

                        let cols = col_ids
                            .iter()
                            .cloned()
                            .map(|col_id| self.eval_column(col_id).clone())
                            .collect::<Vec<_>>();

                        let partition = self.eval_partition_from_calc_node(source_id);

                        let (partition, row_indexes) = Column::group_by(&cols, &partition);

                        CalcResult::GroupByPartition(partition, row_indexes)
                    }
                    CalcNode::Add(left_id, right_id) => {
                        let left_id = *left_id;
                        let right_id = *right_id;

                        let left_col = self.eval_column(left_id).clone();
                        let right_col = self.eval_column(right_id).clone();

                        let partition_id = self.get_partition_id(left_id);
                        assert_eq!(partition_id, self.get_partition_id(right_id));

                        CalcResult::Column(&left_col + &right_col)
                    }
                    CalcNode::Divide(left_id, right_id) => {
                        let left_id = *left_id;
                        let right_id = *right_id;

                        let left_col = self.eval_column(left_id).clone();
                        let right_col = self.eval_column(right_id).clone();

                        let partition_id = self.get_partition_id(left_id);
                        assert_eq!(partition_id, self.get_partition_id(right_id));

                        CalcResult::Column(&left_col / &right_col)
                    }
                    CalcNode::Literal(val) => CalcResult::Column(match val {
                        CalcNodeLiteral::Integer(val) => Column::from_repeated_f64(*val as f64, 1),
                        CalcNodeLiteral::Float64(val) => Column::from_repeated_f64(val.val, 1),
                        CalcNodeLiteral::Bool(val) => Column::from_repeated_bool(*val, 1),
                    }),
                    CalcNode::ScalarPartition => {
                        CalcResult::Partition(Partition::new_single_partition(1))
                    }
                    CalcNode::Alias(col_id, _) => self.eval_calc_node(*col_id).clone(),
                    CalcNode::SpreadScalarCol(source_id, target_partition_id) => {
                        let source_id = *source_id;
                        let target_partition_id = *target_partition_id;
                        let source_col = self.eval_column(source_id).clone();
                        let source_partition =
                            self.eval_partition_from_calc_node(source_id).clone();

                        let target_partition = self
                            .eval_partition_from_partition_id(target_partition_id)
                            .clone();

                        let new_col =
                            source_col.repeat_with_partition(&source_partition, &target_partition);

                        CalcResult::Column(new_col)
                    }
                    CalcNode::RePartition(info) => {
                        let col_id = info.col_id;
                        let col = self.eval_column(col_id).clone();

                        CalcResult::Column(col)
                    }
                    CalcNode::GetUngroupPartition(info) => {
                        let info = *info;
                        let source_part =
                            self.eval_partition_from_calc_node(info.source_id).clone();
                        let group_part = self
                            .eval_partition_from_partition_id(info.group_partition_id)
                            .clone();
                        let result_part = self.eval_partition_from_calc_node(info.result_id);

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

                        CalcResult::Column(source_col.from_indexes(row_indexes))
                    }
                    CalcNode::OrderByRowIndexes(info) => {
                        let info = info.clone();
                        let col_ids = self
                            .get_calc_node_cols(info.order_cols_id)
                            .iter()
                            .cloned()
                            .collect::<Vec<_>>();
                        let sort_cols = col_ids
                            .iter()
                            .cloned()
                            .map(|col_id| self.eval_column(col_id).clone())
                            .collect::<Vec<_>>();

                        let partition = self.eval_partition_from_calc_node(info.order_cols_id);

                        let sort_indexes = Column::get_sorted_indexes(
                            &sort_cols,
                            &info.directions,
                            partition.n_rows(),
                            partition,
                        );

                        CalcResult::RowIndexes(sort_indexes)
                    }
                    CalcNode::ReOrderAndRePartition(info) => {
                        let info = *info;
                        let source_col = self.eval_column(info.col_id).clone();
                        let row_indexes = self.eval_row_indexes(info.row_indexes_id);

                        CalcResult::Column(source_col.from_indexes(row_indexes))
                    }
                    CalcNode::Aggregation {
                        col_id,
                        typ,
                        partition_id: _,
                    } => {
                        let col_id = *col_id;
                        let typ = *typ;

                        let partition = self.eval_partition_from_calc_node(col_id).clone();
                        let col = self.eval_column(col_id);
                        let agg_col = col.aggregate_partition(&partition, &typ);

                        CalcResult::Column(agg_col)
                    }
                    CalcNode::AggregatedPartition(partition_level) => {
                        let partition_level = *partition_level;
                        let in_partition_id =
                            self.partition_level_to_source_partition_id[&partition_level];
                        let in_partition = self.eval_partition_from_partition_id(in_partition_id);

                        CalcResult::Partition(in_partition.get_single_value_partition())
                    }
                    CalcNode::Count(info) => {
                        let info = *info;
                        let in_partition =
                            self.eval_partition_from_partition_id(info.in_partition_id);
                        let out_col = Column::aggregate_count(in_partition);

                        CalcResult::Column(out_col)
                    }
                    CalcNode::LimitRowIndexes(partition_id, limit) => {
                        let partition_id = *partition_id;
                        let limit = *limit;
                        let partition = self.eval_partition_from_calc_node(partition_id);
                        let row_indexes = partition.get_limit_row_indexes(limit);

                        CalcResult::RowIndexes(row_indexes)
                    }
                    CalcNode::PartitionFromRowIndexes(in_partition_id, row_indexes_id) => {
                        let in_partition_id = *in_partition_id;
                        let row_indexes_id = *row_indexes_id;

                        let in_partition = self
                            .eval_partition_from_partition_id(in_partition_id)
                            .clone();
                        let row_indexes = self.eval_row_indexes(row_indexes_id);

                        CalcResult::Partition(in_partition.from_row_indexes(row_indexes))
                    }
                    CalcNode::WhereRowIndexes(col_id) => {
                        let col = self.eval_column(*col_id);
                        CalcResult::RowIndexes(col.get_true_indexes())
                    }
                    CalcNode::Selects { col_ids: _ } => {
                        self._print_debug();
                        panic!("Invalid select for id={}", calc_node_id);
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
            CalcResult::GroupByPartition(_, row_indexes) => row_indexes,
            _ => panic!(),
        }
    }

    fn eval_column(&mut self, node_id: CalcNodeId) -> &Column {
        match self.eval_calc_node(node_id) {
            CalcResult::Column(column) => column,
            _ => panic!(),
        }
    }

    fn eval_partition_from_calc_node(&mut self, calc_node_id: CalcNodeId) -> &Partition {
        let partition_id = self.get_partition_id(calc_node_id);
        self.eval_partition_from_partition_id(partition_id)
    }

    fn eval_partition_from_partition_id(&mut self, partition_id: PartitionId) -> &Partition {
        let result = self.eval_calc_node(partition_id);
        match result {
            CalcResult::Partition(partition) => partition,
            CalcResult::GroupByPartition(partition, _) => partition,
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

#[derive(Debug, PartialEq, Hash, Clone)]
enum CalcNode {
    TableCol(CalcNodeTableCol),
    TablePartition(String),
    ScalarPartition,
    WhereRowIndexes(CalcNodeId),
    PartitionFromRowIndexes(PartitionId, CalcNodeId),
    Selects {
        col_ids: Vec<CalcNodeId>,
    },
    RePartition(CalcNodeRePartition),
    GroupByPartition {
        // A partition calculated from all columns on a table
        source_id: CalcNodeId,
    },
    ReOrderAndRePartition(ReOrderAndRePartition),
    Aggregation {
        col_id: CalcNodeId,
        typ: AggregationType,
        partition_id: PartitionId,
    },
    Count(CalcNodeCount),
    Add(CalcNodeId, CalcNodeId),
    Divide(CalcNodeId, CalcNodeId),
    Literal(CalcNodeLiteral),
    Alias(CalcNodeId, String),
    OrderByRowIndexes(OrderByRowIndexes),
    OrderColumn(OrderColumn),
    ColSpread(CalcNodeId, PartitionLevel),
    SpreadScalarCol(CalcNodeId, PartitionId),
    GetUngroupPartition(CalcNodeGetUngroupPartition),
    LimitRowIndexes(PartitionId, usize),
    AggregatedPartition(PartitionLevel),
}

#[derive(Debug, PartialEq, Hash, Clone)]
enum CalcNodeLiteral {
    Integer(u64),
    Float64(CalcNodeFloat64),
    Bool(bool),
}

impl Eq for CalcNode {}

#[derive(Debug, Clone, Copy, PartialEq, Hash)]
struct ReOrderAndRePartition {
    col_id: CalcNodeId,
    partition_id: CalcNodeId,
    row_indexes_id: CalcNodeId,
}
#[derive(Debug, Clone, PartialEq, Hash)]
struct OrderByRowIndexes {
    source_id: CalcNodeId,
    order_cols_id: CalcNodeId,
    directions: Vec<SortOrderDirection>,
}

#[derive(Debug, Clone, Copy, PartialEq, Hash)]
struct OrderColumn {
    col_id: CalcNodeId,
    row_indexes_id: CalcNodeId,
    partition_id: PartitionId,
}

#[derive(Debug, PartialEq, Hash, Clone)]
struct CalcNodeRePartition {
    col_id: CalcNodeId,
    partition: CalcNodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Hash)]
struct CalcNodeGetUngroupPartition {
    source_id: CalcNodeId,
    group_partition_id: CalcNodeId,
    result_id: CalcNodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Hash)]
struct CalcNodeCount {
    in_partition_id: PartitionId,
    out_partition_id: PartitionId,
}

#[derive(Debug, Clone, Copy, PartialEq, Hash)]
struct CalcNodeLimitColumn {
    col_id: CalcNodeId,
    row_indexes_id: CalcNodeId,
}
#[derive(Debug, Clone, PartialEq, Hash)]
struct CalcNodeTableCol {
    table_name: String,
    col_name: String,
    partition_id: PartitionId,
}

#[derive(Debug, PartialEq, Clone)]
struct CalcNodeFloat64 {
    val: f64,
}

impl std::hash::Hash for CalcNodeFloat64 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.val.to_bits().hash(state);
    }
}

type CalcNodeId = usize;
