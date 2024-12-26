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

        let col_ids = calc_ctx
            .get_calc_node_cols(root_id)
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        let col_names = col_ids
            .iter()
            .cloned()
            .map(|id| calc_ctx.get_calc_node_name2(id, 0).to_string())
            .collect::<Vec<_>>();

        let result = calc_ctx.eval_calc_node(root_id);

        let table = Table::from_columns(
            Partition::new_single_partition(result.cols[0].len()),
            std::iter::zip(col_names, &result.cols).map(|(name, column)| TableColumnWrapper {
                name,
                column: column.clone(),
            }),
        );

        // calc_ctx.print_debug();

        Ok(table)
    }
}

#[derive(Clone)]
struct CalcResult {
    cols: Vec<Column>,
    partition: Partition,
    is_scalar: bool,
}

struct CalcNodeCtx<'a> {
    table_collection: &'a mut TableCollection,
    calc_nodes: Vec<CalcNode>,
    registered_tables: HashMap<String, CalcNodeId>,
    name_cache2: HashMap<(CalcNodeId, usize), String>,
    node_cols_cache: HashMap<CalcNodeId, Vec<CalcNodeId>>,
    table_col_node_cache: HashMap<(String, String), CalcNodeId>,
    result_cache: HashMap<CalcNodeId, CalcResult>,
}

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

    fn get_calc_node_name2(&mut self, id: CalcNodeId, col_idx: usize) -> &str {
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
                        self.get_calc_node_name2(table_id, col_idx).to_string()
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
                            result.push_str(self.get_calc_node_name2(*arg, 0));
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
                    } => self.get_calc_node_name2(*source_id, col_idx).to_string(),
                    CalcNode::GroupBy {
                        source_id: _,
                        partition_id: _,
                        get_id,
                    } => self.get_calc_node_name2(*get_id, col_idx).to_string(),
                    CalcNode::GroupByPartition {
                        source_id,
                        group_by_fields_id: _,
                    } => self.get_calc_node_name2(*source_id, col_idx).to_string(),
                    CalcNode::Selects { col_ids } => {
                        self.get_calc_node_name2(col_ids[col_idx], 0).to_string()
                    }
                    CalcNode::Add(left, right) => {
                        let left = *left;
                        let right = *right;

                        let mut result = String::new();
                        result.push_str("(");
                        result.push_str(self.get_calc_node_name2(left, 0));
                        result.push_str(" + ");
                        result.push_str(self.get_calc_node_name2(right, 0));
                        result.push_str(")");

                        result
                    }
                    CalcNode::GroupByGroupFields { group_by_id } => {
                        self.get_calc_node_name2(*group_by_id, col_idx).to_string()
                    }
                    CalcNode::Integer(val) => val.to_string(),
                    CalcNode::Float64(val) => val.to_string(),
                    CalcNode::Alias(_, name) => name.clone(),
                    CalcNode::OrderBy {
                        source_id,
                        orders_id: _,
                        directions: _,
                    } => self.get_calc_node_name2(*source_id, col_idx).to_string(),
                    CalcNode::Limit(source_id, _) => {
                        self.get_calc_node_name2(*source_id, col_idx).to_string()
                    }
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
                    CalcNode::GroupByPartition {
                        source_id,
                        group_by_fields_id: _,
                    } => {
                        let n_cols = self.get_calc_node_cols(*source_id).len();
                        cols.extend((0..n_cols).map(|col_idx| {
                            self.add_calc_node(CalcNode::FieldSelect {
                                source_id: id,
                                col_idx,
                            })
                        }));
                    }
                    CalcNode::GroupByGroupFields { group_by_id } => {
                        let n_cols = self.get_calc_node_cols(*group_by_id).len();
                        cols.extend((0..n_cols).map(|col_idx| {
                            self.add_calc_node(CalcNode::FieldSelect {
                                source_id: id,
                                col_idx,
                            })
                        }));
                    }
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
                    CalcNode::OrderBy {
                        source_id,
                        orders_id: _,
                        directions: _,
                    } => {
                        let n_cols = self.get_calc_node_cols(*source_id).len();

                        cols.extend((0..n_cols).map(|col_idx| {
                            self.add_calc_node(CalcNode::FieldSelect {
                                source_id: id,
                                col_idx,
                            })
                        }));
                    }
                    CalcNode::Limit(source_id, _) => {
                        let n_cols = self.get_calc_node_cols(*source_id).len();

                        cols.extend((0..n_cols).map(|col_idx| {
                            self.add_calc_node(CalcNode::FieldSelect {
                                source_id: id,
                                col_idx,
                            })
                        }));
                    }
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
            if self.get_calc_node_name2(col_id, 0) == col_name {
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

                self.add_calc_node(CalcNode::FieldSelect {
                    source_id: ctx.parent_id,
                    col_idx,
                })
            }
            AstNodeType::WhereStmt(condition) => {
                let condition_id = self.register_ast_node(ctx, condition);
                self.add_calc_node(CalcNode::Where {
                    source_id: ctx.parent_id,
                    where_col_id: condition_id,
                })
            }
            AstNodeType::GroupBy { group_by, get_expr } => {
                let group_by_id = self.register_select_list(ctx, group_by);
                let group_by_fields_id = self.add_calc_node(CalcNode::GroupByGroupFields {
                    // source_id: ctx.parent_id,
                    group_by_id,
                });
                let partition_id = self.add_calc_node(CalcNode::GroupByPartition {
                    source_id: ctx.parent_id,
                    group_by_fields_id,
                });

                // Create a list of result col IDs as first(group_cols) + get_cols
                let mut get_ids = Vec::<usize>::new();

                // Add first(group_field) for all group fields
                // These are first so they can be overriden in the get
                let group_by_col_ids = self
                    .get_calc_node_cols(group_by_fields_id)
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>();
                let group_by_first_col_ids = group_by_col_ids.iter().cloned().map(|col_id| {
                    let fcn_call_id = self.add_calc_node(CalcNode::FcnCall {
                        name: "first".to_string(),
                        args: vec![col_id],
                    });

                    let col_name = self.get_calc_node_name2(col_id, 0).to_string();

                    self.add_calc_node(CalcNode::Alias(fcn_call_id, col_name))
                });
                get_ids.extend(group_by_first_col_ids);

                let get_id = self.register_select_list(
                    &RegisterAstNodeCtx {
                        parent_id: partition_id,
                    },
                    get_expr,
                );
                for get_col_id in self.get_calc_node_cols(get_id) {
                    get_ids.push(*get_col_id);
                }

                let combined_get_id = self.add_calc_node(CalcNode::Selects { col_ids: get_ids });

                self.add_calc_node(CalcNode::GroupBy {
                    source_id: ctx.parent_id,
                    partition_id,
                    get_id: combined_get_id,
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
                self.add_calc_node(CalcNode::Add(left_id, right_id))
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

                let orders_id = self.add_calc_node(CalcNode::Selects {
                    col_ids: order_col_ids,
                });

                self.add_calc_node(CalcNode::OrderBy {
                    source_id: ctx.parent_id,
                    orders_id,
                    directions,
                })
            }
            AstNodeType::Limit(limit) => self.add_calc_node(CalcNode::Limit(ctx.parent_id, *limit)),
            _ => todo!("Unknown type {:?}", node),
        }
    }

    fn get_table_calc_node(&mut self, name: &str) -> CalcNodeId {
        match self.registered_tables.get(name) {
            None => {
                let node_id = self.add_calc_node(CalcNode::Table {
                    name: name.to_string(),
                });
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

                        CalcResult {
                            cols,
                            partition: Partition::new_single_partition(len),
                            is_scalar: false,
                        }
                    }
                    CalcNode::FieldSelect {
                        source_id: table_id,
                        col_idx,
                    } => {
                        let table_id = *table_id;
                        let col_idx = *col_idx;
                        let result = self.eval_calc_node(table_id);
                        // let table = match &result.result {
                        //     CalcResultType::Table(table) => table,
                        //     _ => panic!(),
                        // };

                        CalcResult {
                            cols: vec![result.cols[col_idx].clone()],
                            partition: result.partition.clone(),
                            // result: CalcResultType::Col(table.get_column(&col_name)),
                            is_scalar: result.is_scalar,
                        }
                    }
                    CalcNode::Where {
                        source_id: table_id,
                        where_col_id,
                    } => {
                        let table_id = *table_id;
                        let where_col_id = *where_col_id;

                        let condition_result = self.eval_calc_node(where_col_id);
                        assert_eq!(condition_result.cols.len(), 1);
                        let condition_col = &condition_result.cols[0];
                        let true_indexes = condition_col.get_true_indexes();

                        let in_result = self.eval_calc_node(table_id);
                        let cols = in_result
                            .cols
                            .iter()
                            .map(|col| col.from_indexes(&true_indexes))
                            .collect::<Vec<_>>();
                        let partition = in_result.partition.filter_indexes(&true_indexes);

                        CalcResult {
                            cols,
                            partition,
                            is_scalar: false,
                        }
                    }
                    CalcNode::FcnCall { name, args } => match name.as_str() {
                        "sum" => {
                            assert_eq!(args.len(), 1);
                            let val = self.eval_calc_node(args[0]);

                            let cols = val
                                .cols
                                .iter()
                                .map(|col| {
                                    col.aggregate_partition(&val.partition, &AggregationType::Sum)
                                })
                                .collect::<Vec<_>>();

                            CalcResult {
                                cols,
                                partition: val.partition.get_single_value_partition(),
                                is_scalar: false,
                            }
                        }
                        "first" => {
                            assert_eq!(args.len(), 1);
                            let val = self.eval_calc_node(args[0]);

                            let cols = val
                                .cols
                                .iter()
                                .map(|col| {
                                    col.aggregate_partition(&val.partition, &AggregationType::First)
                                })
                                .collect::<Vec<_>>();

                            CalcResult {
                                cols,
                                partition: val.partition.get_single_value_partition(),
                                is_scalar: false,
                            }
                        }
                        "avg" => {
                            assert_eq!(args.len(), 1);
                            let val = self.eval_calc_node(args[0]);

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

                            CalcResult {
                                cols,
                                partition: val.partition.get_single_value_partition(),
                                is_scalar: false,
                            }
                        }
                        _ => todo!("Fcn is not implemented: {}", name),
                    },
                    CalcNode::Selects { col_ids } => {
                        let col_ids = col_ids.clone();
                        let mut cols = Vec::<Column>::new();
                        let mut is_scalar: Option<bool> = None;
                        let mut partition: Option<Partition> = None;
                        for col_id in col_ids {
                            let col_result = self.eval_calc_node(col_id);

                            match partition {
                                None => {
                                    partition = Some(col_result.partition.clone());
                                    is_scalar = Some(col_result.is_scalar);
                                }
                                _ => {}
                            }
                            cols.extend(col_result.cols.clone());
                        }

                        let partition = partition.unwrap();
                        let is_scalar = is_scalar.unwrap();

                        CalcResult {
                            cols,
                            partition,
                            is_scalar,
                        }
                    }
                    CalcNode::GroupBy {
                        source_id,
                        partition_id,
                        get_id,
                    } => {
                        let source_id = *source_id;
                        let partition_id = *partition_id;
                        let get_id = *get_id;

                        let source_resp = self.eval_calc_node(source_id).clone();

                        let partition_resp = self.eval_calc_node(partition_id).clone();

                        let get_resp = self.eval_calc_node(get_id);

                        let result_part = Partition::undo_group_by(
                            &source_resp.partition,
                            &partition_resp.partition,
                            &get_resp.partition,
                        );

                        CalcResult {
                            cols: get_resp.cols.clone(),
                            partition: result_part,
                            is_scalar: false,
                        }
                    }
                    CalcNode::GroupByPartition {
                        source_id,
                        group_by_fields_id,
                    } => {
                        let source_id = *source_id;
                        let group_by_fields_id = *group_by_fields_id;

                        let partition = self.eval_calc_node(group_by_fields_id).partition.clone();
                        let source_result = self.eval_calc_node(source_id);

                        CalcResult {
                            cols: source_result.cols.clone(),
                            partition,
                            is_scalar: false,
                        }
                    }
                    CalcNode::GroupByGroupFields {
                        // source_id: _,
                        group_by_id,
                    } => {
                        let group_by_id = *group_by_id;

                        let group_by_result = self.eval_calc_node(group_by_id);
                        let partition =
                            Column::group_by(&group_by_result.cols, &group_by_result.partition);

                        CalcResult {
                            cols: group_by_result.cols.clone(),
                            partition,
                            is_scalar: false,
                        }
                    }
                    CalcNode::Add(left_id, right_id) => {
                        let left_id = *left_id;
                        let right_id = *right_id;

                        let left_result = self.eval_calc_node(left_id);
                        assert_eq!(left_result.cols.len(), 1);
                        let mut left_col = left_result.cols[0].clone();
                        let left_is_scalar = left_result.is_scalar;
                        let left_partition = left_result.partition.clone();

                        let right_result = self.eval_calc_node(right_id);
                        assert_eq!(right_result.cols.len(), 1);
                        let mut right_col = right_result.cols[0].clone();
                        let right_is_scalar = right_result.is_scalar;

                        if left_is_scalar && !right_is_scalar {
                            left_col = left_col.repeat_scalar_col(right_col.len());
                        } else if right_is_scalar && !left_is_scalar {
                            right_col = right_col.repeat_scalar_col(left_col.len());
                        }

                        CalcResult {
                            cols: vec![&left_col + &right_col],
                            partition: left_partition,
                            is_scalar: left_is_scalar && right_is_scalar,
                        }
                    }
                    CalcNode::Integer(val) => CalcResult {
                        cols: vec![Column::from_repeated_f64(*val as f64, 1)],
                        partition: Partition::new_single_partition(1),
                        is_scalar: true,
                    },
                    CalcNode::Float64(val) => CalcResult {
                        cols: vec![Column::from_repeated_f64(*val, 1)],
                        partition: Partition::new_single_partition(1),
                        is_scalar: true,
                    },
                    CalcNode::Alias(col_id, _) => self.eval_calc_node(*col_id).clone(),
                    CalcNode::OrderBy {
                        source_id,
                        orders_id,
                        directions,
                    } => {
                        let source_id = *source_id;
                        let orders_id = *orders_id;
                        let directions = directions.clone();
                        let source = self.eval_calc_node(source_id).clone();

                        let orders_result = self.eval_calc_node(orders_id).clone();

                        let sort_indexes = Column::get_sorted_indexes(
                            &orders_result.cols,
                            &directions,
                            source.cols[0].len(),
                            &source.partition,
                        );
                        let result_cols = source
                            .cols
                            .iter()
                            .map(|col| col.from_indexes(&sort_indexes))
                            .collect::<Vec<_>>();

                        CalcResult {
                            cols: result_cols,
                            partition: source.partition.reset_row_indexes(),
                            is_scalar: false,
                        }
                    }
                    CalcNode::Limit(source_id, limit) => {
                        let source_id = *source_id;
                        let limit = *limit;
                        let source_result = self.eval_calc_node(source_id);

                        let result_cols = source_result
                            .cols
                            .iter()
                            .map(|col| col.limit(limit, &source_result.partition))
                            .collect::<Vec<_>>();

                        CalcResult {
                            cols: result_cols,
                            partition: source_result.partition.limit(limit),
                            is_scalar: false,
                        }
                    }
                };
                self.result_cache.insert(calc_node_id, result);
            }
            _ => {}
        }

        &self.result_cache[&calc_node_id]
    }

    fn _print_debug(&mut self) {
        for calc_node_id in 0..self.calc_nodes.len() {
            let calc_node = &self.calc_nodes[calc_node_id];
            println!("{}: {:?}", calc_node_id, calc_node);
            let col_ids = self
                .get_calc_node_cols(calc_node_id)
                .iter()
                .cloned()
                .collect::<Vec<_>>();

            for (col_idx, col_id) in col_ids.iter().cloned().enumerate() {
                println!(
                    "  - {}: {}",
                    col_id,
                    self.get_calc_node_name2(calc_node_id, col_idx)
                );
            }
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
    GroupByGroupFields {
        // A table of group by fields with grouped partition
        group_by_id: CalcNodeId,
    },
    GroupByPartition {
        // A table of source columns but with grouped partition
        source_id: CalcNodeId,
        group_by_fields_id: CalcNodeId,
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
    OrderBy {
        source_id: CalcNodeId,
        orders_id: CalcNodeId,
        directions: Vec<SortOrderDirection>,
    },
    Limit(CalcNodeId, usize),
}

type CalcNodeId = usize;
