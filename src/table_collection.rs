use std::{
    collections::{
        hash_map::{Entry, OccupiedEntry, VacantEntry},
        HashMap,
    },
    hash::Hash,
};

use crate::{
    ast_node::{AstNode, AstNodeType},
    parser::{ParseError, Parser},
    Aggregation, AggregationType, ColType, Column, Table, TableColumnWrapper,
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

    fn get_table(&self, name: &str) -> &Table {
        &self.table_map[name]
    }

    pub fn query(&self, query: &str) -> Result<Table, ParseError> {
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

        let table = Table::from_columns(col_ids.iter().cloned().map(|col_id| TableColumnWrapper {
            name: calc_ctx.get_calc_node_name(col_id).to_string(),
            column: match &calc_ctx.eval_calc_node(col_id).result {
                CalcResultType::Col(col) => col.clone(),
                _ => panic!(),
            },
        }));

        Ok(table)
    }
}

struct CalcResult {
    result: CalcResultType,
    len: CalcResultLen,
}

#[derive(Clone)]
enum CalcResultLen {
    Scalar,
    Len(usize),
}

enum CalcResultType {
    Col(Column),
    Table(Table),
}

#[derive(Clone, Copy, Debug)]
enum LenExpr {
    Scalar,
    NodeId(usize),
}

struct CalcNodeCtx<'a> {
    table_collection: &'a TableCollection,
    calc_nodes: Vec<CalcNode2>,
    len_node_idx: usize,
    registered_tables: HashMap<String, CalcNodeId>,
    len_expr_cache: HashMap<CalcNodeId, LenExpr>,
    name_cache: HashMap<CalcNodeId, String>,
    node_cols_cache: HashMap<CalcNodeId, Vec<CalcNodeId>>,
    table_col_node_cache: HashMap<(String, String), CalcNodeId>,
    result_cache: HashMap<CalcNodeId, CalcResult>,
}

#[derive(Clone)]
struct RegisterAstNodeCtx {
    parent_id: CalcNodeId,
}

impl<'a> CalcNodeCtx<'a> {
    fn new(table_collection: &'a TableCollection) -> Self {
        CalcNodeCtx {
            table_collection,
            calc_nodes: Vec::new(),
            len_node_idx: 0,
            registered_tables: HashMap::new(),
            len_expr_cache: HashMap::new(),
            name_cache: HashMap::new(),
            node_cols_cache: HashMap::new(),
            table_col_node_cache: HashMap::new(),
            result_cache: HashMap::new(),
        }
    }

    fn get_new_len_node_idx(&mut self) -> usize {
        let result = self.len_node_idx;
        self.len_node_idx += 1;
        result
    }

    fn add_calc_node(&mut self, node: CalcNode2) -> CalcNodeId {
        let idx = self.calc_nodes.len();
        self.calc_nodes.push(node);
        idx
    }

    fn get_calc_node(&self, id: CalcNodeId) -> &CalcNode2 {
        &self.calc_nodes[id]
    }

    fn get_calc_node_len(&mut self, id: CalcNodeId) -> &LenExpr {
        match self.len_expr_cache.get(&id) {
            None => {
                let len_expr = match self.get_calc_node(id) {
                    CalcNode2::Table { name: _ } => LenExpr::NodeId(self.get_new_len_node_idx()),
                    CalcNode2::FieldSelect {
                        table_id,
                        col_name: _,
                    } => *self.get_calc_node_len(*table_id),
                    _ => todo!(),
                };
                self.len_expr_cache.insert(id, len_expr);
            }
            _ => {}
        };

        &self.len_expr_cache[&id]
    }

    fn get_calc_node_name(&mut self, id: CalcNodeId) -> &str {
        match self.name_cache.get(&id) {
            None => {
                let name = match self.get_calc_node(id) {
                    CalcNode2::FieldSelect {
                        table_id: _,
                        col_name: field_name,
                    } => field_name.clone(),
                    _ => todo!(),
                };
                self.name_cache.insert(id, name);
            }
            _ => {}
        }

        self.name_cache[&id].as_str()
    }

    fn get_table_col_node_id(&mut self, table_id: CalcNodeId, col_name: &str) -> CalcNodeId {
        let table_name = match self.get_calc_node(table_id) {
            CalcNode2::Table { name } => name.clone(),
            _ => panic!(),
        };

        let key = (table_name, col_name.to_string());
        match self.table_col_node_cache.get(&key) {
            None => {
                let calc_node = CalcNode2::FieldSelect {
                    table_id,
                    col_name: col_name.to_string(),
                };
                let new_id = self.add_calc_node(calc_node);
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
                    CalcNode2::Table { name } => {
                        let table = self.table_collection.get_table(name);
                        for col in table.col_iter() {
                            cols.push(self.get_table_col_node_id(id, &col.name));
                        }
                    }
                    CalcNode2::FieldSelect {
                        table_id: _,
                        col_name: _,
                    } => {
                        cols.push(id);
                    }
                    CalcNode2::Selects { cols: select_cols } => {
                        cols.extend(select_cols);
                    }
                    CalcNode2::FcnCall { name: _, args: _ } => {
                        cols.push(id);
                    }
                    _ => todo!("{:?}", self.get_calc_node(id)),
                };

                self.node_cols_cache.insert(id, cols);
            }
            _ => {}
        };

        self.node_cols_cache[&id].as_slice()
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

                self.add_calc_node(CalcNode2::Selects {
                    cols: select_node_ids,
                })
            }
            AstNodeType::Identifier(col_name) => self.add_calc_node(CalcNode2::FieldSelect {
                table_id: ctx.parent_id,
                col_name: col_name.clone(),
            }),
            AstNodeType::WhereStmt(condition) => {
                let condition_id = self.register_ast_node(ctx, condition);
                self.add_calc_node(CalcNode2::Where {
                    table_id: ctx.parent_id,
                    where_col_id: condition_id,
                })
            }
            AstNodeType::GroupBy { group_by, get_expr } => {
                let group_by_cols = self.get_ast_col_ids(ctx, group_by);
                let get_cols = self.get_ast_col_ids(ctx, get_expr);

                self.add_calc_node(CalcNode2::GroupBy {
                    group_by_cols,
                    get_cols,
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

                self.add_calc_node(CalcNode2::FcnCall {
                    name: fcn_name.to_string(),
                    args: arg_ids,
                })
            }
            _ => todo!("Unknown type {:?}", node),
        }
    }

    fn get_table_calc_node(&mut self, name: &str) -> CalcNodeId {
        match self.registered_tables.get(name) {
            None => {
                let node_id = self.add_calc_node(CalcNode2::Table {
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
                    CalcNode2::Table { name } => {
                        let table = self.table_collection.get_table(name).clone();
                        let len = CalcResultLen::Len(table.get_n_rows());

                        CalcResult {
                            result: CalcResultType::Table(table),
                            len,
                        }
                    }
                    CalcNode2::FieldSelect { table_id, col_name } => {
                        let table_id = *table_id;
                        let col_name = col_name.clone();
                        let result = self.eval_calc_node(table_id);
                        let table = match &result.result {
                            CalcResultType::Table(table) => table,
                            _ => panic!(),
                        };

                        CalcResult {
                            result: CalcResultType::Col(table.get_column(&col_name)),
                            len: result.len.clone(),
                        }
                    }
                    CalcNode2::Where {
                        table_id,
                        where_col_id,
                    } => {
                        let table_id = *table_id;
                        let where_col_id = *where_col_id;

                        let condition_result = match &self.eval_calc_node(where_col_id).result {
                            CalcResultType::Col(col) => col.clone(),
                            _ => panic!(),
                        };
                        let table_result = match &self.eval_calc_node(table_id).result {
                            CalcResultType::Table(table) => table,
                            _ => panic!(),
                        };

                        let result = table_result.where_col_is_true(&condition_result);
                        let len = result.get_n_rows();

                        CalcResult {
                            result: CalcResultType::Table(result),
                            len: CalcResultLen::Len(len),
                        }
                    }
                    _ => todo!("{:?}", self.get_calc_node(calc_node_id)),
                };
                self.result_cache.insert(calc_node_id, result);
            }
            _ => {}
        }

        &self.result_cache[&calc_node_id]
    }
}

#[derive(Debug)]
enum CalcNode2 {
    FieldSelect {
        table_id: CalcNodeId,
        col_name: String,
    },
    Where {
        table_id: CalcNodeId,
        where_col_id: CalcNodeId,
    },
    Table {
        name: String,
    },
    Selects {
        cols: Vec<CalcNodeId>,
    },
    GroupBy {
        group_by_cols: Vec<CalcNodeId>,
        get_cols: Vec<CalcNodeId>,
    },
    FcnCall {
        name: String,
        args: Vec<CalcNodeId>,
    },
}

type CalcNodeId = usize;
