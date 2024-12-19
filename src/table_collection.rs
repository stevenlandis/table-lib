use std::collections::{
    hash_map::{Entry, OccupiedEntry, VacantEntry},
    HashMap,
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

        match calc_ctx.eval_calc_node(root_id).result {
            CalcResultType::Col(col) => Ok(Table::from_columns([TableColumnWrapper {
                name: calc_ctx.get_calc_node(root_id).get_name(),
                column: col,
            }])),
            CalcResultType::Table(table) => Ok(table),
        }

        // for stmt in ast.iter_list() {
        //     println!("Stmt: {:?}", stmt);
        //     match stmt.get_type() {
        //         AstNodeType::SelectStmt(selects) => {
        //             let node_id = ctx.get_len_node_idx();
        //             let select_lens = selects
        //                 .iter_list()
        //                 .map(|select| get_expr_len(&mut ctx, node_id, select))
        //                 .collect::<Vec<_>>();

        //             let mut len_expr: Option<LenExpr> = None;
        //             for this_len in &select_lens {
        //                 len_expr = match len_expr {
        //                     None => Some(*this_len),
        //                     Some(len) => Some(match len {
        //                         LenExpr::Scalar => match this_len {
        //                             LenExpr::Scalar => LenExpr::Scalar,
        //                             LenExpr::NodeId(_) => *this_len,
        //                         },
        //                         LenExpr::NodeId(node_id) => match this_len {
        //                             LenExpr::Scalar => len,
        //                             LenExpr::NodeId(this_id) => {
        //                                 assert_eq!(node_id, *this_id);
        //                                 len
        //                             }
        //                         },
        //                     }),
        //                 }
        //             }
        //             let len_expr = len_expr.unwrap();

        //             let mut columns = selects
        //                 .iter_list()
        //                 .map(|select| table.eval_col_expr(select))
        //                 .collect::<Vec<_>>();

        //             // get final length
        //             let len: usize = match len_expr {
        //                 LenExpr::Scalar => 1,
        //                 LenExpr::NodeId(_) => {
        //                     let mut len: Option<usize> = None;

        //                     for (len_expr, col) in std::iter::zip(&select_lens, &columns) {
        //                         match len_expr {
        //                             LenExpr::Scalar => {}
        //                             LenExpr::NodeId(_) => match len {
        //                                 None => {
        //                                     len = Some(col.len());
        //                                 }
        //                                 Some(len) => {
        //                                     assert_eq!(len, col.len());
        //                                 }
        //                             },
        //                         }
        //                     }
        //                     len.unwrap()
        //                 }
        //             };

        //             // repeat scalar column to match other column length
        //             for (len_expr, col) in std::iter::zip(&select_lens, columns.iter_mut()) {
        //                 match len_expr {
        //                     LenExpr::Scalar => *col = col.repeat_scalar_col(len),
        //                     LenExpr::NodeId(_) => {}
        //                 }
        //             }

        //             table =
        //                 Table::from_columns(selects.iter_list().map(|select| TableColumnWrapper {
        //                     name: table.get_expr_name(select),
        //                     column: table.eval_col_expr(select),
        //                 }));
        //         }
        //         AstNodeType::WhereStmt(stmt) => {
        //             let col_name = match stmt.get_type() {
        //                 AstNodeType::Identifier(col_name) => col_name.clone(),
        //                 _ => todo!(),
        //             };

        //             table = table.where_col_is_true(&col_name)
        //         }
        //         AstNodeType::GroupBy { group_by, get_expr } => {
        //             let group_col_name_refs = group_by
        //                 .iter_list()
        //                 .map(|col| match col.get_type() {
        //                     AstNodeType::Identifier(name) => name.as_str(),
        //                     _ => todo!("Add support for {:?}", col.get_type()),
        //                 })
        //                 .collect::<Vec<_>>();

        //             let aggs = get_expr
        //                 .iter_list()
        //                 .map(|get_expr| match get_expr.get_type() {
        //                     AstNodeType::FcnCall {
        //                         name: fcn_name,
        //                         args,
        //                     } => match fcn_name.get_type() {
        //                         AstNodeType::Identifier(fcn_name) => match fcn_name.as_str() {
        //                             "sum" => {
        //                                 let col_name = match args {
        //                                     Some(args) => {
        //                                         match args.iter_list().nth(0).unwrap().get_type() {
        //                                             AstNodeType::Identifier(col_name) => {
        //                                                 col_name.as_str()
        //                                             }
        //                                             _ => todo!(),
        //                                         }
        //                                     }
        //                                     _ => todo!(),
        //                                 };

        //                                 Aggregation {
        //                                     in_col_name: col_name,
        //                                     out_col_name: col_name,
        //                                     agg_type: AggregationType::Sum,
        //                                 }
        //                             }
        //                             "first" => {
        //                                 let col_name = match args {
        //                                     Some(args) => {
        //                                         match args.iter_list().nth(0).unwrap().get_type() {
        //                                             AstNodeType::Identifier(col_name) => {
        //                                                 col_name.as_str()
        //                                             }
        //                                             _ => todo!(),
        //                                         }
        //                                     }
        //                                     _ => todo!(),
        //                                 };

        //                                 Aggregation {
        //                                     in_col_name: col_name,
        //                                     out_col_name: col_name,
        //                                     agg_type: AggregationType::First,
        //                                 }
        //                             }
        //                             _ => todo!(),
        //                         },
        //                         _ => todo!(),
        //                     },
        //                     _ => todo!(),
        //                 })
        //                 .collect::<Vec<_>>();

        //             table =
        //                 table.group_and_aggregate(group_col_name_refs.as_slice(), aggs.as_slice())
        //         }
        //         _ => todo!(),
        //     }
        // }

        // Ok(table)
    }

    fn get_expr_name(&self, expr: &AstNode) -> String {
        match expr.get_type() {
            AstNodeType::Identifier(col_name) => col_name.clone(),
            AstNodeType::Add(left, right) => format!(
                "({} + {})",
                self.get_expr_name(left),
                self.get_expr_name(right)
            ),
            AstNodeType::Integer(val) => format!("{}", val),
            AstNodeType::Float64(val) => format!("{}", val),
            AstNodeType::Alias { expr: _, alias } => match alias.get_type() {
                AstNodeType::Identifier(alias) => alias.clone(),
                _ => todo!(),
            },
            _ => todo!(),
        }
    }
}

struct CalcResult {
    result: CalcResultType,
    len: CalcResultLen,
}

enum CalcResultLen {
    Scalar,
    Len(usize),
}

enum CalcResultType {
    Col(Column),
    Table(Table),
}

struct ExprCtx {
    len_node_idx: usize,
}

impl ExprCtx {
    fn new() -> Self {
        ExprCtx { len_node_idx: 0 }
    }

    fn get_len_node_idx(&mut self) -> usize {
        let result = self.len_node_idx;
        self.len_node_idx += 1;
        result
    }
}

#[derive(Clone, Copy, Debug)]
enum LenExpr {
    Scalar,
    NodeId(usize),
}

fn get_expr_len(ctx: &mut ExprCtx, in_node_id: usize, expr: &AstNode) -> LenExpr {
    match expr.get_type() {
        AstNodeType::Identifier(_) => LenExpr::NodeId(in_node_id),
        AstNodeType::Float64(_) => LenExpr::Scalar,
        AstNodeType::Integer(_) => LenExpr::Scalar,
        AstNodeType::Add(left, right) => {
            let left_len = get_expr_len(ctx, in_node_id, left);
            let right_len = get_expr_len(ctx, in_node_id, right);

            match left_len {
                LenExpr::Scalar => match right_len {
                    LenExpr::Scalar => LenExpr::Scalar,
                    LenExpr::NodeId(_) => right_len,
                },
                LenExpr::NodeId(left_id) => match right_len {
                    LenExpr::Scalar => left_len,
                    LenExpr::NodeId(right_id) => {
                        assert_eq!(left_id, right_id);
                        LenExpr::NodeId(left_id)
                    }
                },
            }
        }
        AstNodeType::Alias {
            expr: node,
            alias: _,
        } => get_expr_len(ctx, in_node_id, node),
        _ => todo!(),
    }
}

struct CalcNodeCtx<'a> {
    table_collection: &'a TableCollection,
    calc_nodes: Vec<CalcNode>,
    len_node_idx: usize,
    registered_tables: HashMap<String, CalcNodeId>,
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
        }
    }

    fn get_new_len_node_idx(&mut self) -> usize {
        let result = self.len_node_idx;
        self.len_node_idx += 1;
        result
    }

    fn add_calc_node(&mut self, node: CalcNode) -> CalcNodeId {
        let idx = self.calc_nodes.len();
        self.calc_nodes.push(node);
        idx
    }

    fn get_calc_node(&self, id: CalcNodeId) -> &CalcNode {
        &self.calc_nodes[id]
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
                let mut len_expr: Option<LenExpr> = None;

                // get len_expr
                let mut select_node_ids = Vec::<CalcNodeId>::new();
                for select in selects.iter_list() {
                    let select_id = self.register_ast_node(ctx, select);
                    let select_node = self.get_calc_node(select_id);
                    let this_len = select_node.get_len_expr();

                    match len_expr {
                        None => len_expr = Some(select_node.get_len_expr()),
                        Some(len) => match len {
                            LenExpr::Scalar => match this_len {
                                LenExpr::Scalar => {}
                                LenExpr::NodeId(_) => {
                                    len_expr = Some(this_len);
                                }
                            },
                            LenExpr::NodeId(len_id) => match select_node.get_len_expr() {
                                LenExpr::Scalar => {}
                                LenExpr::NodeId(this_len_id) => {
                                    assert_eq!(len_id, this_len_id);
                                }
                            },
                        },
                    }

                    select_node_ids.push(select_id);
                }

                let len_expr = len_expr.unwrap();

                let mut col_schemas = Vec::<ColSchema>::new();
                for select_id in &select_node_ids {
                    let select_node = self.get_calc_node(*select_id);
                    match select_node.get_type() {
                        CalcNodeType::Table {
                            len_expr: _,
                            col_schemas: this_col_schemas,
                        } => {
                            col_schemas.extend(this_col_schemas.iter().cloned());
                        }
                        CalcNodeType::TextCol { len_expr: _ } => col_schemas.push(ColSchema {
                            name: select_node.get_name(),
                            typ: ColType::Text,
                        }),
                        _ => todo!(),
                    }
                }

                self.add_calc_node(CalcNode {
                    name: None,
                    typ: CalcNodeType::Table {
                        len_expr,
                        col_schemas,
                    },
                    def: CalcNodeDef::Selects(select_node_ids),
                })
            }
            AstNodeType::Identifier(col_name) => {
                let parent_node = self.get_calc_node(ctx.parent_id);
                match parent_node.get_type() {
                    CalcNodeType::Table {
                        len_expr,
                        col_schemas,
                    } => self.add_calc_node(CalcNode {
                        name: None,
                        typ: match col_schemas
                            .iter()
                            .find(|schema| schema.name == *col_name)
                            .unwrap()
                            .typ
                        {
                            ColType::Text => CalcNodeType::TextCol {
                                len_expr: *len_expr,
                            },
                            _ => todo!(),
                        },
                        def: CalcNodeDef::FieldSelect {
                            table_id: ctx.parent_id,
                            field_name: col_name.clone(),
                        },
                    }),
                    _ => panic!(),
                }
            }
            _ => todo!("Unknown type {:?}", node),
        }
    }

    fn get_table_calc_node(&mut self, name: &str) -> CalcNodeId {
        if !self.registered_tables.contains_key(name) {
            let table = self.table_collection.get_table(name);
            let node_id = self.register_table(name, table);
            self.registered_tables.insert(name.to_string(), node_id);
        }

        self.registered_tables[name]
    }

    fn register_table(&mut self, name: &str, table: &Table) -> CalcNodeId {
        let node = CalcNode {
            name: None,
            typ: CalcNodeType::Table {
                len_expr: LenExpr::NodeId(self.get_new_len_node_idx()),
                col_schemas: table
                    .col_iter()
                    .map(|col| ColSchema {
                        name: col.name.clone(),
                        typ: col.column.get_type(),
                    })
                    .collect::<Vec<_>>(),
            },
            def: CalcNodeDef::Table {
                name: name.to_string(),
            },
        };
        self.add_calc_node(node)
    }

    fn eval_calc_node(&self, calc_node_id: CalcNodeId) -> CalcResult {
        let node = self.get_calc_node(calc_node_id);
        match node.get_def() {
            CalcNodeDef::Table { name } => {
                let table = self.table_collection.get_table(name).clone();
                CalcResult {
                    len: CalcResultLen::Len(table.get_n_rows()),
                    result: CalcResultType::Table(table),
                }
            }
            CalcNodeDef::Selects(selects) => {
                let mut results = selects
                    .iter()
                    .map(|select_id| self.eval_calc_node(*select_id))
                    .collect::<Vec<_>>();
                assert!(results.len() > 0);

                let mut len: Option<usize> = None;
                for (result, select_id) in std::iter::zip(&results, selects) {
                    match self.get_calc_node(*select_id).get_len_expr() {
                        LenExpr::Scalar => match result.len {
                            CalcResultLen::Scalar => {}
                            CalcResultLen::Len(_) => panic!(),
                        },
                        LenExpr::NodeId(_) => match result.len {
                            CalcResultLen::Scalar => panic!(),
                            CalcResultLen::Len(this_len) => match len {
                                None => {
                                    len = Some(this_len);
                                }
                                Some(len) => {
                                    assert_eq!(this_len, len);
                                }
                            },
                        },
                    }
                }

                let len = match len {
                    None => CalcResultLen::Scalar,
                    Some(len) => CalcResultLen::Len(len),
                };

                // if fixed size, resize scalars to appropriate length
                match len {
                    CalcResultLen::Scalar => {}
                    CalcResultLen::Len(len) => {
                        for result in &mut results {
                            match &result.len {
                                CalcResultLen::Scalar => {
                                    *result = CalcResult {
                                        result: match &result.result {
                                            CalcResultType::Col(col) => {
                                                CalcResultType::Col(col.repeat_scalar_col(len))
                                            }
                                            CalcResultType::Table(table) => CalcResultType::Table(
                                                table.repeat_scalar_table(len),
                                            ),
                                        },
                                        len: CalcResultLen::Len(len),
                                    };
                                }
                                CalcResultLen::Len(_) => {}
                            }
                        }
                    }
                }

                // combine all results into a single table
                let mut columns = Vec::<TableColumnWrapper>::new();
                for (result, select_id) in std::iter::zip(results, selects) {
                    match result.result {
                        CalcResultType::Col(col) => columns.push(TableColumnWrapper {
                            column: col,
                            name: self.get_calc_node(*select_id).get_name(),
                        }),
                        CalcResultType::Table(table) => {
                            columns.extend(table.col_iter_owned());
                        }
                    }
                }

                CalcResult {
                    result: CalcResultType::Table(Table::from_columns(columns)),
                    len,
                }
            }
            CalcNodeDef::FieldSelect {
                table_id,
                field_name,
            } => {
                let table_result = self.eval_calc_node(*table_id);
                let table = match table_result.result {
                    CalcResultType::Table(table) => table,
                    _ => panic!(),
                };
                CalcResult {
                    result: CalcResultType::Col(table.get_column(field_name)),
                    len: table_result.len,
                }
            }
            _ => todo!(),
        }
    }
}

struct CalcNode {
    name: Option<String>,
    typ: CalcNodeType,
    def: CalcNodeDef,
}

impl CalcNode {
    fn get_def(&self) -> &CalcNodeDef {
        &self.def
    }

    fn get_type(&self) -> &CalcNodeType {
        &self.typ
    }

    fn get_len_expr(&self) -> LenExpr {
        match self.get_type() {
            CalcNodeType::TextCol { len_expr } => *len_expr,
            CalcNodeType::BoolCol { len_expr } => *len_expr,
            CalcNodeType::Float64Col { len_expr } => *len_expr,
            CalcNodeType::Table {
                len_expr,
                col_schemas: _,
            } => *len_expr,
        }
    }

    fn get_name(&self) -> String {
        match &self.name {
            Some(name) => name.clone(),
            None => match self.get_type() {
                CalcNodeType::Table {
                    len_expr: _,
                    col_schemas: _,
                } => unreachable!(),
                CalcNodeType::TextCol { len_expr } => match self.get_def() {
                    CalcNodeDef::FieldSelect {
                        table_id,
                        field_name,
                    } => field_name.clone(),
                    _ => todo!(),
                },
                _ => todo!("get name from {:?}", self.get_type()),
            },
        }
    }
}

type CalcNodeId = usize;

#[derive(Clone, Debug)]
struct ColSchema {
    name: String,
    typ: ColType,
}

#[derive(Clone, Debug)]
enum CalcNodeType {
    TextCol {
        len_expr: LenExpr,
    },
    BoolCol {
        len_expr: LenExpr,
    },
    Float64Col {
        len_expr: LenExpr,
    },
    Table {
        len_expr: LenExpr,
        col_schemas: Vec<ColSchema>,
    },
}

enum CalcNodeDef {
    FieldSelect {
        table_id: CalcNodeId,
        field_name: String,
    },
    Filter {
        table_id: CalcNodeId,
        condition_id: CalcNodeId,
    },
    Table {
        name: String,
    },
    Selects(Vec<CalcNodeId>),
}
