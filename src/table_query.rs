use crate::ast_node::{AstNode, AstNodeType};
use crate::{Column, TableColumnWrapper};

use crate::column::AggregationType;
use crate::parser::{ParseError, Parser};
use crate::table::{Aggregation, Table};

impl Table {
    pub fn query(&self, query: &str) -> Result<Table, ParseError> {
        let mut parser = Parser::new(query);
        let ast = match parser.external_parse_expr() {
            Err(err) => return Err(err),
            Ok(ast) => ast,
        };

        let mut table: Table = self.clone();

        for stmt in ast.iter_list() {
            println!("Stmt: {:?}", stmt);
            match stmt.get_type() {
                AstNodeType::SelectStmt(selects) => {
                    let len = selects
                        .iter_list()
                        .fold(None, |acc, select| match acc {
                            Some(acc) => Some(acc),
                            None => self.get_expr_len(select),
                        })
                        .unwrap_or(1);

                    table =
                        Table::from_columns(selects.iter_list().map(|select| TableColumnWrapper {
                            name: table.get_expr_name(select),
                            column: table.eval_col_expr(select, len),
                        }));
                }
                AstNodeType::WhereStmt(stmt) => {
                    let col_name = match stmt.get_type() {
                        AstNodeType::Identifier(col_name) => col_name.clone(),
                        _ => todo!(),
                    };

                    table = table.where_col_is_true(&col_name)
                }
                AstNodeType::GroupBy { group_by, get_expr } => {
                    let group_col_name_refs = group_by
                        .iter_list()
                        .map(|col| match col.get_type() {
                            AstNodeType::Identifier(name) => name.as_str(),
                            _ => todo!("Add support for {:?}", col.get_type()),
                        })
                        .collect::<Vec<_>>();

                    let aggs = get_expr
                        .iter_list()
                        .map(|get_expr| match get_expr.get_type() {
                            AstNodeType::FcnCall {
                                name: fcn_name,
                                args,
                            } => match fcn_name.get_type() {
                                AstNodeType::Identifier(fcn_name) => match fcn_name.as_str() {
                                    "sum" => {
                                        let col_name = match args {
                                            Some(args) => {
                                                match args.iter_list().nth(0).unwrap().get_type() {
                                                    AstNodeType::Identifier(col_name) => {
                                                        col_name.as_str()
                                                    }
                                                    _ => todo!(),
                                                }
                                            }
                                            _ => todo!(),
                                        };

                                        Aggregation {
                                            in_col_name: col_name,
                                            out_col_name: col_name,
                                            agg_type: AggregationType::Sum,
                                        }
                                    }
                                    "first" => {
                                        let col_name = match args {
                                            Some(args) => {
                                                match args.iter_list().nth(0).unwrap().get_type() {
                                                    AstNodeType::Identifier(col_name) => {
                                                        col_name.as_str()
                                                    }
                                                    _ => todo!(),
                                                }
                                            }
                                            _ => todo!(),
                                        };

                                        Aggregation {
                                            in_col_name: col_name,
                                            out_col_name: col_name,
                                            agg_type: AggregationType::First,
                                        }
                                    }
                                    _ => todo!(),
                                },
                                _ => todo!(),
                            },
                            _ => todo!(),
                        })
                        .collect::<Vec<_>>();

                    table =
                        table.group_and_aggregate(group_col_name_refs.as_slice(), aggs.as_slice())
                }
                _ => todo!(),
            }
        }

        Ok(table)
    }

    fn eval_col_expr(&self, expr: &AstNode, len: usize) -> Column {
        match expr.get_type() {
            AstNodeType::Identifier(col_name) => self.get_column(col_name.as_str()),
            AstNodeType::Add(left, right) => {
                self.eval_col_expr(left, len) + self.eval_col_expr(right, len)
            }
            AstNodeType::Float64(val) => Column::from_repeated_f64(*val, len),
            AstNodeType::Integer(val) => Column::from_repeated_f64(*val as f64, len),
            AstNodeType::Alias { expr, alias: _ } => self.eval_col_expr(expr, len),
            _ => todo!(),
        }
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

    fn get_expr_len(&self, expr: &AstNode) -> Option<usize> {
        match expr.get_type() {
            AstNodeType::Identifier(_) => Some(self.get_n_rows()),
            AstNodeType::Float64(_) => None,
            AstNodeType::Integer(_) => None,
            AstNodeType::Add(left, right) => match self.get_expr_len(left) {
                Some(len) => Some(len),
                None => self.get_expr_len(right),
            },
            AstNodeType::Alias {
                expr: node,
                alias: _,
            } => self.get_expr_len(node),
            _ => todo!(),
        }
    }
}
