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
                    table =
                        Table::from_columns(selects.iter_list().map(|select| TableColumnWrapper {
                            name: table.eval_expr_name(select),
                            column: table.eval_col_expr(select),
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

    fn eval_col_expr(&self, expr: &AstNode) -> Column {
        match expr.get_type() {
            AstNodeType::Identifier(col_name) => self.get_column(col_name.as_str()),
            AstNodeType::Add(left, right) => match right.get_type() {
                AstNodeType::Float64(right_val) => self.eval_col_expr(left).add_f64(*right_val),
                AstNodeType::Integer(right_val) => {
                    self.eval_col_expr(left).add_f64(*right_val as f64)
                }
                _ => self.eval_col_expr(left) + self.eval_col_expr(right),
            },
            _ => todo!(),
        }
    }

    fn eval_expr_name(&self, expr: &AstNode) -> String {
        match expr.get_type() {
            AstNodeType::Identifier(col_name) => col_name.clone(),
            AstNodeType::Add(left, right) => format!(
                "({} + {})",
                self.eval_expr_name(left),
                self.eval_expr_name(right)
            ),
            AstNodeType::Integer(val) => format!("{}", val),
            _ => todo!(),
        }
    }
}
