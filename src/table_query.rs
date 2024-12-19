// use crate::ast_node::{AstNode, AstNodeType};
// use crate::{ColType, Column, TableColumnWrapper};

// use crate::column::AggregationType;
// use crate::parser::{ParseError, Parser};
// use crate::table::{Aggregation, Table};

// impl Table {
//     pub fn query(&self, query: &str) -> Result<Table, ParseError> {
//         let mut parser = Parser::new(query);
//         let ast = match parser.external_parse_expr() {
//             Err(err) => return Err(err),
//             Ok(ast) => ast,
//         };

//         let mut table: Table = self.clone();

//         let mut ctx = ExprCtx::new();

//         for stmt in ast.iter_list() {
//             println!("Stmt: {:?}", stmt);
//             match stmt.get_type() {
//                 AstNodeType::SelectStmt(selects) => {
//                     let node_id = ctx.get_len_node_idx();
//                     let select_lens = selects
//                         .iter_list()
//                         .map(|select| get_expr_len(&mut ctx, node_id, select))
//                         .collect::<Vec<_>>();

//                     let mut len_expr: Option<LenExpr> = None;
//                     for this_len in &select_lens {
//                         len_expr = match len_expr {
//                             None => Some(*this_len),
//                             Some(len) => Some(match len {
//                                 LenExpr::Scalar => match this_len {
//                                     LenExpr::Scalar => LenExpr::Scalar,
//                                     LenExpr::NodeId(_) => *this_len,
//                                 },
//                                 LenExpr::NodeId(node_id) => match this_len {
//                                     LenExpr::Scalar => len,
//                                     LenExpr::NodeId(this_id) => {
//                                         assert_eq!(node_id, *this_id);
//                                         len
//                                     }
//                                 },
//                             }),
//                         }
//                     }
//                     let len_expr = len_expr.unwrap();

//                     let mut columns = selects
//                         .iter_list()
//                         .map(|select| table.eval_col_expr(select))
//                         .collect::<Vec<_>>();

//                     // get final length
//                     let len: usize = match len_expr {
//                         LenExpr::Scalar => 1,
//                         LenExpr::NodeId(_) => {
//                             let mut len: Option<usize> = None;

//                             for (len_expr, col) in std::iter::zip(&select_lens, &columns) {
//                                 match len_expr {
//                                     LenExpr::Scalar => {}
//                                     LenExpr::NodeId(_) => match len {
//                                         None => {
//                                             len = Some(col.len());
//                                         }
//                                         Some(len) => {
//                                             assert_eq!(len, col.len());
//                                         }
//                                     },
//                                 }
//                             }
//                             len.unwrap()
//                         }
//                     };

//                     // repeat scalar column to match other column length
//                     for (len_expr, col) in std::iter::zip(&select_lens, columns.iter_mut()) {
//                         match len_expr {
//                             LenExpr::Scalar => *col = col.repeat_scalar_col(len),
//                             LenExpr::NodeId(_) => {}
//                         }
//                     }

//                     table =
//                         Table::from_columns(selects.iter_list().map(|select| TableColumnWrapper {
//                             name: table.get_expr_name(select),
//                             column: table.eval_col_expr(select),
//                         }));
//                 }
//                 AstNodeType::WhereStmt(stmt) => {
//                     let col_name = match stmt.get_type() {
//                         AstNodeType::Identifier(col_name) => col_name.clone(),
//                         _ => todo!(),
//                     };

//                     table = table.where_col_is_true(&col_name)
//                 }
//                 AstNodeType::GroupBy { group_by, get_expr } => {
//                     let group_col_name_refs = group_by
//                         .iter_list()
//                         .map(|col| match col.get_type() {
//                             AstNodeType::Identifier(name) => name.as_str(),
//                             _ => todo!("Add support for {:?}", col.get_type()),
//                         })
//                         .collect::<Vec<_>>();

//                     let aggs = get_expr
//                         .iter_list()
//                         .map(|get_expr| match get_expr.get_type() {
//                             AstNodeType::FcnCall {
//                                 name: fcn_name,
//                                 args,
//                             } => match fcn_name.get_type() {
//                                 AstNodeType::Identifier(fcn_name) => match fcn_name.as_str() {
//                                     "sum" => {
//                                         let col_name = match args {
//                                             Some(args) => {
//                                                 match args.iter_list().nth(0).unwrap().get_type() {
//                                                     AstNodeType::Identifier(col_name) => {
//                                                         col_name.as_str()
//                                                     }
//                                                     _ => todo!(),
//                                                 }
//                                             }
//                                             _ => todo!(),
//                                         };

//                                         Aggregation {
//                                             in_col_name: col_name,
//                                             out_col_name: col_name,
//                                             agg_type: AggregationType::Sum,
//                                         }
//                                     }
//                                     "first" => {
//                                         let col_name = match args {
//                                             Some(args) => {
//                                                 match args.iter_list().nth(0).unwrap().get_type() {
//                                                     AstNodeType::Identifier(col_name) => {
//                                                         col_name.as_str()
//                                                     }
//                                                     _ => todo!(),
//                                                 }
//                                             }
//                                             _ => todo!(),
//                                         };

//                                         Aggregation {
//                                             in_col_name: col_name,
//                                             out_col_name: col_name,
//                                             agg_type: AggregationType::First,
//                                         }
//                                     }
//                                     _ => todo!(),
//                                 },
//                                 _ => todo!(),
//                             },
//                             _ => todo!(),
//                         })
//                         .collect::<Vec<_>>();

//                     table =
//                         table.group_and_aggregate(group_col_name_refs.as_slice(), aggs.as_slice())
//                 }
//                 _ => todo!(),
//             }
//         }

//         Ok(table)
//     }

//     fn eval_col_expr(&self, expr: &AstNode) -> Column {
//         match expr.get_type() {
//             AstNodeType::Identifier(col_name) => self.get_column(col_name.as_str()),
//             AstNodeType::Add(left, right) => self.eval_col_expr(left) + self.eval_col_expr(right),
//             AstNodeType::Float64(val) => Column::from_repeated_f64(*val, 1),
//             AstNodeType::Integer(val) => Column::from_repeated_f64(*val as f64, 1),
//             AstNodeType::Alias { expr, alias: _ } => self.eval_col_expr(expr),
//             _ => todo!(),
//         }
//     }

//     fn get_expr_name(&self, expr: &AstNode) -> String {
//         match expr.get_type() {
//             AstNodeType::Identifier(col_name) => col_name.clone(),
//             AstNodeType::Add(left, right) => format!(
//                 "({} + {})",
//                 self.get_expr_name(left),
//                 self.get_expr_name(right)
//             ),
//             AstNodeType::Integer(val) => format!("{}", val),
//             AstNodeType::Float64(val) => format!("{}", val),
//             AstNodeType::Alias { expr: _, alias } => match alias.get_type() {
//                 AstNodeType::Identifier(alias) => alias.clone(),
//                 _ => todo!(),
//             },
//             _ => todo!(),
//         }
//     }
// }
