use super::ast_node::{AstNode, AstNodeType};

pub struct Parser<'a> {
    text: &'a str,
    idx: usize,
}

impl<'a> Parser<'a> {
    pub fn new(text: &'a str) -> Self {
        Parser { text, idx: 0 }
    }

    fn peek(&self, n: usize) -> Option<u8> {
        self.text.as_bytes().get(self.idx + n).copied()
    }

    fn is_alpha(val: u8) -> bool {
        ('A' as u8) <= val && val <= ('Z' as u8) || ('a' as u8) <= val && val <= ('z' as u8)
    }

    fn is_alpha_underscore(val: u8) -> bool {
        Parser::is_alpha(val) || val == ('_' as u8)
    }

    fn is_numeric(val: u8) -> bool {
        ('0' as u8) <= val && val <= ('9' as u8)
    }

    fn is_alpha_underscore_numeric(val: u8) -> bool {
        Parser::is_alpha_underscore(val) || Parser::is_numeric(val)
    }

    fn is_whitespace(val: u8) -> bool {
        val == (' ' as u8) || val == ('\n' as u8) || val == ('\t' as u8) || val == ('\r' as u8)
    }

    fn parse_identifier(&mut self) -> Option<AstNode> {
        match self.peek(0) {
            None => {
                return None;
            }
            Some(c0) => {
                if Parser::is_alpha_underscore(c0) {
                    c0
                } else {
                    return None;
                }
            }
        };

        let mut idx: usize = 1;
        loop {
            match self.peek(idx) {
                None => {
                    break;
                }
                Some(cn) => {
                    if !Parser::is_alpha_underscore_numeric(cn) {
                        break;
                    }
                }
            };
            idx += 1;
        }

        let iden_str =
            std::str::from_utf8(&self.text.as_bytes()[self.idx..self.idx + idx]).unwrap();

        self.idx += idx;
        Some(AstNode::new(AstNodeType::Identifier(iden_str.to_string())))
    }

    fn parse_table_name(&mut self) -> Option<AstNode> {
        match self.peek(0) {
            None => {
                return None;
            }
            Some(c0) => {
                if Parser::is_alpha_underscore(c0) {
                    c0
                } else {
                    return None;
                }
            }
        };

        let mut idx: usize = 1;
        loop {
            match self.peek(idx) {
                None => {
                    break;
                }
                Some(cn) => {
                    if !(Parser::is_alpha_underscore_numeric(cn) || cn == ('.' as u8)) {
                        break;
                    }
                }
            };
            idx += 1;
        }

        let iden_str =
            std::str::from_utf8(&self.text.as_bytes()[self.idx..self.idx + idx]).unwrap();

        self.idx += idx;
        Some(AstNode::new(AstNodeType::Identifier(iden_str.to_string())))
    }

    fn parse_ws(&mut self) {
        loop {
            match self.peek(0) {
                None => {
                    break;
                }
                Some(ch) => {
                    if !Parser::is_whitespace(ch) {
                        break;
                    }
                }
            }
            self.idx += 1;
        }
    }

    fn parse_at_least_one_ws(&mut self) -> bool {
        match self.peek(0) {
            None => {
                return false;
            }
            Some(ch) => {
                if !Parser::is_whitespace(ch) {
                    return false;
                }
            }
        }
        self.parse_ws();
        true
    }

    fn parse_integer(&mut self) -> Option<u64> {
        let mut idx = 0 as usize;
        let mut val = 0 as u64;
        loop {
            match self.peek(idx) {
                None => {
                    break;
                }
                Some(ch) => {
                    if Parser::is_numeric(ch) {
                        val *= 10;
                        val += (ch - ('0' as u8)) as u64;
                    } else {
                        break;
                    }
                }
            }
            idx += 1;
        }

        if idx == 0 {
            return None;
        }
        self.idx += idx;

        Some(val)
    }

    fn parse_number(&mut self) -> Option<Result<AstNode, ParseError>> {
        let int_part = match self.parse_integer() {
            None => return None,
            Some(integer_part) => integer_part,
        };

        self.parse_ws();
        if self.parse_str_literal(".") {
            self.parse_ws();

            match self.parse_integer() {
                None => Some(Err(
                    self.get_err(ParseErrorType::MissingFractionPartInFloatLiteral)
                )),
                Some(fraction_part) => {
                    let serialized_number = format!("{}.{}", int_part, fraction_part);

                    let float = serialized_number.parse::<f64>().unwrap();

                    Some(Ok(AstNode::new(AstNodeType::Float64(float))))
                }
            }
        } else {
            Some(Ok(AstNode::new(AstNodeType::Integer(int_part))))
        }
    }

    fn get_err(&self, typ: ParseErrorType) -> ParseError {
        ParseError { idx: self.idx, typ }
    }

    // fn parse_list_literal(&mut self) -> Option<Result<AstNode, ParseError>> {
    //     if !self.parse_str_literal("[") {
    //         return None;
    //     }

    //     let mut parts: Option<AstNode> = None;

    //     loop {
    //         self.parse_ws();

    //         let is_spread = if self.parse_str_literal("*") {
    //             self.parse_ws();
    //             true
    //         } else {
    //             false
    //         };

    //         let mut elem = match self.parse_expr() {
    //             None => {
    //                 break;
    //             }
    //             Some(val) => match val {
    //                 Err(err) => {
    //                     return Some(Err(err));
    //                 }
    //                 Ok(val) => val,
    //             },
    //         };
    //         if is_spread {
    //             elem = AstNode::new(AstNodeType::Spread(elem));
    //         }
    //         match parts {
    //             None => {
    //                 parts = Some(elem);
    //             }
    //             Some(prev_val) => {
    //                 parts = Some(AstNode::new(AstNodeType::ListNode(prev_val, elem)));
    //             }
    //         }

    //         self.parse_ws();

    //         if !self.parse_str_literal(",") {
    //             break;
    //         }
    //     }

    //     if !self.parse_str_literal("]") {
    //         return Some(Err(self.get_err(ParseErrorType::NoListLiteralEndingBracket)));
    //     }

    //     Some(Ok(AstNode::new(AstNodeType::ListLiteral(parts))))
    // }

    // fn parse_map_literal(&mut self) -> Option<Result<AstNode, ParseError>> {
    //     if !self.parse_str_literal("{") {
    //         return None;
    //     }

    //     let mut parts: Option<AstNode> = None;

    //     loop {
    //         self.parse_ws();

    //         if self.parse_str_literal("*") {
    //             self.parse_ws();
    //             let expr = match self.parse_expr() {
    //                 None => {
    //                     return Some(Err(self.get_err(ParseErrorType::NoExpressionAfterMapSpread)))
    //                 }
    //                 Some(expr) => match expr {
    //                     Err(err) => {
    //                         return Some(Err(err));
    //                     }
    //                     Ok(expr) => expr,
    //                 },
    //             };
    //             let new_part = AstNode::new(AstNodeType::Spread(expr));
    //             match parts {
    //                 None => {
    //                     parts = Some(new_part);
    //                 }
    //                 Some(prev_val) => {
    //                     parts = Some(AstNode::new(AstNodeType::ListNode(prev_val, new_part)));
    //                 }
    //             }
    //         } else if self.parse_str_literal("-") {
    //             self.parse_ws();
    //             let expr = match self.parse_expr() {
    //                 None => {
    //                     return Some(Err(self.get_err(ParseErrorType::NoExpressionAfterMapDelete)))
    //                 }
    //                 Some(expr) => match expr {
    //                     Err(err) => return Some(Err(err)),
    //                     Ok(expr) => expr,
    //                 },
    //             };
    //             let new_part = AstNode::new(AstNodeType::MapDelete(expr));
    //             match parts {
    //                 None => {
    //                     parts = Some(new_part);
    //                 }
    //                 Some(prev_val) => {
    //                     parts = Some(AstNode::new(AstNodeType::ListNode(prev_val, new_part)));
    //                 }
    //             }
    //         } else {
    //             let key = if self.parse_str_literal("[") {
    //                 self.parse_ws();
    //                 let key = match self.parse_expr() {
    //                     None => return Some(Err(self.get_err(ParseErrorType::NoExprInMapKey))),
    //                     Some(key) => match key {
    //                         Err(err) => return Some(Err(err)),
    //                         Ok(key) => key,
    //                     },
    //                 };
    //                 self.parse_ws();
    //                 if !self.parse_str_literal("]") {
    //                     return Some(Err(self.get_err(ParseErrorType::NoClosingBracketForMapKey)));
    //                 }
    //                 key
    //             } else if let Some(f_string) = self.parse_format_string() {
    //                 match f_string {
    //                     Err(err) => return Some(Err(err)),
    //                     Ok(f_string) => f_string,
    //                 }
    //             } else {
    //                 match self.parse_identifier(false) {
    //                     None => {
    //                         break;
    //                     }
    //                     Some(val) => val,
    //                 }
    //             };
    //             self.parse_ws();

    //             if !self.parse_str_literal(":") {
    //                 return Some(Err(self.get_err(ParseErrorType::NoColonInMapLiteral)));
    //             }
    //             self.parse_ws();

    //             let val = match self.parse_expr() {
    //                 None => {
    //                     return Some(Err(self.get_err(ParseErrorType::NoMapLiteralValue)));
    //                 }
    //                 Some(val) => match val {
    //                     Err(err) => {
    //                         return Some(Err(err));
    //                     }
    //                     Ok(val) => val,
    //                 },
    //             };

    //             let kv_pair = AstNode::new(AstNodeType::MapKeyValPair { key, val });
    //             match parts {
    //                 None => {
    //                     parts = Some(kv_pair);
    //                 }
    //                 Some(prev_val) => {
    //                     parts = Some(AstNode::new(AstNodeType::ListNode(prev_val, kv_pair)));
    //                 }
    //             }
    //         }

    //         self.parse_ws();

    //         if !self.parse_str_literal(",") {
    //             break;
    //         }
    //     }

    //     if !self.parse_str_literal("}") {
    //         return Some(Err(self.get_err(ParseErrorType::NoMapLiteralEndingBrace)));
    //     }

    //     Some(Ok(AstNode::new(AstNodeType::MapLiteral(parts))))
    // }

    // fn get_substring(&mut self, start_offset: usize, end_offset: usize) -> AstNode {
    //     AstNode::new(AstNodeType::SubString(
    //         std::str::from_utf8(
    //             &self.text.as_bytes()[self.idx + start_offset..self.idx + end_offset],
    //         )
    //         .unwrap()
    //         .to_string(),
    //     ))
    // }

    fn parse_str_literal(&mut self, text: &str) -> bool {
        let text_bytes = text.as_bytes();
        let mut idx = 0 as usize;
        while idx < text_bytes.len() {
            match self.peek(idx) {
                None => {
                    return false;
                }
                Some(ch) => {
                    if ch != text_bytes[idx] {
                        return false;
                    }
                }
            };
            idx += 1;
        }
        self.idx += idx;

        return true;
    }

    pub fn external_parse_expr(&mut self) -> Result<AstNode, ParseError> {
        self.parse_ws();
        let expr = match self.parse_query() {
            None => return Err(self.get_err(ParseErrorType::EmptyParse)),
            Some(expr) => match expr {
                Err(err) => {
                    return Err(err);
                }
                Ok(expr) => expr,
            },
        };
        self.parse_ws();
        if self.idx < self.text.as_bytes().len() {
            return Err(self.get_err(ParseErrorType::IncompleteParse));
        }

        Ok(expr)
    }

    fn parse_query(&mut self) -> Option<Result<AstNode, ParseError>> {
        let mut expr: Option<AstNode> = None;

        'stmt_loop: loop {
            let stmt = match self.parse_statement() {
                None => {
                    break 'stmt_loop;
                }
                Some(stmt) => match stmt {
                    Err(err) => return Some(Err(err)),
                    Ok(stmt) => stmt,
                },
            };

            expr = match expr {
                None => Some(stmt),
                Some(expr) => Some(AstNode::new(AstNodeType::ListNode(expr, stmt))),
            };
        }

        match expr {
            None => None,
            Some(expr) => Some(Ok(AstNode::new(AstNodeType::StmtList(expr)))),
        }
    }

    fn parse_alias(&mut self) -> Option<Result<AstNode, ParseError>> {
        let mut result = match self.parse_expr() {
            None => {
                return Some(Err(
                    self.get_err(ParseErrorType::MissingExpressionAfterComma)
                ))
            }
            Some(expr) => match expr {
                Err(err) => return Some(Err(err)),
                Ok(expr) => expr,
            },
        };

        self.parse_ws();

        if self.parse_str_literal("as") {
            if !self.parse_at_least_one_ws() {
                return Some(Err(self.get_err(ParseErrorType::MissingSpace)));
            }

            let alias = match self.parse_identifier() {
                None => return Some(Err(self.get_err(ParseErrorType::MissingAlias))),
                Some(alias) => alias,
            };

            result = AstNode::new(AstNodeType::Alias {
                expr: result,
                alias,
            })
        }

        Some(Ok(result))
    }

    fn parse_comma_separated_expr(&mut self) -> Option<Result<AstNode, ParseError>> {
        let mut exprs = match self.parse_alias() {
            None => {
                return Some(Err(
                    self.get_err(ParseErrorType::MissingExpressionAfterComma)
                ))
            }
            Some(expr) => match expr {
                Err(err) => return Some(Err(err)),
                Ok(expr) => expr,
            },
        };
        self.parse_ws();

        loop {
            if !self.parse_str_literal(",") {
                break;
            }
            self.parse_ws();

            exprs = match self.parse_alias() {
                None => {
                    return Some(Err(
                        self.get_err(ParseErrorType::MissingExpressionAfterComma)
                    ))
                }
                Some(expr) => match expr {
                    Err(err) => return Some(Err(err)),
                    Ok(expr) => AstNode::new(AstNodeType::ListNode(exprs, expr)),
                },
            };

            self.parse_ws();
        }

        Some(Ok(exprs))
    }

    fn parse_statement(&mut self) -> Option<Result<AstNode, ParseError>> {
        self.parse_ws();

        if self.parse_str_literal("get") {
            if !self.parse_at_least_one_ws() {
                return Some(Err(self.get_err(ParseErrorType::MissingSpaceAfterSelect)));
            }

            let select_expr = match self.parse_comma_separated_expr() {
                None => return Some(Err(self.get_err(ParseErrorType::MissingSelectField))),
                Some(expr) => match expr {
                    Err(err) => return Some(Err(err)),
                    Ok(expr) => expr,
                },
            };

            Some(Ok(AstNode::new(AstNodeType::SelectStmt(select_expr))))
        } else if self.parse_str_literal("from") {
            if !self.parse_at_least_one_ws() {
                return Some(Err(self.get_err(ParseErrorType::MissingSpaceAfterFrom)));
            }

            match self.parse_table_name() {
                None => Some(Err(self.get_err(ParseErrorType::NoTableNameInFromStatement))),
                Some(identifier) => Some(Ok(AstNode::new(AstNodeType::FromStmt(identifier)))),
            }
        } else if self.parse_str_literal("where") {
            if !self.parse_at_least_one_ws() {
                return Some(Err(self.get_err(ParseErrorType::MissingSpaceAfterWhere)));
            }

            match self.parse_expr() {
                None => Some(Err(
                    self.get_err(ParseErrorType::NoConditionAfterWhereStatement)
                )),
                Some(expr) => match expr {
                    Err(err) => Some(Err(err)),
                    Ok(expr) => Some(Ok(AstNode::new(AstNodeType::WhereStmt(expr)))),
                },
            }
        } else if self.parse_str_literal("group") {
            if !self.parse_at_least_one_ws() {
                return Some(Err(self.get_err(ParseErrorType::MissingSpaceAfterGroup)));
            }

            if !self.parse_str_literal("by") {
                return Some(Err(self.get_err(ParseErrorType::MissingBy)));
            }
            self.parse_ws();

            let group_by = match self.parse_comma_separated_expr() {
                None => return Some(Err(self.get_err(ParseErrorType::GroupByMissingGroupByField))),
                Some(expr) => match expr {
                    Err(err) => return Some(Err(err)),
                    Ok(expr) => expr,
                },
            };

            self.parse_ws();

            if !self.parse_str_literal("get") {
                return Some(Err(self.get_err(ParseErrorType::MissingGroupByGet)));
            }

            if !self.parse_at_least_one_ws() {
                return Some(Err(
                    self.get_err(ParseErrorType::MissingSpaceAfterGroupByGet)
                ));
            }

            let get_expr = match self.parse_comma_separated_expr() {
                None => return Some(Err(self.get_err(ParseErrorType::GroupByMissingGet))),
                Some(expr) => match expr {
                    Err(err) => return Some(Err(err)),
                    Ok(expr) => expr,
                },
            };

            Some(Ok(AstNode::new(AstNodeType::GroupBy {
                group_by,
                get_expr,
            })))
        } else {
            None
        }
    }

    fn parse_expr(&mut self) -> Option<Result<AstNode, ParseError>> {
        #[derive(Clone)]
        enum Op {
            Unary(UnaryOp),
            Binary(BinaryOp),
        }

        #[derive(Clone)]
        enum UnaryOp {
            Not,
            Negative,
        }

        #[derive(Clone)]
        enum BinaryOp {
            Or,
            And,
            Equals,
            NotEquals,
            LessThan,
            LessThanOrEqual,
            GreaterThan,
            GreaterThanOrEqual,
            Add,
            Subtract,
            Multiply,
            Divide,
        }

        #[derive(PartialEq, PartialOrd, Clone, Copy)]
        enum OpOrder {
            End,
            Or,
            And,
            Not,
            Equality,
            Add,
            Multiply,
            Negative,
        }

        enum Node {
            Expr(AstNode),
            Op(Op, OpOrder),
        }

        let mut stack = Vec::<Node>::new();

        fn reduce_for_op_order(stack: &mut Vec<Node>, order: OpOrder) {
            while stack.len() > 1 {
                if let (Node::Op(temp_op, temp_order), Node::Expr(right)) =
                    (&stack[stack.len() - 2], &stack[stack.len() - 1])
                {
                    let temp_order = *temp_order;
                    let right = right.clone();
                    if temp_order >= order {
                        match temp_op {
                            Op::Unary(temp_op) => {
                                let new_expr = match temp_op {
                                    UnaryOp::Not => AstNode::new(AstNodeType::Not(right)),
                                    UnaryOp::Negative => AstNode::new(AstNodeType::Negative(right)),
                                };
                                stack.pop();
                                stack.pop();
                                stack.push(Node::Expr(new_expr));
                            }
                            Op::Binary(temp_op) => {
                                if let Node::Expr(left) = &stack[stack.len() - 3] {
                                    let left = left.clone();
                                    let new_expr = match temp_op {
                                        BinaryOp::Or => AstNode::new(AstNodeType::Or(left, right)),
                                        BinaryOp::And => {
                                            AstNode::new(AstNodeType::And(left, right))
                                        }
                                        BinaryOp::Equals => {
                                            AstNode::new(AstNodeType::Equals(left, right))
                                        }
                                        BinaryOp::NotEquals => {
                                            AstNode::new(AstNodeType::NotEquals(left, right))
                                        }
                                        BinaryOp::LessThan => {
                                            AstNode::new(AstNodeType::LessThan(left, right))
                                        }
                                        BinaryOp::LessThanOrEqual => {
                                            AstNode::new(AstNodeType::LessThanOrEqual(left, right))
                                        }
                                        BinaryOp::GreaterThan => {
                                            AstNode::new(AstNodeType::GreaterThan(left, right))
                                        }
                                        BinaryOp::GreaterThanOrEqual => AstNode::new(
                                            AstNodeType::GreaterThanOrEqual(left, right),
                                        ),
                                        BinaryOp::Add => {
                                            AstNode::new(AstNodeType::Add(left, right))
                                        }
                                        BinaryOp::Subtract => {
                                            AstNode::new(AstNodeType::Subtract(left, right))
                                        }
                                        BinaryOp::Multiply => {
                                            AstNode::new(AstNodeType::Multiply(left, right))
                                        }
                                        BinaryOp::Divide => {
                                            AstNode::new(AstNodeType::Divide(left, right))
                                        }
                                    };
                                    stack.pop();
                                    stack.pop();
                                    stack.pop();
                                    stack.push(Node::Expr(new_expr));
                                } else {
                                    panic!();
                                }
                            }
                        };
                    } else {
                        break;
                    }
                } else {
                    panic!()
                }
            }
        }

        fn parse_base_expr(
            stack: &mut Vec<Node>,
            parser: &mut Parser,
        ) -> Option<Result<(), ParseError>> {
            let mut has_unary_op = false;

            loop {
                if parser.parse_str_literal("not") {
                    parser.parse_ws();
                    stack.push(Node::Op(Op::Unary(UnaryOp::Not), OpOrder::Not));
                    has_unary_op = true;
                } else if parser.parse_str_literal("-") {
                    parser.parse_ws();
                    stack.push(Node::Op(Op::Unary(UnaryOp::Negative), OpOrder::Negative));
                    has_unary_op = true;
                } else {
                    break;
                }
            }

            let expr = match parser.parse_base_expr() {
                None => {
                    if has_unary_op {
                        return Some(Err(parser.get_err(ParseErrorType::NoExprAfterUnaryOperator)));
                    }
                    return None;
                }
                Some(expr) => match expr {
                    Err(err) => {
                        return Some(Err(err));
                    }
                    Ok(expr) => expr,
                },
            };
            stack.push(Node::Expr(expr));
            Some(Ok(()))
        }

        match parse_base_expr(&mut stack, self) {
            None => {
                return None;
            }
            Some(expr) => match expr {
                Err(err) => {
                    return Some(Err(err));
                }
                Ok(_) => {}
            },
        };

        loop {
            self.parse_ws();
            if let Some((next_op, next_order)) = if self.parse_str_literal("or") {
                Some((BinaryOp::Or, OpOrder::Or))
            } else if self.parse_str_literal("and") {
                Some((BinaryOp::And, OpOrder::And))
            } else if self.parse_str_literal("==") {
                Some((BinaryOp::Equals, OpOrder::Equality))
            } else if self.parse_str_literal("!=") {
                Some((BinaryOp::NotEquals, OpOrder::Equality))
            } else if self.parse_str_literal("<=") {
                Some((BinaryOp::LessThanOrEqual, OpOrder::Equality))
            } else if self.parse_str_literal("<") {
                Some((BinaryOp::LessThan, OpOrder::Equality))
            } else if self.parse_str_literal(">=") {
                Some((BinaryOp::GreaterThanOrEqual, OpOrder::Equality))
            } else if self.parse_str_literal(">") {
                Some((BinaryOp::GreaterThan, OpOrder::Equality))
            } else if self.parse_str_literal("+") {
                Some((BinaryOp::Add, OpOrder::Add))
            } else if self.parse_str_literal("-") {
                Some((BinaryOp::Subtract, OpOrder::Add))
            } else if self.parse_str_literal("*") {
                Some((BinaryOp::Multiply, OpOrder::Multiply))
            } else if self.parse_str_literal("/") {
                Some((BinaryOp::Divide, OpOrder::Multiply))
            } else {
                break;
            } {
                self.parse_ws();

                reduce_for_op_order(&mut stack, next_order);
                stack.push(Node::Op(Op::Binary(next_op), next_order));

                match parse_base_expr(&mut stack, self) {
                    None => {
                        return Some(Err(self.get_err(ParseErrorType::NoExprAfterOperator)));
                    }
                    Some(expr) => match expr {
                        Err(err) => {
                            return Some(Err(err));
                        }
                        Ok(_) => {}
                    },
                };
            }
        }

        reduce_for_op_order(&mut stack, OpOrder::End);

        assert!(stack.len() == 1);

        if let Node::Expr(expr) = stack.pop().unwrap() {
            Some(Ok(expr))
        } else {
            panic!();
        }
    }

    fn parse_base_expr(&mut self) -> Option<Result<AstNode, ParseError>> {
        if self.parse_str_literal("(") {
            let expr = match self.parse_expr() {
                None => return Some(Err(self.get_err(ParseErrorType::NoParenContents))),
                Some(expr) => match expr {
                    Err(err) => {
                        return Some(Err(err));
                    }
                    Ok(expr) => expr,
                },
            };
            self.parse_ws();
            if !self.parse_str_literal(")") {
                return Some(Err(self.get_err(ParseErrorType::NoClosingParen)));
            }
            return Some(Ok(expr));
        }

        match self.parse_number() {
            None => {}
            Some(expr) => match expr {
                Err(err) => return Some(Err(err)),
                Ok(expr) => return Some(Ok(expr)),
            },
        }
        if self.parse_str_literal("null") {
            return Some(Ok(AstNode::new(AstNodeType::Null)));
        }
        if self.parse_str_literal("true") {
            return Some(Ok(AstNode::new(AstNodeType::Bool(true))));
        }
        if self.parse_str_literal("false") {
            return Some(Ok(AstNode::new(AstNodeType::Bool(false))));
        }
        if let Some(expr) = self.parse_identifier() {
            self.parse_ws();
            if self.parse_str_literal("(") {
                // This is a function call
                self.parse_ws();
                let mut args_node: Option<AstNode> = None;
                loop {
                    match self.parse_expr() {
                        None => {
                            break;
                        }
                        Some(expr) => match expr {
                            Err(err) => {
                                return Some(Err(err));
                            }
                            Ok(expr) => {
                                args_node = Some(match args_node {
                                    None => expr,
                                    Some(args_node) => {
                                        AstNode::new(AstNodeType::ListNode(args_node, expr))
                                    }
                                })
                            }
                        },
                    };

                    self.parse_ws();
                    if !self.parse_str_literal(",") {
                        break;
                    }
                    self.parse_ws();
                }

                if self.parse_str_literal(")") {
                    return Some(Ok(AstNode::new(AstNodeType::FcnCall {
                        name: expr,
                        args: args_node,
                    })));
                } else {
                    return Some(Err(self.get_err(ParseErrorType::NoClosingParenFcnCall)));
                }
            }

            return Some(Ok(expr));
        }

        return None;
    }

    // fn parse_let_expr(&mut self) -> Option<Result<AstNode, ParseError>> {
    //     if !self.parse_str_literal("let") {
    //         return None;
    //     }
    //     self.parse_ws();

    //     let identifier = match self.parse_identifier(true) {
    //         None => return Some(Err(self.get_err(ParseErrorType::NoIdentifierInLetStmt))),
    //         Some(res) => res,
    //     };
    //     self.parse_ws();

    //     if !self.parse_str_literal("=") {
    //         return Some(Err(self.get_err(ParseErrorType::NoEqualsInLetStmt)));
    //     }
    //     self.parse_ws();

    //     let expr = match self.inner_parse_expr(false) {
    //         None => return Some(Err(self.get_err(ParseErrorType::NoExprInLetStmt))),
    //         Some(expr) => match expr {
    //             Err(err) => return Some(Err(err)),
    //             Ok(expr) => expr,
    //         },
    //     };

    //     Some(Ok(AstNode::new(AstNodeType::LetStmt { identifier, expr })))
    // }
}

#[derive(Debug)]
pub struct ParseError {
    idx: usize,
    typ: ParseErrorType,
}

impl ParseError {
    pub fn to_string(&self) -> String {
        format!("Parser error '{:?}' at index {}", self.typ, self.idx)
    }
}

#[derive(Debug)]
enum ParseErrorType {
    EmptyParse,
    IncompleteParse,
    NoTableNameInFromStatement,
    NoConditionAfterWhereStatement,
    MissingBy,
    MissingExpressionAfterComma,
    GroupByMissingGet,
    GroupByMissingGroupByField,
    MissingGroupByGet,
    MissingSpaceAfterGroupByGet,
    MissingSpaceAfterSelect,
    MissingAlias,
    MissingSpace,
    MissingSelectField,
    MissingSpaceAfterFrom,
    MissingSpaceAfterWhere,
    MissingSpaceAfterGroup,
    NoExprAfterOperator,
    NoExprAfterUnaryOperator,
    NoParenContents,
    NoClosingParen,
    NoClosingParenFcnCall,
    MissingFractionPartInFloatLiteral,
}
