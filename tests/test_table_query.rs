#[cfg(test)]
mod tests {
    use table_lib::*;

    #[test]
    fn basic_select() {
        let table = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "text_col0",
                        "type": "text",
                        "values": ["val0", "val1", null]
                    },
                    {
                        "name": "text_col1",
                        "type": "text",
                        "values": ["val a", "val b", null]
                    },
                    {
                        "name": "float_col",
                        "type": "float64",
                        "values": ["1.25", "invalid", null]
                    }
                ]
            }
            "#,
        );

        let result = table.query("select text_col1 , text_col0").unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {"name":"text_col1","type":"text","values":["val a","val b",null]},
                    {"name":"text_col0","type":"text","values":["val0","val1",null]}
                ]}"#
            )
        );
    }

    #[test]
    fn basic_where() {
        let table = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "id",
                        "type": "text",
                        "values": ["id0", "id1", "id2", "id3"]
                    },
                    {
                        "name": "condition",
                        "type": "bool",
                        "values": ["false", "true", null, "true"]
                    }
                ]
            }
            "#,
        );

        let result = table.query("where condition select id").unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "id",
                        "type": "text",
                        "values": ["id1", "id3"]
                    }
                ]}"#
            )
        );
    }

    #[test]
    fn basic_group_by_and_get() {
        let table = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "id0",
                        "type": "text",
                        "values": ["a", "a", "a", "b", "b", "c"]
                    },
                    {
                        "name": "id1",
                        "type": "text",
                        "values": ["x", "y", "x", "x", "x", "z"]
                    },
                    {
                        "name": "value",
                        "type": "float64",
                        "values": ["1", "2", "4", "5", "6", "7"]
                    }
                ]
            }
            "#,
        );

        let result = table.query("group by id0, id1 get sum(value)").unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "id0",
                        "type": "text",
                        "values": ["a", "a", "b", "c"]
                    },
                    {
                        "name": "id1",
                        "type": "text",
                        "values": ["x", "y", "x", "z"]
                    },
                    {
                        "name": "value",
                        "type": "float64",
                        "values": ["5", "2", "11", "7"]
                    }
                ]}"#
            )
        );
    }

    #[test]
    fn basic_select_expr() {
        let table = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "val0",
                        "type": "float64",
                        "values": ["0", "1", "2"]
                    },
                    {
                        "name": "val1",
                        "type": "float64",
                        "values": ["3", "4", "5"]
                    }
                ]
            }
            "#,
        );

        let result = table.query("select val0 + val1 + 1").unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "((val0 + val1) + 1)",
                        "type": "float64",
                        "values": ["4", "6", "8"]
                    }
                ]}"#
            )
        );
    }
}
