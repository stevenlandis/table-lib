#[cfg(test)]
mod tests {
    use table_lib::*;

    #[test]
    fn query_basic() {
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
}
