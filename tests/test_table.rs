#[cfg(test)]
mod tests {
    use table_lib::*;

    #[test]
    fn deserialize_and_serialize() {
        let table = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "text_col",
                        "type": "text",
                        "values": ["val0", "val1", null]
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

        let serialized = table.to_json();
        assert_eq!(
            serialized,
            r#"{"columns":[{"name":"text_col","type":"text","values":["val0","val1",null]},{"name":"float_col","type":"float64","values":["1.25",null,null]}]}"#
        );
    }

    #[test]
    fn equal() {
        let t0 = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "text_col",
                        "type": "text",
                        "values": ["val0", "val1", null]
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

        let t1 = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "text_col",
                        "type": "text",
                        "values": ["val0", "val1", null]
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

        assert_eq!(t0, t1);
        assert_eq!(t1, t0);
    }

    #[test]
    fn not_equal_text_col_value() {
        let t0 = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "text_col",
                        "type": "text",
                        "values": ["val0", "val1", null]
                    }
                ]
            }
            "#,
        );

        let t1 = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "text_col",
                        "type": "text",
                        "values": ["val0", "val100", null]
                    }
                ]
            }
            "#,
        );

        assert_ne!(t0, t1);
        assert_ne!(t1, t0);
    }

    #[test]
    fn not_equal_float_col_value() {
        let t0 = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "col",
                        "type": "float64",
                        "values": ["1"]
                    }
                ]
            }
            "#,
        );

        let t1 = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "col",
                        "type": "float64",
                        "values": ["2"]
                    }
                ]
            }
            "#,
        );

        assert_ne!(t0, t1);
        assert_ne!(t1, t0);
    }

    #[test]
    fn not_equal_missing_col() {
        let t0 = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "text_col",
                        "type": "text",
                        "values": ["val0", "val1", null]
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

        let t1 = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "text_col",
                        "type": "text",
                        "values": ["val0", "val1", null]
                    }
                ]
            }
            "#,
        );

        assert_ne!(t0, t1);
        assert_ne!(t1, t0);
    }

    #[test]
    fn equal_empty_table() {
        let t0 = Table::from_json_str(
            r#"
            {
                "columns": []
            }
            "#,
        );

        let t1 = Table::from_json_str(
            r#"
            {
                "columns": []
            }
            "#,
        );

        assert_eq!(t0, t1);
        assert_eq!(t1, t0);
    }
}
