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

        let serialized = table.to_json_str();
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

    #[test]
    fn basic_group_and_aggregate() {
        let t0 = Table::from_json_str(
            r#"{"columns": [
                {"name": "g0", "type": "text", "values": [
                    "a", "a", "b", "b", "c", null, null
                ]},
                {"name": "f0", "type": "float64", "values": [
                    "1", "7", "3", null, null, "5", "6"
                ]}
            ]}"#,
        );

        let t0_agg = t0.group_and_aggregate(
            &["g0"],
            &[Aggregation {
                in_col_name: "f0",
                out_col_name: "f0_sum",
                agg_type: AggregationType::Sum,
            }],
        );

        let t1 = Table::from_json_str(
            r#"{"columns": [
                {"name": "g0", "type": "text", "values": [
                    "a", "b", "c", null
                ]},
                {"name": "f0_sum", "type": "float64", "values": [
                    "8", "3", null, "11"
                ]}
            ]}"#,
        );

        assert_eq!(t0_agg, t1);
        assert_eq!(t1, t0_agg);
    }

    #[test]
    fn group_on_no_group_fields() {
        let t0 = Table::from_json_str(
            r#"{"columns": [
                {"name": "f0", "type": "float64", "values": [
                    "1", "2", "3"
                ]}
            ]}"#,
        );

        let t0_agg = t0.group_and_aggregate(
            &[],
            &[Aggregation {
                in_col_name: "f0",
                out_col_name: "f0_sum",
                agg_type: AggregationType::Sum,
            }],
        );

        let t1 = Table::from_json_str(
            r#"{"columns": [
                {"name": "f0_sum", "type": "float64", "values": [
                    "6"
                ]}
            ]}"#,
        );

        assert_eq!(t0_agg, t1);
    }

    #[test]
    fn group_on_no_rows() {
        let t0 = Table::from_json_str(
            r#"{"columns": [
                {"name": "g0", "type": "text", "values": []},
                {"name": "f0", "type": "float64", "values": []}
            ]}"#,
        );

        let t0_agg = t0.group_and_aggregate(
            &["g0"],
            &[Aggregation {
                in_col_name: "f0",
                out_col_name: "f0_sum",
                agg_type: AggregationType::Sum,
            }],
        );

        let t1 = Table::from_json_str(
            r#"{"columns": [
                {"name": "g0", "type": "text", "values": []},
                {"name": "f0_sum", "type": "float64", "values": []}
            ]}"#,
        );

        assert_eq!(t0_agg, t1);
        assert_eq!(t1, t0_agg);
    }
}
