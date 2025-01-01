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

    #[test]
    fn group_unordered_rows() {
        let t0 = Table::from_json_str(
            r#"{"columns": [
                {"name": "g0", "type": "text", "values": [
                    "A", "B", "C", "B", "A"
                ]},
                {"name": "f0", "type": "float64", "values": [
                    "1", "2", "4", "8", "16"
                ]}
            ]}"#,
        );

        let t0_agg = t0.group_and_aggregate(
            &["g0"],
            &[
                Aggregation {
                    in_col_name: "f0",
                    out_col_name: "f0_sum",
                    agg_type: AggregationType::Sum,
                },
                Aggregation {
                    in_col_name: "f0",
                    out_col_name: "f0_first",
                    agg_type: AggregationType::First,
                },
            ],
        );

        let t1 = Table::from_json_str(
            r#"{"columns": [
                {"name": "g0", "type": "text", "values": [
                    "A", "B", "C"
                ]},
                {"name": "f0_sum", "type": "float64", "values": [
                    "17", "10", "4"
                ]},
                {"name": "f0_first", "type": "float64", "values": [
                    "1", "2", "4"
                ]}
            ]}"#,
        );

        assert_eq!(t0_agg, t1);
        assert_eq!(t1, t0_agg);
    }

    #[test]
    fn select_and_rename() {
        let t0 = Table::from_json_str(
            r#"{"columns": [
                {"name": "f0", "type": "text", "values": ["v0"]},
                {"name": "f1", "type": "text", "values": ["v1"]},
                {"name": "f2", "type": "text", "values": ["v2"]}
            ]}"#,
        );

        let t1 = t0.select_and_rename(&[
            RenameCol {
                old_name: "f1",
                new_name: "F1",
            },
            RenameCol {
                old_name: "f0",
                new_name: "F0",
            },
        ]);

        let tr = Table::from_json_str(
            r#"{"columns": [
                {"name": "F1", "type": "text", "values": ["v1"]},
                {"name": "F0", "type": "text", "values": ["v0"]}
            ]}"#,
        );

        assert_eq!(t1, tr);
    }

    #[test]
    fn basic_augment() {
        let t0 = Table::from_json_str(
            r#"{"columns": [
                {"name": "f0", "type": "text", "values": ["f0_a", "f0_b"]},
                {"name": "left_join_col", "type": "text", "values": ["v1", "v2"]}
            ]}"#,
        );

        let t1 = Table::from_json_str(
            r#"{"columns": [
                {"name": "right_join_col", "type": "text", "values": ["v2", "v1"]},
                {"name": "f1", "type": "text", "values": ["f1_b", "f1_a"]}
            ]}"#,
        );

        let t2 = t0.augment(
            &t1,
            &[("left_join_col", "right_join_col")],
            &[RenameCol {
                old_name: "f1",
                new_name: "F1",
            }],
        );

        let tr = Table::from_json_str(
            r#"{"columns": [
                {"name": "f0", "type": "text", "values": ["f0_a", "f0_b"]},
                {"name": "left_join_col", "type": "text", "values": ["v1", "v2"]},
                {"name": "F1", "type": "text", "values": ["f1_a", "f1_b"]}
            ]}"#,
        );

        assert_eq!(t2, tr);
    }

    #[test]
    fn augment_with_null() {
        let t0 = Table::from_json_str(
            r#"{"columns": [
                {"name": "f0", "type": "text", "values": ["a", "b", "c"]},
                {"name": "left_val", "type": "text", "values": ["1l", "2l", "3l"]}
            ]}"#,
        );

        let t1 = Table::from_json_str(
            r#"{"columns": [
                {"name": "f0", "type": "text", "values": ["a", "c"]},
                {"name": "right_val", "type": "text", "values": ["1r", "3r"]}
            ]}"#,
        );

        let t2 = t0.augment(
            &t1,
            &[("f0", "f0")],
            &[RenameCol {
                old_name: "right_val",
                new_name: "right_val",
            }],
        );

        let tr = Table::from_json_str(
            r#"{"columns": [
                {"name": "f0", "type": "text", "values": ["a", "b", "c"]},
                {"name": "left_val", "type": "text", "values": ["1l", "2l", "3l"]},
                {"name": "right_val", "type": "text", "values": ["1r", null, "3r"]}
            ]}"#,
        );

        assert_eq!(t2, tr);
    }

    #[test]
    fn augment_with_multiple_join_fields() {
        let t0 = Table::from_json_str(
            r#"{"columns": [
                {"name": "f0", "type": "text", "values": ["f0_a", "f0_a", "f0_a"]},
                {"name": "f1", "type": "text", "values": ["f1_a", "f1_b", "f1_a"]},
                {"name": "left_val", "type": "text", "values": ["1l", "2l", "3l"]}
            ]}"#,
        );

        let t1 = Table::from_json_str(
            r#"{"columns": [
                {"name": "F0", "type": "text", "values": ["f0_a", "f0_a", "f0_a", "f0_a"]},
                {"name": "F1", "type": "text", "values": ["f1_a", "f1_a", "f1_b", "f1_b"]},
                {"name": "right_val", "type": "text", "values": ["1r", "2r", "3r", "4r"]}
            ]}"#,
        );

        let t2 = t0.augment(
            &t1,
            &[("f0", "F0"), ("f1", "F1")],
            &[RenameCol {
                old_name: "right_val",
                new_name: "right_val",
            }],
        );

        let tr = Table::from_json_str(
            r#"{"columns": [
                {"name": "f0", "type": "text", "values": ["f0_a", "f0_a", "f0_a", "f0_a", "f0_a", "f0_a"]},
                {"name": "f1", "type": "text", "values": ["f1_a", "f1_a", "f1_b", "f1_b", "f1_a", "f1_a"]},
                {"name": "left_val", "type": "text", "values": ["1l", "1l", "2l", "2l", "3l", "3l"]},
                {"name": "right_val", "type": "text", "values": ["1r", "2r", "3r", "4r", "1r", "2r"]}
            ]}"#,
        );

        assert_eq!(t2, tr);
    }

    #[test]
    fn augment_on_null() {
        let t0 = Table::from_json_str(
            r#"{"columns": [
                {"name": "f0", "type": "text", "values": ["a", null, "c"]},
                {"name": "left_val", "type": "text", "values": ["1l", "2l", "3l"]}
            ]}"#,
        );

        let t1 = Table::from_json_str(
            r#"{"columns": [
                {"name": "F0", "type": "text", "values": [null, "a"]},
                {"name": "right_val", "type": "text", "values": ["2r", "1r"]}
            ]}"#,
        );

        let t2 = t0.augment(
            &t1,
            &[("f0", "F0")],
            &[RenameCol {
                old_name: "right_val",
                new_name: "right_val",
            }],
        );

        let tr = Table::from_json_str(
            r#"{"columns": [
                {"name": "f0", "type": "text", "values": ["a", null, "c"]},
                {"name": "left_val", "type": "text", "values": ["1l", "2l", "3l"]},
                {"name": "right_val", "type": "text", "values": ["1r", "2r", null]}
            ]}"#,
        );

        assert_eq!(t2, tr);
    }

    #[test]
    fn augment_with_no_join_fields() {
        let t0 = Table::from_json_str(
            r#"{"columns": [
                {"name": "left_val", "type": "text", "values": ["a", "b"]}
            ]}"#,
        );

        let t1 = Table::from_json_str(
            r#"{"columns": [
                {"name": "right_val", "type": "text", "values": ["x", "y", "z"]}
            ]}"#,
        );

        let t2 = t0.augment(
            &t1,
            &[],
            &[RenameCol {
                old_name: "right_val",
                new_name: "right_val",
            }],
        );

        let tr = Table::from_json_str(
            r#"{"columns": [
                {"name":  "left_val", "type": "text", "values": ["a", "a", "a", "b", "b", "b"]},
                {"name": "right_val", "type": "text", "values": ["x", "y", "z", "x", "y", "z"]}
            ]}"#,
        );

        assert_eq!(t2, tr);
    }

    #[test]
    fn add_two_columns_and_add_new_col_to_table() {
        let t0 = Table::from_json_str(
            r#"{"columns": [
                {"name": "id", "type": "text", "values": ["a", "b"]},
                {"name": "v0", "type": "float64", "values": ["1", "2", "3", null]},
                {"name": "v1", "type": "float64", "values": ["3", "4", null, null]}
            ]}"#,
        );

        let c0 = t0.get_column("v0");
        let c1 = t0.get_column("v1");
        let c2 = &c0 + &c1;

        let t1 = t0.with_column("v2", c2.into());

        let tr = Table::from_json_str(
            r#"{"columns": [
                {"name": "id", "type": "text", "values": ["a", "b"]},
                {"name": "v0", "type": "float64", "values": ["1", "2", "3", null]},
                {"name": "v1", "type": "float64", "values": ["3", "4", null, null]},
                {"name": "v2", "type": "float64", "values": ["4", "6", null, null]}
            ]}"#,
        );

        assert_eq!(t1, tr);

        let t2 = t0.with_column("v2", &t0.get_column("v0") + &t0.get_column("v1").into());

        assert_eq!(t2, tr);
    }

    #[cfg(test)]
    mod csv_parse_tests {
        use table_lib::*;

        #[test]
        fn basic() {
            let csv_text = r#"
col0,col1,col2
stuff,and,things
1,2,3
"#
            .trim_start();

            let table = Table::from_csv_reader(std::io::Cursor::new(csv_text));
            assert_eq!(
                table,
                Table::from_json_str(
                    r#"{"columns": [
                        {"name": "col0", "type": "text", "values": ["stuff", "1"]},
                        {"name": "col1", "type": "text", "values": ["and", "2"]},
                        {"name": "col2", "type": "text", "values": ["things", "3"]}
                    ]}"#,
                )
            );
        }

        #[test]
        fn infer_col_types() {
            let csv_text = r#"
col0,col1,col2
stuff,1,false
and,2,true
"#
            .trim_start();

            let table = Table::from_csv_reader(std::io::Cursor::new(csv_text));
            assert_eq!(
                table,
                Table::from_json_str(
                    r#"{"columns": [
                        {"name": "col0", "type": "text", "values": ["stuff", "and"]},
                        {"name": "col1", "type": "float64", "values": ["1", "2"]},
                        {"name": "col2", "type": "bool", "values": ["false", "true"]}
                    ]}"#,
                )
            );
        }

        #[test]
        fn infer_col_types_nulls() {
            let csv_text = r#"
col0,col1,col2
,,
stuff,1,false
and,2,true
"#
            .trim_start();

            let table = Table::from_csv_reader(std::io::Cursor::new(csv_text));
            println!("{}", table.to_json_str());
            assert_eq!(
                table,
                Table::from_json_str(
                    r#"{"columns": [
                        {"name": "col0", "type": "text", "values": [null, "stuff", "and"]},
                        {"name": "col1", "type": "float64", "values": [null, "1", "2"]},
                        {"name": "col2", "type": "bool", "values": [null, "false", "true"]}
                    ]}"#,
                )
            );
        }
    }
}
