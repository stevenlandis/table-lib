#[cfg(test)]
mod tests {
    use table_lib::*;

    #[test]
    fn basic_from() {
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
                    }
                ]
            }
            "#,
        );

        let mut collection = TableCollection::new();
        collection.add_table("test_tbl", table);

        let result = collection.query("from test_tbl").unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{
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
                    }
                ]
                }"#
            )
        );
    }

    #[test]
    fn basic_get() {
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

        let mut collection = TableCollection::new();
        collection.add_table("tbl0", table);

        let result = collection
            .query("from tbl0 get text_col1 , text_col0")
            .unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns": [
                {
                    "name": "text_col1",
                    "type": "text",
                    "values": ["val a", "val b", null]
                },
                {
                    "name": "text_col0",
                    "type": "text",
                    "values": ["val0", "val1", null]
                }
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

        let mut collection = TableCollection::new();
        collection.add_table("tbl0", table);

        let result = collection
            .query("from tbl0 where condition get id")
            .unwrap();

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

        let mut collection = TableCollection::new();
        collection.add_table("tbl0", table);

        let result = collection
            .query("from tbl0 group by id0, id1 get id0, id1, sum(value)")
            .unwrap();

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
                        "name": "sum(value)",
                        "type": "float64",
                        "values": ["5", "2", "11", "7"]
                    }
                ]}"#
            )
        );
    }

    #[test]
    fn basic_get_expr() {
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

        let mut collection = TableCollection::new();
        collection.add_table("tbl0", table);
        let result = collection.query("from tbl0 get val0 + val1 + 1").unwrap();

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

    #[test]
    fn query_scalar() {
        let mut collection = TableCollection::new();
        let result = collection.query("get 1.5, 1.5 + 2.5").unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "1.5",
                        "type": "float64",
                        "values": ["1.5"]
                    },
                    {
                        "name": "(1.5 + 2.5)",
                        "type": "float64",
                        "values": ["4"]
                    }
                ]}"#
            )
        );
    }

    #[test]
    fn get_alias() {
        let table = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "val0",
                        "type": "float64",
                        "values": ["0", "1", "2"]
                    }
                ]
            }
            "#,
        );

        let mut collection = TableCollection::new();
        collection.add_table("tbl0", table);
        let result = collection
            .query("from tbl0 get val0 as my_alias, val0 as other_alias")
            .unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "my_alias",
                        "type": "float64",
                        "values": ["0", "1", "2"]
                    },
                    {
                        "name": "other_alias",
                        "type": "float64",
                        "values": ["0", "1", "2"]
                    }
                ]}"#
            )
        );
    }

    #[test]
    fn get_sum_no_group() {
        let table = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "val0",
                        "type": "float64",
                        "values": ["0", "1", "2"]
                    }
                ]
            }
            "#,
        );

        let mut collection = TableCollection::new();
        collection.add_table("tbl0", table);
        let result = collection.query("from tbl0 get sum(val0)").unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "sum(val0)",
                        "type": "float64",
                        "values": ["3"]
                    }
                ]}"#
            )
        );
    }

    #[test]
    fn test_sum_of_average() {
        let table = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "col0",
                        "type": "text",
                        "values": ["A", "A", "A", "B", "B", "C"]
                    },
                    {
                        "name": "col1",
                        "type": "text",
                        "values": ["x", "x", "y", "x", "x", "z"]
                    },
                    {
                        "name": "val0",
                        "type": "float64",
                        "values": ["1", "3", "4", "5", "7", "8"]
                    }
                ]
            }
            "#,
        );

        let mut collection = TableCollection::new();
        collection.add_table("tbl0", table);
        let result = collection
            .query(
                r#"
                from tbl0
                group by col0 get col0, (
                    group by col1 get avg(val0) as val0
                    get sum(val0)
                )
                "#,
            )
            .unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "col0",
                        "type": "text",
                        "values": ["A", "B", "C"]
                    },
                    {
                        "name": "sum(val0)",
                        "type": "float64",
                        "values": ["6", "6", "8"]
                    }
                ]}"#
            )
        );
    }

    #[test]
    fn basic_sort() {
        let table = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "id",
                        "type": "text",
                        "values": ["id0", "id1", "id2", "id3", "id4"]
                    },
                    {
                        "name": "col0",
                        "type": "float64",
                        "values": ["3", "1", "3", "2", "0"]
                    },
                    {
                        "name": "col1",
                        "type": "float64",
                        "values": ["5", "4", "3", "2", "1"]
                    }
                ]
            }
            "#,
        );

        let mut collection = TableCollection::new();
        collection.add_table("tbl0", table);

        let result = collection
            .query(
                r#"
                from tbl0
                order by col0
                get id
                "#,
            )
            .unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "id",
                        "type": "text",
                        "values": ["id4", "id1", "id3", "id0", "id2"]
                    }
                ]}"#
            )
        );

        let result = collection
            .query(
                r#"
                from tbl0
                order by col0, col1
                get id
                "#,
            )
            .unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "id",
                        "type": "text",
                        "values": ["id4", "id1", "id3", "id2", "id0"]
                    }
                ]}"#
            )
        );

        let result = collection
            .query(
                r#"
                from tbl0
                order by col0 asc
                get id
                "#,
            )
            .unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "id",
                        "type": "text",
                        "values": ["id4", "id1", "id3", "id0", "id2"]
                    }
                ]}"#
            )
        );

        let result = collection
            .query(
                r#"
                from tbl0
                order by col0 desc
                get id
                "#,
            )
            .unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "id",
                        "type": "text",
                        "values": ["id0", "id2", "id3", "id1", "id4"]
                    }
                ]}"#
            )
        );
    }

    #[test]
    fn basic_limit() {
        let table = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "id",
                        "type": "text",
                        "values": ["id0", "id1", "id2"]
                    }
                ]
            }
            "#,
        );

        let mut collection = TableCollection::new();
        collection.add_table("tbl0", table);

        let result = collection
            .query(
                r#"
                from tbl0
                limit 2
                "#,
            )
            .unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "id",
                        "type": "text",
                        "values": ["id0", "id1"]
                    }
                ]}"#
            )
        );

        let result = collection
            .query(
                r#"
                from tbl0
                limit 0
                "#,
            )
            .unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "id",
                        "type": "text",
                        "values": []
                    }
                ]}"#
            )
        );

        let result = collection
            .query(
                r#"
                from tbl0
                limit 9999999
                "#,
            )
            .unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "id",
                        "type": "text",
                        "values": ["id0", "id1", "id2"]
                    }
                ]}"#
            )
        );
    }
}
