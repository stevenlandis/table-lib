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
    fn where_literal() {
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

        let result = collection.query("from tbl0 where true get id").unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "id",
                        "type": "text",
                        "values": ["id0", "id1", "id2", "id3"]
                    }
                ]}"#
            )
        );

        let result = collection.query("from tbl0 where false get id").unwrap();

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

    #[test]
    fn sum_plus_value() {
        let table = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "id",
                        "type": "text",
                        "values": ["id0", "id0", "id0", "id1", "id2", "id1"]
                    },
                    {
                        "name": "val",
                        "type": "float64",
                        "values": ["1", "2", "3", "4", null, "5"]
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
                group by id get id, val + sum(val) + 1 as val
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
                        "values": ["id0", "id0", "id0", "id1", "id1", "id2"]
                    },
                    {
                        "name": "val",
                        "type": "float64",
                        "values": ["8", "9", "10", "14", "15", null]
                    }
                ]}"#
            )
        );
    }

    #[test]
    fn double_aggregated_scalar_addition() {
        let table = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "id0",
                        "type": "text",
                        "values": ["id0", "id0", "id0", "id1", "id2", "id1"]
                    },
                    {
                        "name": "id1",
                        "type": "text",
                        "values": ["x", "x", "y", "x", "x", "x"]
                    },
                    {
                        "name": "val",
                        "type": "float64",
                        "values": ["1", "2", "3", "4", "6", "5"]
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
                group by id0 get id0, (
                    group by id1 get id1, val + 1 as val
                )
                "#,
            )
            .unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "id0",
                        "type": "text",
                        "values": ["id0", "id0", "id0", "id1", "id1", "id2"]
                    },
                    {
                        "name": "id1",
                        "type": "text",
                        "values": ["x", "x", "y", "x", "x", "x"]
                    },
                    {
                        "name": "val",
                        "type": "float64",
                        "values": ["2", "3", "4", "5", "6", "7"]
                    }
                ]}"#
            )
        );
    }

    #[test]
    fn multi_row_agg_fcn() {
        let table = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "id0",
                        "type": "text",
                        "values": ["id0", "id1", "id0", "id0", "id2", "id2", "id2", "id2"]
                    },
                    {
                        "name": "val",
                        "type": "float64",
                        "values": ["0", "1", "2", "3", "4", "5", "6", "7"]
                    }
                ]
            }
            "#,
        );

        let mut collection = TableCollection::new();
        collection.add_table("tbl0", table);

        let result = collection
            .query(
                r#"from tbl0 group by id0 get (limit 2)
                "#,
            )
            .unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "id0",
                        "type": "text",
                        "values": ["id0", "id0", "id1", "id2", "id2"]
                    },
                    {
                        "name": "val",
                        "type": "float64",
                        "values": ["0", "2", "1", "4", "5"]
                    }
                ]}"#
            )
        );
    }

    #[test]
    fn basic_window() {
        let table = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "id0",
                        "type": "text",
                        "values": ["id0", "id1", "id2", "id3", "id4", "id5"]
                    },
                    {
                        "name": "val",
                        "type": "float64",
                        "values": ["1", "2", "3", "4", "5", "6"]
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
                window 3 get
                    first(id0) as first_id,
                    last(id0) as last_id,
                    sum(val) as val,
                    last(val) - first(val) as delta
                "#,
            )
            .unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "first_id",
                        "type": "text",
                        "values": ["id0", "id1", "id2", "id3"]
                    },
                    {
                        "name": "last_id",
                        "type": "text",
                        "values": ["id2", "id3", "id4", "id5"]
                    },
                    {
                        "name": "val",
                        "type": "float64",
                        "values": ["6", "9", "12", "15"]
                    },
                    {
                        "name": "delta",
                        "type": "float64",
                        "values": ["2", "2", "2", "2"]
                    }
                ]}"#
            )
        );
    }

    #[test]
    fn basic_count() {
        let table = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "id",
                        "type": "text",
                        "values": ["id0", "id1", "id0", "id0", "id2", "id1"]
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
                group by id get id, count()
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
                    },
                    {
                        "name": "count()",
                        "type": "float64",
                        "values": ["3", "2", "1"]
                    }
                ]}"#
            )
        );

        let result = collection
            .query(
                r#"
                from tbl0 get count()
                "#,
            )
            .unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "count()",
                        "type": "float64",
                        "values": ["6"]
                    }
                ]}"#
            )
        );
    }

    #[test]
    fn basic_min_and_max() {
        let table = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "val",
                        "type": "float64",
                        "values": ["0", "2", "1"]
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
                get min(val), max(val)
                "#,
            )
            .unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "min(val)",
                        "type": "float64",
                        "values": ["0"]
                    },
                    {
                        "name": "max(val)",
                        "type": "float64",
                        "values": ["2"]
                    }
                ]}"#
            )
        );
    }

    #[test]
    fn basic_divide() {
        let table = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "val0",
                        "type": "float64",
                        "values": ["8", "1", "0", "2", null, null]
                    },
                    {
                        "name": "val1",
                        "type": "float64",
                        "values": ["2", "0", "1", null, "2", null]
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
                get val0 / val1
                "#,
            )
            .unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "(val0 / val1)",
                        "type": "float64",
                        "values": ["4", null, "0", null, null, null]
                    }
                ]}"#
            )
        );
    }

    #[test]
    fn basic_multiply() {
        let table = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "val0",
                        "type": "float64",
                        "values": ["8", "1", "0", "2", null, null]
                    },
                    {
                        "name": "val1",
                        "type": "float64",
                        "values": ["2", "0", "1", null, "2", null]
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
                get val0 * val1
                "#,
            )
            .unwrap();

        assert_eq!(
            result,
            Table::from_json_str(
                r#"{"columns":[
                    {
                        "name": "(val0 * val1)",
                        "type": "float64",
                        "values": ["16", "0", "0", null, null, null]
                    }
                ]}"#
            )
        );
    }

    #[test]
    fn aggregate_empty_partition() {
        let table = Table::from_json_str(
            r#"
            {
                "columns": [
                    {
                        "name": "id",
                        "type": "text",
                        "values": ["id0", "id1", "id1", "id0", "id2", "id3", "id3", "id0"]
                    },
                    {
                        "name": "include",
                        "type": "bool",
                        "values": ["true", "true", "true", "false", "false", "true", "true", "true"]
                    },
                    {
                        "name": "val1",
                        "type": "float64",
                        "values": ["1", "2", "3", "4", "5", "6", "7", "8"]
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
                group by id get id, (
                    where include
                    get first(val1), last(val1), sum(val1)
                )
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
                        "values": ["id0", "id1", "id2", "id3"]
                    },
                    {
                        "name": "first(val1)",
                        "type": "float64",
                        "values": ["1", "2", null, "6"]
                    },
                    {
                        "name": "last(val1)",
                        "type": "float64",
                        "values": ["8", "3", null, "7"]
                    },
                    {
                        "name": "sum(val1)",
                        "type": "float64",
                        "values": ["9", "5", "0", "13"]
                    }
                ]}"#
            )
        );
    }
}
