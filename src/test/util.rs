use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::logical_plan::{LogicalPlan};

// pub fn test_table_scan() -> Result<LogicalPlan> {
//     let schema = Schema::new(vec![
//         Field::new("a", DataType::UInt32, false),
//         Field::new("b", DataType::UInt32, false),
//         Field::new("c", DataType::UInt32, false),
//     ]);
//     LogicalPlanBuilder::scan("default", "test", &schema, None)?.build()
// }

pub fn assert_fields_eq(plan: &LogicalPlan, expected: Vec<&str>) {
    let actual: Vec<String> = plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(actual, expected);
}
