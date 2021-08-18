// use std::collections::HashMap;
// use std::sync::Arc;
//
// use arrow::array::{ArrayRef, Float64Array, Int32Array, StringArray, Array};
// use arrow::compute::add;
// use arrow::datatypes::*;
// use datafusion::logical_plan::{create_udf};
// use datafusion::physical_plan::udf::ScalarUDF;
// use datafusion::physical_plan::udaf::AggregateUDF;
// use datafusion::physical_plan::functions::ScalarFunctionImplementation;
//
// use crate::core::context::CoreContext;
//
// #[derive(Clone, Debug)]
// pub struct UdfContext {
//     pub scalar_functions: HashMap<String, ScalarUDF>,
//     pub aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
// }
//
// impl UdfContext {
//     pub fn init () -> Self {
//         let mut scalar_functions:HashMap<String, ScalarUDF> = Default::default();
//         let mut aggregate_functions:HashMap<String, Arc<AggregateUDF>> = Default::default();
//
//         scalar_functions.insert(String::from("my_add"), my_add());
//
//         Self {
//             scalar_functions,
//             aggregate_functions,
//         }
//     }
// }
//
// fn my_add () -> ScalarUDF{
//
//     let myfunc: ScalarFunctionImplementation = |args: &[ArrayRef]| {
//         let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
//         Ok(Arc::new(array) as ArrayRef)
//     };
//
//     create_udf(
//         "my_add",
//         vec![DataType::Int32, DataType::Int32],
//         Arc::new(DataType::Int32),
//         myfunc,
//     )
// }
