use datafusion::scalar::ScalarValue;

pub fn to_utf8(scalar_value: ScalarValue) -> Option<String> {
    match scalar_value {
        ScalarValue::Utf8(value) => value,
        _ => None,
    }
}