use sqlparser::ast::{ObjectName, Ident};

pub trait ToObjectName {
    fn to_object_name(&self) -> ObjectName;
}

impl ToObjectName for str {
    fn to_object_name(&self) -> ObjectName {
        let object_names: Vec<&str> = self.split(".").collect();

        let mut idents = vec![];
        for object_name in object_names {
            let ident = Ident::new(object_name);
            idents.push(ident);
        }

        let object_name = ObjectName(idents);
        object_name
    }
}

impl ToObjectName for String {
    fn to_object_name(&self) -> ObjectName {
        self.as_str().to_object_name()
    }
}

pub trait ToLowercase {
    fn to_lowercase(&self) -> ObjectName;
}

impl ToLowercase for ObjectName {
    fn to_lowercase(&self) -> ObjectName {
        convert_object_name_to_lowercase(self.clone())
    }
}

pub fn convert_object_name_to_lowercase(object_name: ObjectName) -> ObjectName {
    let mut idents = vec![];
    for ident in object_name.0 {
        idents.push(convert_ident_to_lowercase(&ident));
    }

    ObjectName(idents)
}

pub fn convert_ident_to_lowercase(ident: &Ident) -> Ident {
    match ident.quote_style {
        None => {
            Ident::new(ident.value.to_lowercase())
        }
        Some(quote) => {
            Ident::with_quote(quote, ident.value.to_lowercase())
        }
    }
}

pub trait ToIdent {
    fn to_ident(&self) -> Ident;
}

impl ToIdent for str {
    fn to_ident(&self) -> Ident {
        Ident::new(self)
    }
}

impl ToIdent for String {
    fn to_ident(&self) -> Ident {
        Ident::new(self)
    }
}
//
// pub trait ScalarValueToString {
//     fn to_string(&self) -> MysqlResult<Option<String>>;
// }
//
// impl ScalarValueToString for ScalarValue {
//     fn to_string(&self) -> MysqlResult<Option<String>> {
//         match self {
//             ScalarValue::Int32(limit) => {
//                 if let Some(value) = limit {
//                     let new_value = (value as u64) ^ meta_const::SIGN_MASK;
//                     Ok(Some(new_value.to_string()))
//                 } else {
//                     Ok(None)
//                 }
//             }
//             ScalarValue::Int64(limit) => {
//                 if let Some(value) = limit {
//                     let new_value = (value as u64) ^ meta_const::SIGN_MASK;
//                     Ok(Some(new_value.to_string()))
//                 } else {
//                     Ok(None)
//                 }
//             }
//             ScalarValue::Utf8(limit) => {
//                 if let Some(value) = limit {
//                     Ok(Some(value.to_string()))
//                 } else {
//                     Ok(None)
//                 }
//             }
//             _ => Err(MysqlError::new_global_error(
//                 meta_const::MYSQL_ERROR_CODE_UNKNOWN_ERROR,
//                 format!("Unsupported convert scalar value to string: {:?}", self).as_str(),
//             )),
//         }
//     }
// }

