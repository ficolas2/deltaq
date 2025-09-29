use deltalake::arrow::{self, datatypes::Fields};

use arrow::datatypes::{DataType, Field};
use std::fmt::Write as _;

pub fn arrow_type_to_delta_str(t: &DataType) -> Result<String, String> {
    fn rec(t: &DataType) -> Result<String, String> {
        match t {
            DataType::Boolean => Ok("boolean".into()),
            DataType::Int8 => Ok("byte".into()),
            DataType::Int16 => Ok("short".into()),
            DataType::Int32 => Ok("integer".into()),
            DataType::Int64 => Ok("long".into()),
            DataType::Float32 => Ok("float".into()),
            DataType::Float64 => Ok("double".into()),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok("string".into()),
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Ok("binary".into()),
            DataType::Date32 | DataType::Date64 => Ok("date".into()),
            DataType::Timestamp(_, _) => {
                Ok("timestamp".into())
            }
            DataType::Decimal128(p, s) => {
                Ok(format!("decimal({},{})", p, s))
            }
            DataType::List(f) | DataType::LargeList(f) => {
                Ok(format!("array<{}>", rec(f.data_type())?))
            }
            DataType::FixedSizeList(f, _) => {
                Ok(format!("array<{}>", rec(f.data_type())?))
            }
            DataType::Struct(fields) => Ok(struct_to_delta(fields)?),
            DataType::Map(entry, _) => map_to_delta(entry),
            DataType::Dictionary(_, value_ty) => rec(value_ty.as_ref()),

            _ => Err(format!("unsupported Arrow type for Delta: {:?}", t)),
        }
    }

    fn struct_to_delta(fields: &Fields) -> Result<String, String> {
        let mut out = String::from("struct<");
        for (i, f) in fields.iter().enumerate() {
            if i > 0 { out.push(','); }
            let ty = rec(f.data_type())?;
            let _ = write!(out, "{}:{}{}", f.name(), ty, if f.is_nullable() { "" } else { "" });
        }
        out.push('>');
        Ok(out)
    }

    fn map_to_delta(entry: &Field) -> Result<String, String> {
        match entry.data_type() {
            DataType::Struct(children) if children.len() == 2 => {
                let key = &children[0];
                if key.is_nullable() {
                    return Err("Delta map key must be non-nullable".into());
                }
                let kt = rec(key.data_type())?;
                let vt = rec(children[1].data_type())?;
                Ok(format!("map<{},{}>", kt, vt))
            }
            _ => Err("invalid Arrow map layout".into()),
        }
    }

    rec(t)
}
