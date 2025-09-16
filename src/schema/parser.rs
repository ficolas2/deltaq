use std::fmt::Display;

use deltalake::{DataType, StructField};

use super::tokenizer::{Token, tokenize};

#[derive(Debug)]
pub struct ParseError {
    pub message: String,
}

impl From<String> for ParseError {
    fn from(message: String) -> Self {
        ParseError { message: message }
    }
}

impl ParseError {
    pub fn new(message: &str) -> Self {
        ParseError {
            message: message.to_string(),
        }
    }
}

impl Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ParseError: {}", self.message)
    }
}

pub fn parse_schema(schema_str: &str) -> Result<Vec<StructField>, ParseError> {
    let mut columns = Vec::new();
    let token_list = tokenize(schema_str);

    let mut i = 0;

    while i < token_list.len() {
        columns.push(parse_field(&token_list, &mut i)?);
        if i < token_list.len() {
            let token = next_token(&token_list, &mut i)?;
            if !matches!(token, Token::Comma(_)) {
                return Err(format!("Expected ',', got: {}", token).into());
            }
        }
    }

    Ok(columns)
}

fn parse_field(token_list: &[Token], i: &mut usize) -> Result<StructField, ParseError> {
    let token = next_token(token_list, i)?;
    let name = if let Token::Ident(_, ident) = token {
        ident
    } else {
        return Err(format!(
            "Expected column name at index {}, got: {}",
            token.get_index(),
            token
        )
        .into());
    };

    let token = next_token(token_list, i)?;
    if !matches!(token, Token::Colon(_)) {
        return Err(format!("Expected ':', got: {}", token).into());
    }

    let t = parse_type(token_list, i)?;

    let mut nullable = false;
    if matches!(peek_token(token_list, *i), Some(Token::Question(_))) {
        nullable = true;
        *i += 1;
    }

    Ok(StructField::new(name, t, nullable))
}

fn parse_type(token_list: &[Token], i: &mut usize) -> Result<DataType, ParseError> {
    let token = next_token(token_list, i)?;
    if let Token::Ident(token_index, type_str) = token {
        match type_str.as_str() {
            "string" => Ok(DataType::Primitive(deltalake::PrimitiveType::String)),
            "long" => Ok(DataType::Primitive(deltalake::PrimitiveType::Long)),
            "int" => Ok(DataType::Primitive(deltalake::PrimitiveType::Integer)),
            "short" => Ok(DataType::Primitive(deltalake::PrimitiveType::Short)),
            "byte" => Ok(DataType::Primitive(deltalake::PrimitiveType::Byte)),
            "float" => Ok(DataType::Primitive(deltalake::PrimitiveType::Float)),
            "double" => Ok(DataType::Primitive(deltalake::PrimitiveType::Double)),
            "boolean" => Ok(DataType::Primitive(deltalake::PrimitiveType::Boolean)),
            "binary" => Ok(DataType::Primitive(deltalake::PrimitiveType::Binary)),
            "date" => Ok(DataType::Primitive(deltalake::PrimitiveType::Date)),
            "timestamp" => Ok(DataType::Primitive(deltalake::PrimitiveType::Timestamp)),
            // TODO: TimestampNtz, Decimal, Array, Struct, Map
            _ => Err(format!("Unknown type at index {}: {}", token_index, type_str).into()),
        }
    } else {
        return Err(format!(
            "Expected type type at index {}, got: {}",
            token.get_index(),
            token
        )
        .into());
    }
}

fn peek_token<'a>(token_list: &'a [Token], i: usize) -> Option<&'a Token> {
    if i < token_list.len() {
        Some(&token_list[i])
    } else {
        None
    }
}

fn next_token<'a>(token_list: &'a [Token], i: &mut usize) -> Result<&'a Token, ParseError> {
    if *i < token_list.len() {
        let token = &token_list[*i];
        *i += 1;
        Ok(token)
    } else {
        Err("Unexpected end of input".to_string().into())
    }
}

#[cfg(test)]
mod test {
    use deltalake::DataType;
    use crate::schema::parser::parse_schema;

    #[test]
    fn test_parse_schema() {
        let schema_str = "name:string, age:int?, salary:double";
        let schema = parse_schema(schema_str).unwrap();
        assert_eq!(schema.len(), 3);
        assert_eq!(schema[0].name, "name");
        assert_eq!(schema[0].data_type, DataType::Primitive(deltalake::PrimitiveType::String));
        assert_eq!(schema[0].nullable, false);
        assert_eq!(schema[1].name, "age");
        assert_eq!(schema[1].data_type, DataType::Primitive(deltalake::PrimitiveType::Integer));
        assert_eq!(schema[1].nullable, true);
        assert_eq!(schema[2].name, "salary");
        assert_eq!(schema[2].data_type, DataType::Primitive(deltalake::PrimitiveType::Double));
        assert_eq!(schema[2].nullable, false);
    }
}
