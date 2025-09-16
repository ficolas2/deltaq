use std::fmt::Display;

use deltalake::{ArrayType, DataType, MapType, PrimitiveType, StructField, StructType};

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
    let token_list = tokenize(schema_str);
    let mut i = 0;
    parse_field_list(&token_list, &mut i)
}

fn parse_field_list(token_list: &[Token], i: &mut usize) -> Result<Vec<StructField>, ParseError> {
    let mut columns = Vec::new();

    while *i < token_list.len() {
        columns.push(parse_field(&token_list, i)?);
        if *i < token_list.len() {
            expect_comma(&token_list, i)?;
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

    expect_colon(token_list, i)?;
    let t = parse_type(token_list, i)?;
    let nullable = check_nullable(token_list, i);

    Ok(StructField::new(name, t, nullable))
}

fn parse_type(token_list: &[Token], i: &mut usize) -> Result<DataType, ParseError> {
    let token = next_token(token_list, i)?;
    if let Token::Ident(token_index, type_str) = token {
        match type_str.as_str() {
            "string" => Ok(DataType::Primitive(PrimitiveType::String)),
            "long" => Ok(DataType::Primitive(PrimitiveType::Long)),
            "int" => Ok(DataType::Primitive(PrimitiveType::Integer)),
            "short" => Ok(DataType::Primitive(PrimitiveType::Short)),
            "byte" => Ok(DataType::Primitive(PrimitiveType::Byte)),
            "float" => Ok(DataType::Primitive(PrimitiveType::Float)),
            "double" => Ok(DataType::Primitive(PrimitiveType::Double)),
            "boolean" => Ok(DataType::Primitive(PrimitiveType::Boolean)),
            "binary" => Ok(DataType::Primitive(PrimitiveType::Binary)),
            "date" => Ok(DataType::Primitive(PrimitiveType::Date)),
            "timestamp" => Ok(DataType::Primitive(PrimitiveType::Timestamp)),
            "array" => parse_array_type(token_list, i),
            "struct" => parse_struct_type(token_list, i),
            "map" => parse_map_type(token_list, i),
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

pub fn parse_array_type(token_list: &[Token], i: &mut usize) -> Result<DataType, ParseError> {
    expect_lt(token_list, i)?;
    let elem = parse_type(token_list, i)?;
    let nullable = check_nullable(token_list, i);
    expect_gt(token_list, i)?;

    Ok(DataType::Array(Box::new(ArrayType {
        type_name: "".into(),
        element_type: elem,
        contains_null: nullable,
    })))
}

pub fn parse_struct_type(token_list: &[Token], i: &mut usize) -> Result<DataType, ParseError> {
    expect_lt(token_list, i)?;
    let gt_index = find_matching_gt(token_list, *i)?;
    let fields = parse_field_list(&token_list[0..gt_index], i)?;
    expect_gt(token_list, i)?;

    Ok(DataType::Struct(Box::new(StructType::new(fields))))
}

pub fn parse_map_type(token_list: &[Token], i: &mut usize) -> Result<DataType, ParseError> {
    expect_lt(token_list, i)?;
    let key_type = parse_type(token_list, i)?;
    expect_comma(token_list, i)?;
    let value_type = parse_type(token_list, i)?;
    let nullable = check_nullable(token_list, i);
    expect_gt(token_list, i)?;

    Ok(DataType::Map(Box::new(MapType {
        type_name: "".into(),
        key_type: key_type,
        value_type: value_type,
        value_contains_null: nullable,
    })))
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

fn expect<F>(token_list: &[Token], i: &mut usize, what: &str, ok: F) -> Result<(), ParseError>
where
    F: Fn(&Token) -> bool,
{
    let t = next_token(token_list, i)?;
    if !ok(t) {
        return Err(format!("Expected {}, got: {}", what, t).into());
    }
    Ok(())
}

fn expect_comma(token_list: &[Token], i: &mut usize) -> Result<(), ParseError> {
    expect(token_list, i, "','", |t| matches!(t, Token::Comma(_)))
}
fn expect_lt(token_list: &[Token], i: &mut usize) -> Result<(), ParseError> {
    expect(token_list, i, "'<'", |t| matches!(t, Token::Lt(_)))
}
fn expect_gt(token_list: &[Token], i: &mut usize) -> Result<(), ParseError> {
    expect(token_list, i, "'>'", |t| matches!(t, Token::Gt(_)))
}

fn expect_colon(token_list: &[Token], i: &mut usize) -> Result<(), ParseError> {
    expect(token_list, i, "':'", |t| matches!(t, Token::Colon(_)))
}

fn check_nullable(token_list: &[Token], i: &mut usize) -> bool {
    let mut nullable = false;
    if matches!(peek_token(token_list, *i), Some(Token::Question(_))) {
        nullable = true;
        *i += 1;
    }
    nullable
}

fn find_matching_gt(tokens: &[Token], start: usize) -> Result<usize, ParseError> {
    let mut depth = 1;
    let mut i = start;

    while i < tokens.len() {
        match &tokens[i] {
            Token::Lt(_) => depth += 1,
            Token::Gt(_) => {
                depth -= 1;
                if depth == 0 {
                    return Ok(i);
                }
            }
            _ => {}
        }
        i += 1;
    }

    Err("Unmatched '<' in type expression".to_string().into())
}

#[cfg(test)]
mod test {
    use crate::schema::parser::parse_schema;
    use deltalake::{ArrayType, DataType, MapType, PrimitiveType, StructField, StructType};

    #[test]
    fn test_parse_schema() {
        let schema_str = "name:string, age :int?, salary: double";
        let schema = parse_schema(schema_str).unwrap();
        assert_eq!(schema.len(), 3);
        assert_eq!(schema[0].name, "name");
        assert_eq!(
            schema[0].data_type,
            DataType::Primitive(PrimitiveType::String)
        );
        assert_eq!(schema[0].nullable, false);
        assert_eq!(schema[1].name, "age");
        assert_eq!(
            schema[1].data_type,
            DataType::Primitive(PrimitiveType::Integer)
        );
        assert_eq!(schema[1].nullable, true);
        assert_eq!(schema[2].name, "salary");
        assert_eq!(
            schema[2].data_type,
            DataType::Primitive(PrimitiveType::Double)
        );
        assert_eq!(schema[2].nullable, false);
    }

    #[test]
    fn test_parse_array() {
        let schema_str = "arr_name:array<string>, arr_name2:array<int?>";
        let schema = parse_schema(schema_str).unwrap();

        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].name, "arr_name");
        assert_eq!(
            schema[0].data_type(),
            &DataType::Array(Box::new(ArrayType {
                type_name: "".into(),
                element_type: DataType::Primitive(PrimitiveType::String),
                contains_null: false,
            }))
        );
        assert_eq!(schema[1].name, "arr_name2");
        assert_eq!(
            schema[1].data_type(),
            &DataType::Array(Box::new(ArrayType {
                type_name: "".into(),
                element_type: DataType::Primitive(PrimitiveType::Integer),
                contains_null: true,
            }))
        );
    }

    #[test]
    fn test_parse_struct() {
        let schema_str = "struct_name: struct<id: string, count: int>";
        let schema = parse_schema(schema_str).unwrap();

        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].name, "struct_name");
        assert_eq!(
            schema[0].data_type(),
            &DataType::Struct(Box::new(StructType::new(vec![
                StructField::new("id", DataType::Primitive(PrimitiveType::String), false),
                StructField::new("count", DataType::Primitive(PrimitiveType::Integer), false),
            ])))
        )
    }

    #[test]
    fn test_parse_map() {
        let schema_str = "map_name: map<string, string>, map_name2: map<string, int?>";
        let schema = parse_schema(schema_str).unwrap();

        assert_eq!(schema.len(), 2);

        assert_eq!(schema[0].name, "map_name");
        assert_eq!(
            schema[0].data_type(),
            &DataType::Map(Box::new(MapType {
                type_name: "".into(),
                key_type: DataType::Primitive(PrimitiveType::String),
                value_type: DataType::Primitive(PrimitiveType::String),
                value_contains_null: false,
            }))
        );

        assert_eq!(schema[1].name, "map_name2");
        assert_eq!(
            schema[1].data_type(),
            &DataType::Map(Box::new(MapType {
                type_name: "".into(),
                key_type: DataType::Primitive(PrimitiveType::String),
                value_type: DataType::Primitive(PrimitiveType::Integer),
                value_contains_null: true,
            }))
        );
    }
}
