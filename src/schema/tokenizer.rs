use std::fmt::{Display, Formatter};

#[derive(PartialEq, Debug)]
pub enum Token {
    Ident(usize, String),
    Colon(usize),    // :
    Comma(usize),    // ,
    Question(usize), // ?
    Lt(usize),       // <
    Gt(usize),       // >
    LParen(usize),   // (
    RParen(usize),   // )
}

impl Token {
    pub fn get_index (&self) -> usize {
        match self {
            Token::Ident(i, _) => *i,
            Token::Colon(i) => *i,
            Token::Comma(i) => *i,
            Token::Question(i) => *i,
            Token::Lt(i) => *i,
            Token::Gt(i) => *i,
            Token::LParen(i) => *i,
            Token::RParen(i) => *i,
        }
    }
}

impl Display for Token {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::Ident(_, s) => write!(f, "Ident: {}", s),
            Token::Colon(_) => write!(f, ":"),
            Token::Comma(_) => write!(f, ","),
            Token::Question(_) => write!(f, "?"),
            Token::Lt(_) => write!(f, "<"),
            Token::Gt(_) => write!(f, ">"),
            Token::LParen(_) => write!(f, "("),
            Token::RParen(_) => write!(f, ")"),
        }
    }
}

pub fn tokenize(schema_str: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut buf = String::new();
    let mut buf_start: usize = 0;

    let flush = |buf: &mut String, buf_start: usize, tokens: &mut Vec<Token>| {
        if !buf.is_empty() {
            tokens.push(Token::Ident(buf_start, buf.clone()));
            buf.clear();
        }
    };

    for (i, c) in schema_str.chars().enumerate() {
        match c {
            ':' => {
                flush(&mut buf, buf_start, &mut tokens);
                tokens.push(Token::Colon(i));
            }
            ',' => {
                flush(&mut buf, buf_start, &mut tokens);
                tokens.push(Token::Comma(i));
            }
            '?' => {
                flush(&mut buf, buf_start, &mut tokens);
                tokens.push(Token::Question(i));
            }
            '<' => {
                flush(&mut buf, buf_start, &mut tokens);
                tokens.push(Token::Lt(i));
            }
            '>' => {
                flush(&mut buf, buf_start, &mut tokens);
                tokens.push(Token::Gt(i));
            }
            '(' => {
                flush(&mut buf, buf_start, &mut tokens);
                tokens.push(Token::LParen(i));
            }
            ')' => {
                flush(&mut buf, buf_start, &mut tokens);
                tokens.push(Token::RParen(i));
            }
            c if c.is_whitespace() => flush(&mut buf, buf_start, &mut tokens),
            c => {
                if buf.is_empty() {
                    buf_start = i;
                }
                buf.push(c)
            }
        }
    }

    flush(&mut buf, buf_start, &mut tokens);

    tokens
}
