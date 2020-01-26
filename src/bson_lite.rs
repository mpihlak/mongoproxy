/// A low overhead BSON parser that only looks at documen fields that are explicitly selected.
/// Useful if we only want to collect some metadata about BSON. Eg. what were the "collection" and
/// "db" of the MongoDb client request.
///
/// It works by having the caller specify a `FieldSelector`, indicating which fields
/// need to be collected. The parser then goes through the BSON, skipping unneeded
/// fields and collecting what was asked of it.
///
/// Returns a `HashMap` of `BsonValue`, keyed by the field name. If the same field exists
/// multiple times, the value that is encountered last is used.

use std::fmt;
use std::io::{self,Read,BufRead,Error,ErrorKind};
use std::collections::{HashMap,HashSet};
use byteorder::{LittleEndian, ReadBytesExt};


#[derive(Debug)]
pub struct FieldSelector<'a> {
    // Match labels keyed by the fully qualified element name (/ as separator) or alternatively
    // with the element position (@<position number> instead of name)
    matchers:       HashMap<&'a str, String>,

    // Map of subdocument prefixes that we are interested in. We're using this to skip
    // documents that don't contain anything interesting.
    match_prefixes: HashSet<&'a str>
}

impl <'a>FieldSelector<'a> {
    pub fn build() -> Self {
        FieldSelector {
            matchers: HashMap::new(),
            match_prefixes: HashSet::new(),
        }
    }

    pub fn with(mut self, match_label: &'a str, match_pattern: &'a str) -> Self {
        self.matchers.insert(match_pattern, match_label.to_owned());

        // Now make a note of all the prefixes leading up to the exact value. So that
        // encountering /foo/bar/baz we insert /foo/bar/baz, /foo/bar and /foo
        let mut prefix = match_pattern;
        while let Some(pos) = prefix.rfind('/') {
            prefix = &prefix[..pos];
            if !prefix.is_empty() {
                self.match_prefixes.insert(prefix);
            }
        }
        self
    }

    fn get(&self, field: &str) -> Option<&String> {
        self.matchers.get(field)
    }

    fn want_prefix(&self, prefix: &str) -> bool {
        self.match_prefixes.contains(prefix)
    }
}

#[derive(Debug,Clone)]
pub enum BsonValue {
    Float(f64),
    String(String),
    Int32(i32),
    Int64(i64),
    ObjectId([u8;12]),
    Boolean(bool),
    Placeholder(&'static str),
    None,
}

impl fmt::Display for BsonValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BsonValue::Float(v) => v.fmt(f),
            BsonValue::String(v) => write!(f, "\"{}\"", v),
            BsonValue::Int32(v) => v.fmt(f),
            BsonValue::Int64(v) => v.fmt(f),
            BsonValue::ObjectId(v) => write!(f, "ObjectId({:?})", v),
            BsonValue::Boolean(v) => v.fmt(f),
            BsonValue::Placeholder(v) => v.fmt(f),
            other => write!(f, "Other({:?})", other)
        }
    }
}

#[derive(Debug)]
pub struct BsonLiteDocument {
    doc:    HashMap<String, BsonValue>,
}

impl fmt::Display for BsonLiteDocument {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{ ")?;
        for (i, (k, v)) in self.doc.iter().enumerate() {
            let comma = if i == self.doc.len() - 1 { "" } else { "," };
            write!(f, "{}: {}{} ", k, v, comma)?;
        }
        write!(f, "}}")
    }
}

impl BsonLiteDocument {
    fn new() -> Self {
        BsonLiteDocument {
            doc: HashMap::new(),
        }
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        if let Some(s) = self.doc.get(key) {
            if let BsonValue::String(result) = s {
                return Some(result)
            }
        }
        None
    }

    #[allow(dead_code)]
    pub fn get_float(&self, key: &str) -> Option<f64> {
        if let Some(s) = self.doc.get(key) {
            if let BsonValue::Float(result) = s {
                return Some(*result)
            }
        }
        None
    }

    pub fn get_i32(&self, key: &str) -> Option<i32> {
        if let Some(s) = self.doc.get(key) {
            if let BsonValue::Int32(result) = s {
                return Some(*result)
            }
        }
        None
    }

    pub fn get_i64(&self, key: &str) -> Option<i64> {
        if let Some(s) = self.doc.get(key) {
            if let BsonValue::Int64(result) = s {
                return Some(*result)
            }
        }
        None
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.doc.contains_key(key)
    }

    fn insert(&mut self, key: String, value: BsonValue) {
        self.doc.insert(key, value);
    }

    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.doc.len()
    }
}

/// Parser guts, collect the selected values into a HashMap
#[allow(dead_code)]
fn parse_document(mut rdr: &mut impl BufRead,
                  selector: &FieldSelector,
                  prefix: &str,
                  position: u32,
                  mut doc: &mut BsonLiteDocument) -> io::Result<()> {

    let mut position = position;

    loop {
        position += 1;

        let elem_type = rdr.read_u8()?;

        if elem_type == 0x00 {
            break;
        }

        let elem_name = read_cstring(&mut rdr)?;

        let prefix_name = format!("{}/{}", prefix, elem_name);
        let prefix_pos = format!("{}/@{}", prefix, position);

        // Check if we just want the element name
        let want_field_name_by_pos = format!("{}/#{}", prefix, position);
        if let Some(item_key) = selector.get(&want_field_name_by_pos) {
            doc.insert(item_key.to_string(), BsonValue::String(elem_name.to_string()));
        }

        // Check if we just want the count of keys (i.e array len)
        // Take a simple approach and just set the array len to current position
        // we end up updating it for every value, but the benefit is simplicity.
        let want_array_len = format!("{}/[]", prefix);
        if let Some(item_key) = selector.get(&want_array_len) {
            doc.insert(item_key.to_string(), BsonValue::Int32(position as i32));
        }

        // List of wanted elements. tuple of (name prefix, name alias)
        let mut wanted_elements = Vec::new();
        for elem_prefix in [&prefix_name, &prefix_pos].iter() {
            if let Some(elem_name) = selector.get(elem_prefix) {
                wanted_elements.push(elem_name);
            }
        }

        let want_this_value = !wanted_elements.is_empty();

        let elem_value =  match elem_type {
            0x01 => {
                // Float
                BsonValue::Float(rdr.read_f64::<LittleEndian>()?)
            },
            0x02 => {
                // String
                let str_len = rdr.read_i32::<LittleEndian>()?;
                if want_this_value {
                    BsonValue::String(read_string_with_len(&mut rdr, str_len as usize)?)
                } else {
                    skip_bytes(&mut rdr, str_len as usize)?;
                    BsonValue::None
                }
            },
            0x03 | 0x04 => {
                // Embedded document or an array. Both are represented as a document.
                // We only go through the trouble of parsing this if the field selector
                // wants the document value or some element within it.
                let _doc_len = rdr.read_i32::<LittleEndian>()?;
                if want_this_value || selector.want_prefix(&prefix_name) {
                    parse_document(rdr, selector, &prefix_name, 0, &mut doc)?;
                    BsonValue::Placeholder("<nested document>")
                } else {
                    skip_bytes(&mut rdr, _doc_len as usize - 4)?;
                    BsonValue::None
                }
            },
            0x05 => {
                // Binary data
                let len = rdr.read_i32::<LittleEndian>()?;
                skip_bytes(&mut rdr, (len+1) as usize)?;
                BsonValue::Placeholder("<binary data>")
            },
            0x06 => {
                // Undefined value. Deprecated.
                BsonValue::None
            },
            0x07 => {
                let mut bytes = [0 as u8; 12];
                rdr.read_exact(&mut bytes)?;
                BsonValue::ObjectId(bytes)
            },
            0x08 => {
                // Boolean
                let val = match rdr.read_u8()? { 0x00 => false, _ => true };
                BsonValue::Boolean(val)
            },
            0x09 => {
                // UTC Datetime
                skip_bytes(&mut rdr, 8)?;
                BsonValue::Placeholder("<UTC datetime>")
            },
            0x0A => {
                // Null value
                BsonValue::None
            },
            0x0B => {
                // Regular expression
                let _regx = read_cstring(&mut rdr)?;
                let _opts = read_cstring(&mut rdr)?;
                BsonValue::Placeholder("<regex>")
            },
            0x0C => {
                // DBPointer. Deprecated.
                let len = rdr.read_i32::<LittleEndian>()?;
                skip_bytes(&mut rdr, (len + 12) as usize)?;
                BsonValue::None
            },
            0x0D => {
                // Javascript code
                skip_read_len(&mut rdr)?;
                BsonValue::Placeholder("<Javascript>")
            },
            0x0E => {
                // Symbol. Deprecated.
                skip_read_len(&mut rdr)?;
                BsonValue::Placeholder("<symbol>")
            },
            0x0F => {
                // Code w/ scope
                // TODO: Test that this parses properly
                skip_read_len(&mut rdr)?;
                BsonValue::Placeholder("<Javascript with scope>")
            },
            0x10 => {
                // Int32
                BsonValue::Int32(rdr.read_i32::<LittleEndian>()?)
            },
            0x11 => {
                // Timestamp
                skip_bytes(&mut rdr, 8)?;
                BsonValue::Placeholder("<timestamp>")
            },
            0x12 => {
                // Int64
                BsonValue::Int64(rdr.read_i64::<LittleEndian>()?)
            },
            0x13 => {
                // Decimal128
                skip_bytes(&mut rdr, 16)?;
                BsonValue::Placeholder("<decimal128>")
            },
            0xFF => {
                // Min key.
                BsonValue::Placeholder("<min key>")
            },
            0x7F => {
                // Min key.
                BsonValue::Placeholder("<max key>")
            },
            other => {
                return Err(Error::new(ErrorKind::Other, format!("unrecognized type: 0x{:02x}", other)));
            },
        };

        for elem_name in wanted_elements.iter() {
            doc.insert(elem_name.to_string(), elem_value.clone());
        }
    }
    Ok(())
}

/// Parse the BSON document, collecting selected fields into a HashMap
pub fn decode_document(mut rdr: impl BufRead, selector: &FieldSelector) -> io::Result<BsonLiteDocument> {
    let _document_size = rdr.read_i32::<LittleEndian>()?;

    let mut doc = BsonLiteDocument::new();
    parse_document(&mut rdr, &selector, "", 0, &mut doc)?;

    Ok(doc)
}

fn skip_bytes<T: Read>(rdr: &mut T, bytes_to_skip: usize) -> io::Result<u64> {
    io::copy(&mut rdr.take(bytes_to_skip as u64), &mut io::sink())
}

fn skip_read_len<T: Read>(rdr: &mut T) -> io::Result<u64> {
    let str_len = rdr.read_i32::<LittleEndian>()?;
    skip_bytes(rdr, str_len as usize)
}

pub fn read_cstring(rdr: &mut impl BufRead) -> io::Result<String> {
    let mut bytes = Vec::new();

    rdr.read_until(0, &mut bytes)?;
    let _ = bytes.pop();

    if let Ok(res) = String::from_utf8(bytes) {
        return Ok(res)
    }

    Err(Error::new(ErrorKind::Other, "conversion error"))
}

fn read_string_with_len(rdr: impl Read, str_len: usize) -> io::Result<String> {
    let mut buf = Vec::with_capacity(str_len);
    rdr.take(str_len as u64).read_to_end(&mut buf)?;

    // Remove the trailing null, we won't need it
    let _ = buf.pop();

    if let Ok(res) = String::from_utf8(buf) {
        return Ok(res);
    }

    Err(Error::new(ErrorKind::Other, "conversion error"))
}

#[cfg(test)]

mod tests {
    use super::*;
    use env_logger;
    use bson::{Array,Bson,oid};

    #[test]
    fn test_parse_bson() {
        let _ = env_logger::builder().is_test(true).try_init();

        let mut doc = bson::Document::new();

        doc.insert("kala".to_owned(), bson::Bson::String("maja".to_owned()));
        doc.insert("puu".to_owned(), bson::Bson::FloatingPoint(3.14));

        // nested document that we want to look at
        let mut nested = bson::Document::new();
        let mut deeply_nested = bson::Document::new();
        deeply_nested.insert("name", bson::Bson::String("nilsson".to_owned()));
        nested.insert("monkey", deeply_nested);
        doc.insert("nested", nested);

        // and a nested document that we don't care about
        let mut nested = bson::Document::new();
        let mut deeply_nested = bson::Document::new();
        deeply_nested.insert("name", bson::Bson::String("johnsson".to_owned()));
        nested.insert("monkey", deeply_nested);
        doc.insert("nested-ignore", nested);

        doc.insert("bool".to_owned(), bson::Bson::Boolean(true));
        doc.insert("eee".to_owned(), bson::Bson::FloatingPoint(2.7));
        println!("original: {}", doc);

        let mut buf = Vec::new();
        bson::encode_document(&mut buf, &doc).unwrap();

        let selector = FieldSelector::build()
            .with("first", "/@1")
            .with("first_elem_name", "/#1")
            .with("e", "/eee")
            .with("b", "/bool")
            .with("c", "/deeply/nested/array/len/[]")
            .with("monkey", "/nested/monkey/name");
        println!("matching fields: {:?}", selector);
        let doc = decode_document(&buf[..], &selector).unwrap();
        println!("decoded: {}", doc);

        assert_eq!("kala", doc.get_str("first_elem_name").unwrap());
        assert_eq!("maja", doc.get_str("first").unwrap());
        assert_eq!(2.7, doc.get_float("e").unwrap());
        assert_eq!("nilsson", doc.get_str("monkey").unwrap());
        assert_eq!(5, doc.len());
    }

    #[test]
    fn test_array() {
        let _ = env_logger::builder().is_test(true).try_init();

        let mut doc = bson::Document::new();

        doc.insert("first".to_string(), Bson::String("foo".to_string()));

        let mut arr = Array::new();
        arr.push(Bson::String("blah".to_string()));
        arr.push(Bson::ObjectId(oid::ObjectId::with_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])));

        doc.insert("array".to_string(), Bson::Array(arr));
        doc.insert("last".to_owned(), bson::Bson::FloatingPoint(2.7));

        let mut buf = Vec::new();
        bson::encode_document(&mut buf, &doc).unwrap();
        println!("original: {}", &doc);

        let selector = FieldSelector::build()
            .with("first", "/@1")
            .with("array_len", "/array/[]")
            .with("array_first", "/array/@1")
            .with("last", "/last");
        println!("matching fields: {:?}", selector);

        let doc = decode_document(&buf[..], &selector).unwrap();
        println!("decoded: {}", &doc);

        assert_eq!("foo", doc.get_str("first").unwrap());
        assert_eq!(2, doc.get_i32("array_len").unwrap());
        assert_eq!("blah", doc.get_str("array_first").unwrap());
        assert_eq!(2.7, doc.get_float("last").unwrap());
    }

    #[test]
    fn test_nested_array() {
        let _ = env_logger::builder().is_test(true).try_init();

        let mut doc = bson::Document::new();

        let mut arr = Array::new();
        let mut nested_doc = bson::Document::new();
        nested_doc.insert("foo".to_owned(), 42);
        nested_doc.insert("bar".to_owned(), 43);
        arr.push(Bson::Document(nested_doc));
        let mut nested_doc = bson::Document::new();
        nested_doc.insert("foo".to_owned(), 44);
        nested_doc.insert("bar".to_owned(), 45);
        nested_doc.insert("baz".to_owned(), 46);
        arr.push(Bson::Document(nested_doc));

        doc.insert("array".to_string(), Bson::Array(arr));

        let mut buf = Vec::new();
        bson::encode_document(&mut buf, &doc).unwrap();
        println!("original: {}", &doc);

        let selector = FieldSelector::build()
            .with("array_len", "/array/[]")
            .with("array_first_foo", "/array/0/foo")
            .with("array_any_baz", "/array/*/baz");

        println!("matching fields: {:?}", selector);

        let doc = decode_document(&buf[..], &selector).unwrap();
        println!("decoded: {}", &doc);

        assert_eq!(2, doc.get_i32("array_len").unwrap());
        assert_eq!(42, doc.get_i32("array_first_foo").unwrap());
        // assert_eq!(46, doc.get_i32("array_any_baz").unwrap());
    }

    use std::io::{Cursor};

    #[test]
    fn test_read_cstring() {
        let buf = b"kala\0";
        let res = read_cstring(&mut Cursor::new(&buf[..])).unwrap();
        assert_eq!(res, "kala");

        let buf = b"\0";
        let res = read_cstring(&mut Cursor::new(&buf[..])).unwrap();
        assert_eq!(res, "");
    }
}
