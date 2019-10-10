/// This is a low overhead BSON parser that only looks at a tiny subset of support
/// data types. Useful if we only want to collect some metadata about BSON. Eg. what
/// were the "collection" and "db" of the MongoDb client request.
///
/// It works by letting the caller specify a FieldSelector, indicating which fields
/// need to be collected. The parser then goes through the BSON, skipping unneeded
/// fields and collecting what needs to be collected.
///
/// Returns a HashMap of BsonValue, keyed by the field name. If the same field exists
/// multiple times, the value that is encountered last is used.

use std::io::{self,Read,Error,ErrorKind};
use std::collections::HashMap;
use byteorder::{LittleEndian, ReadBytesExt};

use log::{debug};


#[derive(Debug)]
pub struct FieldSelector<'a> {
    // Match labels keyed by the fully qualified element name (/ as separator) or alternatively
    // with the element position (@<position number> instead of name)
    matchers:   HashMap<&'a str, String>,
}

impl <'a>FieldSelector<'a> {
    pub fn build() -> Self {
        FieldSelector {
            matchers: HashMap::new(),
        }
    }

    pub fn with(mut self, match_label: &'a str, match_pattern: &'a str) -> Self {
        self.matchers.insert(match_pattern, match_label.to_owned());
        self
    }

    fn get(&self, field: &str) -> Option<&String> {
        self.matchers.get(field)
    }
}

#[derive(Debug)]
pub enum BsonValue {
    Float(f64),
    String(String),
    Int32(i32),
    Int64(i64),
    ObjectId([u8;12]),
    Boolean(bool),
    Placeholder(String),
    None,
}

#[derive(Debug)]
pub struct BsonLiteDocument {
    doc:    HashMap<String, BsonValue>,
}

impl BsonLiteDocument {
    fn new() -> Self {
        BsonLiteDocument {
            doc: HashMap::new(),
        }
    }

    pub fn get_str(&self, key: &str) -> Option<String> {
        if let Some(s) = self.doc.get(key) {
            if let BsonValue::String(result) = s {
                return Some(result.clone())
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
fn parse_document<R: Read>(
                  mut rdr: &mut R,
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

        debug!("elem type=0x{:0x} name={}", elem_type, elem_name);

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

        // See if any of the wanted elements matches either name or position at current level
        let want_this_key = &[&prefix_name, &prefix_pos]
            .iter()
            .map(|x| selector.get(x))
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .next();

        let elem_value =  match elem_type {
            0x01 => {
                // Float
                BsonValue::Float(rdr.read_f64::<LittleEndian>()?)
            },
            0x02 => {
                // String
                let str_len = rdr.read_i32::<LittleEndian>()?;
                BsonValue::String(read_string_with_len(&mut rdr, str_len as usize)?)
            },
            0x03 | 0x04 => {
                // Embedded document or an array. Both are represented as a document.
                let _doc_len = rdr.read_i32::<LittleEndian>()?;
                // TODO: Here we could also choose to skip the nested document if none
                // of it's elements are selected.
                // XXX: When parsing a nested document we need to deal with the position
                // restarting from zero. For now just move it forward.
                parse_document(rdr, selector, &prefix_name, 0, &mut doc)?;
                // For now, don't collect the whole subdocument. Instead have the user
                // explicitly pick out any fields from there with a FieldSelector.
                // TODO: What if we need array length?
                BsonValue::None
            },
            0x05 => {
                // Binary data
                let len = rdr.read_i32::<LittleEndian>()?;
                skip_bytes(&mut rdr, (len+1) as usize)?;
                BsonValue::Placeholder(String::from("TODO: binary data"))
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
                BsonValue::Placeholder(String::from("TODO: UTC datetime"))
            },
            0x0A => {
                // Null value
                BsonValue::Placeholder(String::from("TODO: NULL"))
            },
            0x0B => {
                // Regular expression
                let _regx = read_cstring(&mut rdr)?;
                let _opts = read_cstring(&mut rdr)?;
                BsonValue::Placeholder(String::from("TODO: Regex"))
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
                BsonValue::Placeholder(String::from("TODO: Javascript"))
            },
            0x0E => {
                // Symbol. Deprecated.
                skip_read_len(&mut rdr)?;
                BsonValue::Placeholder(String::from("TODO: Symbol"))
            },
            0x0F => {
                // Code w/ scope
                skip_read_len(&mut rdr)?;
                BsonValue::Placeholder(String::from("TODO: Code with scope"))
            },
            0x10 => {
                // Int32
                BsonValue::Int32(rdr.read_i32::<LittleEndian>()?)
            },
            0x11 => {
                // Timestamp
                skip_bytes(&mut rdr, 8)?;
                BsonValue::Placeholder(String::from("TODO: Timestamp"))
            },
            0x12 => {
                // Int64
                BsonValue::Int64(rdr.read_i64::<LittleEndian>()?)
            },
            0x13 => {
                // Decimal128
                skip_bytes(&mut rdr, 16)?;
                BsonValue::Placeholder(String::from("TODO: Decimal128"))
            },
            0xFF => {
                // Min key.
                BsonValue::Placeholder(String::from("TODO: Min key"))
            },
            0x7F => {
                // Min key.
                BsonValue::Placeholder(String::from("TODO: Max key"))
            },
            other => {
                return Err(Error::new(ErrorKind::Other, format!("unrecognized type: 0x{:02x}", other)));
            },
        };

        debug!("elem_value={:?}", elem_value);

        if let Some(want_elem) = want_this_key {
            debug!("want this because: {}", want_elem);
            doc.insert(want_elem.to_string(), elem_value);
        }
    }
    Ok(())
}

/// Parse the BSON document, collecting selected fields into a HashMap
pub fn decode_document(mut rdr: impl Read, selector: &FieldSelector) -> io::Result<BsonLiteDocument> {
    let mut doc = BsonLiteDocument::new();

    let _document_size = rdr.read_i32::<LittleEndian>()?;

    parse_document(&mut rdr, &selector, "", 0, &mut doc)?;

    Ok(doc)
}

fn skip_bytes<T: Read>(rdr: &mut T, skip_bytes: usize) -> io::Result<()> {
    // TODO: Seek instead of looping
    for byte in rdr.take(skip_bytes as u64).bytes() {
        if let Err(e) = byte {
            return Err(e);
        }
    }
    Ok(())
}

fn skip_read_len<T: Read>(rdr: &mut T) -> io::Result<()> {
    let str_len = rdr.read_i32::<LittleEndian>()?;
    skip_bytes(rdr, str_len as usize)
}

fn read_cstring(rdr: impl Read) -> io::Result<String> {
    let mut bytes = Vec::new();
    for byte in rdr.bytes() {
        match byte {
            Ok(b) if b == 0 => break,
            Ok(b) => bytes.push(b),
            Err(e) => return Err(e),
        }
    }

    if let Ok(res) = String::from_utf8(bytes) {
        return Ok(res)
    }

    Err(Error::new(ErrorKind::Other, "conversion error"))
}

fn read_string_with_len(rdr: impl Read, str_len: usize) -> io::Result<String> {
    assert!(str_len > 0);

    let mut buf = Vec::with_capacity(str_len);
    rdr.take(str_len as u64).read_to_end(&mut buf)?;

    // Remove the trailing null, we won't need it
    buf.remove((str_len - 1) as usize);

    if let Ok(res) = String::from_utf8(buf) {
        return Ok(res);
    }

    Err(Error::new(ErrorKind::Other, "conversion error"))
}

#[cfg(test)]

mod tests {
    use super::*;
    use bson::{Array,Bson,oid};

    #[test]
    fn test_parse() {
        let mut doc = bson::Document::new();

        doc.insert("kala".to_owned(), bson::Bson::String("maja".to_owned()));
        doc.insert("puu".to_owned(), bson::Bson::FloatingPoint(3.14));

        let mut nested = bson::Document::new();
        nested.insert("ahv", bson::Bson::String("Rsk!".to_owned()));
        doc.insert("nested", nested);

        doc.insert("bool".to_owned(), bson::Bson::Boolean(true));
        doc.insert("eee".to_owned(), bson::Bson::FloatingPoint(2.7));
        println!("original: {:?}", doc);

        let mut buf = Vec::new();
        bson::encode_document(&mut buf, &doc).unwrap();

        let selector = FieldSelector::build()
            .with("first", "/@1")
            .with("first_elem_name", "/#1")
            .with("e", "/eee")
            .with("b", "/bool")
            .with("puu", "/nested/ahv");
        println!("matching fields: {:?}", selector);
        let doc = decode_document(&buf[..], &selector).unwrap();

        assert_eq!(5, doc.len());
        assert_eq!("kala", doc.get_str("first_elem_name").unwrap());
        assert_eq!("maja", doc.get_str("first").unwrap());
        assert_eq!(2.7, doc.get_float("e").unwrap());

        assert_eq!("Rsk!", doc.get_str("puu").unwrap());
    }

    #[test]
    fn test_array() {
        let mut doc = bson::Document::new();

        doc.insert("first".to_string(), Bson::String("foo".to_string()));

        let mut arr = Array::new();
        arr.push(Bson::String("blah".to_string()));
        arr.push(Bson::ObjectId(oid::ObjectId::with_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])));

        doc.insert("array".to_string(), Bson::Array(arr));
        doc.insert("last".to_owned(), bson::Bson::FloatingPoint(2.7));

        let mut buf = Vec::new();
        bson::encode_document(&mut buf, &doc).unwrap();
        println!("original: {:?}", &doc);

        let selector = FieldSelector::build()
            .with("first", "/@1")
            .with("array_len", "/array/[]")
            .with("last", "/last");
        println!("matching fields: {:?}", selector);

        let doc = decode_document(&buf[..], &selector).unwrap();

        assert_eq!("foo", doc.get_str("first").unwrap());
        assert_eq!(2, doc.get_i32("array_len").unwrap());
        assert_eq!(2.7, doc.get_float("last").unwrap());
    }
}
