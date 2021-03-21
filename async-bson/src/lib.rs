//! An asynchronous BSON parser that only parses explicitly specified subset of fields.
//! Useful for extracting a handful of fields from a larger document.
//!
//! It works by having the caller initialize a `DocumentParser`, specifying the fields
//! to be extracted. Then calling `parse_document` with a stream the parser goes through the input,
//! extracting the specified elements and ignoring the rest. The input can be an async stream
//! or a buffer. In case of an async stream the parser simply yields the task when no more input
//! is available. If a buffer is passed, it expects the whole message to be there.
//!
//! The emphasis on **asynchronous** -- this is a streaming parser that does not require the whole
//! BSON to be loaded into memory. As such it uses less memory than a conventional parser, but
//! does use extra CPU for pulling the bytes from a stream.
//!
//! The parser can be also used in synchronous mode by specifying `is_sync` feature. This is
//! useful in when we want to have a really fast parser and don't care about the memory overhead
//! of buffering the whole message. This is also useful in cases when don't want (or can't) use
//! an async runtime (i.e. in a WASM sandbox).
//!
//! ```text
//! [dependencies]
//! async-bson = { version = "0.2", features = ["is_sync"] }
//! ```
//!
//! The default is to use the asynchronous parser.
//!
//! # Example:
//!
//! ```
//! use async_bson::{DocumentParser, Document};
//!
//! #[tokio::main]
//!
//! async fn main() {
//!     // This is our BSON "stream"
//!     let buf = b"\x16\x00\x00\x00\x02hello\x00\x06\x00\x00\x00world\x00\x00";
//!
//!     // Parse the value of /hello, storing the value under "foo"
//!     let parser = DocumentParser::builder().match_exact("/hello", "foo");
//!     let doc = parser.parse_document(&buf[..]).await.unwrap();
//!
//!     assert_eq!("world", doc.get_str("foo").unwrap());
//! }
//! ```
//!

use std::fmt;
use std::io::{Error, ErrorKind};
use std::io::{Cursor, Result};
use std::collections::{HashMap, HashSet};

#[cfg(not(feature="is_sync"))]
use {
    tokio::io::{AsyncReadExt, AsyncBufReadExt},
    std::future::Future,
    std::pin::Pin,
};

#[cfg(not(feature="is_sync"))]
type ParserResult<'a> = Pin<Box<dyn Future<Output = Result<()>> + 'a + Send>>;
#[cfg(not(feature="is_sync"))]
fn pin_maybe<T>(v: T) -> Pin<Box<T>> { Box::pin(v) }

#[cfg(feature="is_sync")]
type ParserResult<'a> = Result<()>;
#[cfg(feature="is_sync")]
fn pin_maybe<T>(v: T) -> T { v }

#[cfg(not(feature="is_sync"))]
pub trait DocumentReader: AsyncReadExt+AsyncBufReadExt+Unpin+Send {}
#[cfg(not(feature="is_sync"))]
impl <T>DocumentReader for T where T: AsyncReadExt+AsyncBufReadExt+Unpin+Send {}

#[cfg(feature="is_sync")]
use byteorder::{LittleEndian, ReadBytesExt};

#[cfg(feature="is_sync")]
pub trait DocumentReader: std::io::BufRead {
    fn read_i32_le(&mut self) -> Result<i32> {
        self.read_i32::<LittleEndian>()
    }

    fn read_u32_le(&mut self) -> Result<u32> {
        self.read_u32::<LittleEndian>()
    }

    fn read_i64_le(&mut self) -> Result<i64> {
        self.read_i64::<LittleEndian>()
    }

    fn read_u64_le(&mut self) -> Result<u64> {
        self.read_u64::<LittleEndian>()
    }
}

#[cfg(feature="is_sync")]
impl <T>DocumentReader for T where T: std::io::BufRead {}

/// Async parser that extracts BSON fields into a Document.
///
/// The fields to be extracted are specified by giving it a name and a pattern to match.
/// During parsing when a BSON element matches any of the patterns, it's value is stored in the
/// resulting Document. If there are multiple patterns for the same field name, the last match
/// is used as the result.
///
/// In addition to element values, also their names and length (for arrays) can be extracted.
///
/// # Example:
/// ```
/// use async_bson::{DocumentParser};
///
/// let parser = DocumentParser::builder()
///     .match_exact("/foo", "foo")
///     .match_value_at("/foo", 1, "first_of_foo")
///     .match_name_at("/foo", 1, "first_element_name")
///     .match_array_len("/foo/items", "items_len");
///
/// ```


#[derive(Debug)]
struct Matcher {
    match_exact:        Option<String>,
    match_name_at_pos:  Option<(String, u32)>,
    match_value_at_pos: Option<(String, u32)>,
    match_array_len:    Option<String>,
}

impl Matcher {
    pub fn new() -> Self {
        Matcher {
            match_exact: None,
            match_name_at_pos: None,
            match_value_at_pos: None,
            match_array_len: None,
        }
    }
}

/// Parse a BSON document from an async reader.
///
/// The parser is initialized with a set of matching patterns that specify which elements to
/// extract from the stream. During parsing it matches those patterns against the BSON stream and
/// collects the matching elements.
///
/// The matching patterns consists of a prefix, an optional position and a label. The prefix
/// identifies the location of the element in the BSON with the forward slash character `/`
/// denoting hierarchy.
///
/// ```text
/// {
///     "name": "Data",
///     "pets": [
///         { "name": "Spot", "type": "cat" }
///     ],
/// }
/// ```
///
/// * The prefix `/name` would match the `name` element in the document root and yield a value of
/// "Data".
/// * `/pets/0/name` would match the first element in the `pets` array and yield a value of
/// `Spot`.
/// * We could use the prefix `/pets/0` and position `2` to signify that we want the
/// `type` field of the first element of the `pets` array.
/// * We can also extract the length of the array by prefix `/pets`.
///
/// The collected values are stored in a flat key/value structure using the label given to the
/// matching patterns. If multiple patterns have the same label, the last parsed value will be kept.
///
/// Only primitive types can be collect (strings and numbers).
///
#[derive(Debug)]
pub struct DocumentParser<'a> {
    // Matching rules for the parser. These consist of a prefix and a set of "matchers"
    // for that prefix. These are going to be looked up a lot, so we keep them sorted
    // for binary search.
    prefix_matchers: Vec<(&'a str, Matcher)>,

    // Map of subdocument prefixes that we are interested in. We're using this to skip
    // documents that don't contain anything interesting.
    match_prefixes: HashSet<&'a str>,

    // Do we want a copy of the document bytes?
    keep_bytes: bool,

    // Do we sink the leftover bytes from partial parse?
    sink_bytes: bool,
}

impl<'a> DocumentParser<'a> {

    /// Create a new parser. It doesn't have any fields specified, so it doesn't match anything yet.
    /// Use the match* functions to build up the parser definition.
    pub fn builder() -> Self {
        DocumentParser {
            prefix_matchers: Vec::new(),
            match_prefixes: HashSet::new(),
            keep_bytes: false,
            sink_bytes: true,
        }
    }

    /// Matches the element by name and extracts its value.
    ///
    /// Example: Match element `foo.name` and store it's value under "name"
    /// ```
    /// use async_bson::{DocumentParser};
    ///
    /// let parser = DocumentParser::builder().match_exact("/foo/name", "name");
    /// ```
    pub fn match_exact(mut self, prefix: &'a str, label: &'a str) -> Self {
        let mut matcher = self.matcher_entry(prefix);
        matcher.match_exact = Some(label.to_string());
        self
    }

    /// Matches nth element name after the prefix and extracts the name.
    ///
    /// Example: Match the first element in foo and store it's **name** under "x"
    /// ```
    /// use async_bson::{DocumentParser};
    ///
    /// let parser = DocumentParser::builder().match_name_at("/foo", 1, "x");
    /// ```
    pub fn match_name_at(mut self, prefix: &'a str, pos: u32, label: &'a str) -> Self {
        let mut matcher = self.matcher_entry(prefix);
        matcher.match_name_at_pos = Some((label.to_string(), pos));
        self
    }

    /// Matches nth element value after the prefix and extracts the value.
    ///
    /// Example: Match the first element in foo and store it's **value** under "x"
    /// ```
    /// use async_bson::{DocumentParser};
    ///
    /// let parser = DocumentParser::builder().match_value_at("/foo", 1, "x");
    /// ```
    pub fn match_value_at(mut self, prefix: &'a str, pos: u32, label: &'a str) -> Self {
        let mut matcher = self.matcher_entry(prefix);
        matcher.match_value_at_pos = Some((label.to_string(), pos));
        self
    }

    /// Matches the named array and extracts its length.
    ///
    /// Example: Match the array `foo.pets` and store it's *length* under "num_pets"
    /// ```
    /// use async_bson::{DocumentParser};
    ///
    /// let parser = DocumentParser::builder().match_array_len("/foo/pets", "num_pets");
    /// ```
    pub fn match_array_len(mut self, prefix: &'a str, label: &'a str) -> Self {
        let mut matcher = self.matcher_entry(prefix);
        matcher.match_array_len = Some(label.to_string());
        self
    }

    /// Set this to grab a copy of the document bytes or not.
    /// The implication of setting this `true` is that we're going to read the
    /// bytes into a buffer and then parse. Default is `false`.
    pub fn keep_bytes(mut self, keep: bool) -> Self {
        self.keep_bytes = keep;
        self
    }

    /// Don't sink the left over bytes. Mostly useful for debugging.
    pub fn no_sink(mut self) -> Self {
        self.sink_bytes = false;
        self
    }

    /// Collect a new document from byte stream.
    /// Only the elements specified with matching patterns are collected, the
    /// rest is simply discarded.
    #[maybe_async::maybe_async]
    pub async fn parse_document<'b, R: DocumentReader>(
        &self,
        rdr: R,
    ) -> Result<Document> {
        self.parse_document_keep_bytes(rdr, self.keep_bytes).await
    }

    /// Collect a new document from a byte stream, with additional options.
    #[maybe_async::maybe_async]
    pub async fn parse_document_keep_bytes<'b, R: DocumentReader>(
        &self,
        mut rdr: R,
        keep_bytes: bool,
    ) -> Result<Document> {
        let mut doc = Document::new();
        let starting_prefix = "";
        let starting_matcher = self.get_matcher(starting_prefix);

        let document_size = rdr.read_i32_le().await?;

        if keep_bytes || self.keep_bytes {
            let length_bytes = document_size.to_le_bytes();
            let mut buf = vec![0u8; document_size as usize];

            // Put the length back so that the caller has the whole BSON
            buf[..length_bytes.len()].copy_from_slice(&length_bytes);

            rdr.read_exact(&mut buf[4..]).await?;

            // Use a Cursor to detect partial parses
            let mut cur = Cursor::new(&buf[..]);
            cur.set_position(4);
            self.parse_internal(&mut cur, starting_prefix, 0, starting_matcher, &mut doc).await?;

            let remaining_bytes = document_size as u64 - cur.position();
            if remaining_bytes > 0 {
                doc.is_partial = true;
            }

            doc.raw_bytes = Some(buf);
        } else {
            self.parse_internal(&mut rdr, starting_prefix, 0, starting_matcher, &mut doc).await?;
        }

        Ok(doc)
    }

    fn get_matcher(&self, prefix: &'a str) -> Option<&Matcher> {
        if let Ok(pos) = self.prefix_matchers.binary_search_by(|x| x.0.cmp(prefix)) {
            Some(&self.prefix_matchers[pos].1)
        } else {
            None
        }
    }

    /// Find the matcher and return a mutable reference to it. Create it if it doesn't exist.
    fn matcher_entry(&mut self, prefix: &'a str) -> &mut Matcher {
        // Strip the extra / here, so that we don't have to do it later
        // during parsing.
        let prefix = if prefix == "/" { "" } else { prefix };

        if let Some(pos) = self.prefix_matchers.iter().position(|x| x.0 == prefix) {
            return &mut self.prefix_matchers[pos].1
        }

        self.prefix_matchers.push((&prefix, Matcher::new()));

        // Sort the matchers so that we don't have to mutate self in parser
        // Assuming that this won't get called too often.
        self.prefix_matchers.sort_by(|a, b| a.0.cmp(b.0));

        // Make a note of all the prefixes leading up to the exact value. So that
        // encountering /foo/bar/baz we insert /foo/bar/baz, /foo/bar and /foo
        let mut work_prefix = prefix;
        while let Some(pos) = work_prefix.rfind('/') {
            work_prefix = &work_prefix[..pos];
            if !work_prefix.is_empty() {
                self.match_prefixes.insert(work_prefix);
            }
        }

        // Find again, because we lost the position after sort
        let pos = self.prefix_matchers.iter().position(|x| x.0 == prefix).unwrap();
        &mut self.prefix_matchers[pos].1
    }

    fn want_prefix(&self, prefix: &str) -> bool {
        self.match_prefixes.contains(prefix)
    }

    #[maybe_async::maybe_async]
    fn parse_internal<'x, R: DocumentReader + 'x>(
        &'x self,
        mut rdr: &'x mut R,
        prefix: &'x str,
        position: u32,
        prefix_matcher: Option<&'x Matcher>,
        mut doc: &'x mut Document,
    ) -> ParserResult<'x>
    {
        pin_maybe(async move {
            let mut position = position;

            loop {
                position += 1;

                let elem_type = rdr.read_u8().await?;

                if elem_type == 0x00 {
                    break;
                }

                let elem_name = read_cstring(&mut rdr).await?;
                let prefix_name = format!("{}/{}", prefix, elem_name);

                // We have 2 matchers - one that matches elements by prefix and position
                // and another that matches the exact element name. Note: that when we
                // recurse the exact matcher becomes the prefix matcher, thus we just
                // pass it along to avoid a lookup.
                let exact_matcher = self.get_matcher(&prefix_name);

                let mut want_this_value = false;

                // Match for array length and element name. This will not use the matcher
                // for the current element but instead need to use the matcher for its
                // parent.
                if let Some(matcher) = prefix_matcher {
                    if let Some(ref label) = matcher.match_array_len {
                        doc.insert(label.clone(), BsonValue::Int32(position as i32));
                    }

                    if let Some((ref label, pos)) = matcher.match_name_at_pos {
                        if pos == position {
                            doc.insert(label.clone(), BsonValue::String(elem_name.to_string()));
                        }
                    }

                    if matcher.match_value_at_pos.is_some() {
                        // Yes we want the value, by position
                        want_this_value = true;
                    }
                }

                if let Some(matcher) = exact_matcher {
                    // Yes, we want the value
                    want_this_value = want_this_value
                        || matcher.match_exact.is_some() || matcher.match_array_len.is_some();
                }

                let elem_value = match elem_type {
                    0x01 => {
                        // A float
                        let mut buf = [0 as u8; 8];
                        rdr.read_exact(&mut buf).await?;
                        BsonValue::Float(f64::from_le_bytes(buf))
                    }
                    0x02 => {
                        // String
                        let str_len = rdr.read_i32_le().await?;
                        if want_this_value {
                            BsonValue::String(read_string_with_len(&mut rdr, str_len as usize).await?)
                        } else {
                            skip_bytes(&mut rdr, str_len as usize).await?;
                            BsonValue::None
                        }
                    }
                    0x03 | 0x04 => {
                        // Embedded document or an array. Both are represented as a document.
                        // We only go through the trouble of parsing this if the field selector
                        // wants the document value or some element within it.
                        let doc_len = rdr.read_i32_le().await?;

                        if want_this_value || self.want_prefix(&prefix_name) {
                            self.parse_internal(rdr, &prefix_name, 0, exact_matcher, &mut doc).await?;
                            BsonValue::Placeholder("<nested document>")
                        } else {
                            skip_bytes(&mut rdr, doc_len as usize - 4).await?;
                            BsonValue::None
                        }
                    }
                    0x05 => {
                        // Binary data
                        let len = rdr.read_i32_le().await?;
                        skip_bytes(&mut rdr, (len + 1) as usize).await?;
                        BsonValue::Placeholder("<binary data>")
                    }
                    0x06 => {
                        // Undefined value. Deprecated.
                        BsonValue::None
                    }
                    0x07 => {
                        let mut bytes = [0 as u8; 12];
                        rdr.read_exact(&mut bytes).await?;
                        BsonValue::ObjectId(bytes)
                    }
                    0x08 => {
                        // Boolean
                        let val = match rdr.read_u8().await? {
                            0x00 => false,
                            _ => true,
                        };
                        BsonValue::Boolean(val)
                    }
                    0x09 => {
                        // UTC Datetime
                        skip_bytes(&mut rdr, 8).await?;
                        BsonValue::Placeholder("<UTC datetime>")
                    }
                    0x0A => {
                        // Null value
                        BsonValue::None
                    }
                    0x0B => {
                        // Regular expression
                        let _regx = read_cstring(&mut rdr).await?;
                        let _opts = read_cstring(&mut rdr).await?;
                        BsonValue::Placeholder("<regex>")
                    }
                    0x0C => {
                        // DBPointer. Deprecated.
                        let len = rdr.read_i32_le().await?;
                        skip_bytes(&mut rdr, (len + 12) as usize).await?;
                        BsonValue::None
                    }
                    0x0D => {
                        // Javascript code
                        skip_read_len(&mut rdr).await?;
                        BsonValue::Placeholder("<Javascript>")
                    }
                    0x0E => {
                        // Symbol. Deprecated.
                        skip_read_len(&mut rdr).await?;
                        BsonValue::Placeholder("<symbol>")
                    }
                    0x0F => {
                        // Code w/ scope
                        skip_read_len(&mut rdr).await?;
                        BsonValue::Placeholder("<Javascript with scope>")
                    }
                    0x10 => {
                        // Int32
                        BsonValue::Int32(rdr.read_i32_le().await?)
                    }
                    0x11 => {
                        // Timestamp
                        skip_bytes(&mut rdr, 8).await?;
                        BsonValue::Placeholder("<timestamp>")
                    }
                    0x12 => {
                        // Int64
                        BsonValue::Int64(rdr.read_i64_le().await?)
                    }
                    0x13 => {
                        // Decimal128
                        skip_bytes(&mut rdr, 16).await?;
                        BsonValue::Placeholder("<decimal128>")
                    }
                    0xFF => {
                        // Min key.
                        BsonValue::Placeholder("<min key>")
                    }
                    0x7F => {
                        // Min key.
                        BsonValue::Placeholder("<max key>")
                    }
                    other => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!("BSON: unrecognized type: 0x{:02x}", other),
                        ));
                    }
                };

                if let Some(matcher) = prefix_matcher {
                    if let Some((ref label, pos)) = matcher.match_value_at_pos {
                        if pos == position {
                            doc.insert(label.clone(), elem_value.clone());
                        }
                    }
                }

                if let Some(matcher) = exact_matcher {
                    if let Some(ref label) = matcher.match_exact {
                        doc.insert(label.clone(), elem_value);
                    }
                }
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone)]
enum BsonValue {
    Float(f64),
    String(String),
    Int32(i32),
    Int64(i64),
    ObjectId([u8; 12]),
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
            other => write!(f, "Other({:?})", other),
        }
    }
}

/// A flat key-value structure, representing the parsed BSON.
///
/// Getter methods are provided for extracting the value for named fields. The getters only return
/// a value if the field is present and is of the requested type.
#[derive(Debug)]
pub struct Document {
    doc: HashMap<String, BsonValue>,
    raw_bytes: Option<Vec<u8>>,
    is_partial: bool
}

impl fmt::Display for Document {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{ ")?;
        for (i, (k, v)) in self.doc.iter().enumerate() {
            let comma = if i == self.doc.len() - 1 { "" } else { "," };
            write!(f, "{}: {}{} ", k, v, comma)?;
        }
        write!(f, "}}")
    }
}

impl Document {
    // Creates an empty Document.
    fn new() -> Self {
        Document {
            doc: HashMap::new(),
            raw_bytes: None,
            is_partial: false,
        }
    }

    pub fn is_partial(&self) -> bool {
        self.is_partial
    }

    /// Return the str value for this key.
    pub fn get_str(&self, key: &str) -> Option<&str> {
        match self.doc.get(key) {
            Some(BsonValue::String(result)) => Some(result),
            _ => None,
        }
    }

    /// Returns the float value for this key.
    pub fn get_float(&self, key: &str) -> Option<f64> {
        match self.doc.get(key) {
            Some(BsonValue::Float(result)) => Some(*result),
            _ => None,
        }
    }

    /// Returns the i32 value for this key.
    pub fn get_i32(&self, key: &str) -> Option<i32> {
        match self.doc.get(key) {
            Some(BsonValue::Int32(result)) => Some(*result),
            _ => None,
        }
    }

    /// Returns the i64 value for this key.
    pub fn get_i64(&self, key: &str) -> Option<i64> {
        match self.doc.get(key) {
            Some(BsonValue::Int64(result)) => Some(*result),
            _ => None,
        }
    }

    /// Returns true if the document contains the key.
    ///
    /// It only checks if the key exists in the document, ignoring any type information.
    pub fn contains_key(&self, key: &str) -> bool {
        self.doc.contains_key(key)
    }

    /// Returns the number of keys in the document.
    pub fn len(&self) -> usize {
        self.doc.len()
    }

    /// Return the raw bytes of the Document.
    pub fn get_raw_bytes(&self) -> Option<&Vec<u8>> {
        if let Some(ref raw_bytes) = self.raw_bytes {
            Some(raw_bytes)
        } else {
            None
        }
    }

    /// Returns true if the document has no keys
    pub fn is_empty(&self) -> bool {
        self.doc.len() == 0
    }

    fn insert(&mut self, key: String, value: BsonValue) {
        self.doc.insert(key, value);
    }
}

#[maybe_async::maybe_async]
async fn skip_bytes<T: DocumentReader>(rdr: &mut T, bytes_to_skip: usize) -> Result<usize> {
    let mut buf = vec![0u8; bytes_to_skip];
    let bytes_read = buf.len();

    rdr.read_exact(&mut buf).await?;
    Ok(bytes_read)
}

#[maybe_async::maybe_async]
async fn skip_read_len<T: DocumentReader>(rdr: &mut T) -> Result<usize> {
    let str_len = rdr.read_i32_le().await?;
    skip_bytes(rdr, str_len as usize).await
}

/// Read a null terminated string from async stream.
#[maybe_async::maybe_async]
pub async fn read_cstring<R: DocumentReader>(rdr: &mut R) -> Result<String> {
    let mut bytes = Vec::new();

    rdr.read_until(0, &mut bytes).await?;
    let _ = bytes.pop();    // Drop the trailing zero

    if let Ok(res) = String::from_utf8(bytes) {
        return Ok(res);
    }

    Err(Error::new(ErrorKind::Other, "cstring conversion error"))
}

#[maybe_async::maybe_async]
async fn read_string_with_len<R: DocumentReader>(rdr: &mut R, str_len: usize) -> Result<String> {
    let mut buf = vec![0u8; str_len];
    rdr.read_exact(&mut buf).await?;

    // Remove the trailing null, we won't need it
    let _ = buf.pop();

    if let Ok(res) = String::from_utf8(buf) {
        return Ok(res);
    }

    Err(Error::new(ErrorKind::Other, "string conversion error"))
}

#[cfg(test)]

mod tests {
    use super::*;
    use bson::doc;

    #[maybe_async::test(feature="is_sync", async(not(feature="is_sync"), tokio::test))]
    async fn test_parse_bson() {

        let doc = doc! {
            "first": "foo",
            "a_string": "bar",
            "an_f64": 3.14,
            "an_i32": 123i32,
            "an_i64": 12345678910i64,
            "oid": bson::oid::ObjectId::with_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
            "bool": true,
            "nested": {
                "monkey": {
                    "name": "nilsson",
                },
            },
            "deeply": {
                "nested": {
                    "array": [1, 2, 3],
                },
            },
        };

        let mut buf = Vec::new();
        doc.to_writer(&mut buf).unwrap();

        let parser = DocumentParser::builder()
            .match_name_at("/", 1, "first_elem_name")
            .match_value_at("/", 1, "first_elem_value")
            .match_exact("/a_string", "string")
            .match_exact("/an_f64", "f64")
            .match_exact("/an_i32", "i32")
            .match_exact("/an_i64", "i64")
            .match_array_len("/deeply/nested/array", "array_len")
            .match_value_at("/deeply/nested/array", 1, "array_first")
            .match_exact("/nested/monkey/name", "monkey");

        let doc = parser.parse_document(&buf[..]).await.unwrap();

        assert_eq!("first", doc.get_str("first_elem_name").unwrap());
        assert_eq!("foo", doc.get_str("first_elem_value").unwrap());
        assert_eq!("bar", doc.get_str("string").unwrap());
        assert_eq!(3.14, doc.get_float("f64").unwrap());
        assert_eq!(123, doc.get_i32("i32").unwrap());
        assert_eq!(12345678910i64, doc.get_i64("i64").unwrap());
        assert_eq!(3, doc.get_i32("array_len").unwrap());
        assert_eq!(1, doc.get_i32("array_first").unwrap());
        assert_eq!("nilsson", doc.get_str("monkey").unwrap());
        assert_eq!(9, doc.len());
    }

    #[maybe_async::test(feature="is_sync", async(not(feature="is_sync"), tokio::test))]
    async fn test_multiple_docs() {
        let mut buf = Vec::new();

        let doc = doc! {
            "foo": 1,
        };
        doc.to_writer(&mut buf).unwrap();
        let doc = doc! {
            "bar": 2,
        };
        doc.to_writer(&mut buf).unwrap();

        let parser = DocumentParser::builder()
            .match_exact("/foo", "foo")
            .match_exact("/bar", "bar");

        for keep_bytes in vec![true, false] {
            let mut cursor = Cursor::new(&buf[..]);

            let doc = parser.parse_document_keep_bytes(&mut cursor, keep_bytes).await.unwrap();
            assert_eq!(1, doc.get_i32("foo").unwrap());

            let doc = parser.parse_document_keep_bytes(&mut cursor, keep_bytes).await.unwrap();
            assert_eq!(2, doc.get_i32("bar").unwrap());

            assert_eq!(buf.len(), cursor.position() as usize);
        }
    }

    #[maybe_async::test(feature="is_sync", async(not(feature="is_sync"), tokio::test))]
    async fn test_nested_array() {
        let doc = doc! {
            "f": doc! {
                "array": [
                    doc! { "foo": 42 },
                    doc! { "bar": 43 },
                    doc! { "baz": 44 },
                ],
            },
        };

        let mut buf = Vec::new();
        doc.to_writer(&mut buf).unwrap();

        let parser = DocumentParser::builder()
            .match_array_len("/f/array", "a")
            .match_exact("/f/array/0/foo", "b")
            .match_exact("/f/array/2/baz", "c");

        let doc = parser.parse_document(&buf[..]).await.unwrap();

        assert_eq!(3, doc.get_i32("a").unwrap());
        assert_eq!(42, doc.get_i32("b").unwrap());
        assert_eq!(44, doc.get_i32("c").unwrap());
    }

    #[maybe_async::test(feature="is_sync", async(not(feature="is_sync"), tokio::test))]
    async fn test_keep_bytes() {
        let buf = b"\x16\x00\x00\x00\x02hello\x00\x06\x00\x00\x00world\x00\x00";

        let parser = DocumentParser::builder()
            .match_exact("/hello", "foo")
            .keep_bytes(true);

        let doc = parser.parse_document(&buf[..]).await.unwrap();

        assert_eq!(buf, doc.get_raw_bytes().unwrap().as_slice());
    }

    // TODO: Add test cases for skipping unwanted nested elements
    // TODO: Add test cases that required elements are not skipped

    // This is an expensive benchmark, ignore this by default
    // Run with: time cargo test -- --ignored
    #[maybe_async::test(feature="is_sync", async(not(feature="is_sync"), tokio::test))]
    #[ignore]
    async fn benchmark_parser() {
        const NUM_ITERATIONS: i32 = 100_000;

        let doc = doc! {
            "f": doc! {
                "array": [
                    doc! { "foo": "x".repeat(1000) },
                    doc! { "foo": "x".repeat(1000) },
                    doc! { "foo": "x".repeat(1000) },
                    doc! { "foo": "x".repeat(1000) },
                    doc! { "foo": "x".repeat(1000) },
                    doc! { "foo": "x".repeat(1000) },
                ],
            },
        };

        let parser = DocumentParser::builder()
            .match_exact("/f/array/[]", "a")
            .match_exact("/f/array/0/foo", "b")
            .match_exact("/f/array/2/foo", "c");

        let mut buf = Vec::new();
        doc.to_writer(&mut buf).unwrap();

        println!("Parsing a {} byte document {} times.", buf.len(), NUM_ITERATIONS);
        for _ in 1..NUM_ITERATIONS {
            let _ = parser.parse_document(&buf[..]).await.unwrap();
        }
    }

    #[maybe_async::test(feature="is_sync", async(not(feature="is_sync"), tokio::test))]
    async fn test_read_cstring() {
        let buf = b"kala\0";
        let res = read_cstring(&mut Cursor::new(&buf[..])).await.unwrap();
        assert_eq!(res, "kala");

        let buf = b"\0";
        let res = read_cstring(&mut Cursor::new(&buf[..])).await.unwrap();
        assert_eq!(res, "");
    }
}
