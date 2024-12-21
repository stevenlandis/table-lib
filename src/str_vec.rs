use std::fmt::{Formatter, Write};

struct Span {
    start: usize,
    len: usize,
}

pub struct StringVec {
    data: String,
    spans: Vec<Span>,
}

impl std::fmt::Debug for StringVec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("<StringVec [")?;
        for (idx, str) in self.iter().enumerate() {
            if idx > 0 {
                f.write_str(", ")?;
            }
            str.fmt(f)?;
        }
        f.write_str("]>")?;
        Ok(())
    }
}

impl std::ops::Index<usize> for StringVec {
    type Output = str;

    fn index(&self, index: usize) -> &Self::Output {
        let span = &self.spans[index];
        &self.data[span.start..span.start + span.len]
    }
}

impl<'a> IntoIterator for &'a StringVec {
    type Item = &'a str;

    type IntoIter = StringVecIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        StringVecIterator { idx: 0, vec: self }
    }
}

pub struct StringVecIterator<'a> {
    vec: &'a StringVec,
    idx: usize,
}

impl<'a> Iterator for StringVecIterator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.vec.len() {
            return None;
        }
        let result = &self.vec[self.idx];
        self.idx += 1;
        Some(result)
    }
}

impl StringVec {
    pub fn new() -> Self {
        StringVec {
            data: String::new(),
            spans: Vec::new(),
        }
    }

    pub fn push(&mut self, value: &str) {
        let span = Span {
            start: self.data.len(),
            len: value.len(),
        };

        self.spans.push(span);
        self.data.push_str(value);
    }

    pub fn len(&self) -> usize {
        self.spans.len()
    }

    pub fn iter<'a>(&'a self) -> StringVecIterator<'a> {
        StringVecIterator { vec: self, idx: 0 }
    }

    pub fn get_writer<'a>(&'a mut self) -> ValueWriter<'a> {
        ValueWriter {
            str_vec: self,
            has_unwritten: false,
        }
    }
}

pub struct ValueWriter<'a> {
    str_vec: &'a mut StringVec,
    has_unwritten: bool,
}

impl<'a> std::io::Write for ValueWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let str = std::str::from_utf8(buf).unwrap();
        self.write_str(str);

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> std::fmt::Write for ValueWriter<'a> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.write_str(s);
        Ok(())
    }
}

impl<'a> Drop for ValueWriter<'a> {
    fn drop(&mut self) {
        self.finish_value();
    }
}

impl<'a> ValueWriter<'a> {
    pub fn finish_value(&mut self) {
        if self.has_unwritten {
            self.has_unwritten = false;

            let start = match self.str_vec.spans.last() {
                None => 0,
                Some(last) => last.start + last.len,
            };
            let len = self.str_vec.data.len() - start;
            self.str_vec.spans.push(Span { start, len });
        }
    }

    pub fn write_str(&mut self, val: &str) {
        self.has_unwritten = true;
        self.str_vec.data.write_str(val).unwrap();
    }
}
