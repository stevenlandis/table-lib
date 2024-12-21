struct Span {
    start: usize,
    len: usize,
}

pub struct StringVec {
    data: String,
    spans: Vec<Span>,
}

impl std::fmt::Debug for StringVec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
        todo!()
    }
}

pub struct StringVecIterator<'a> {
    vec: &'a StringVec,
    idx: usize,
}

impl<'a> Iterator for StringVecIterator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
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
}
