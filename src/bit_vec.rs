#[derive(Clone)]
pub struct BitVec {
    values: Vec<u8>,
    length: usize,
}

impl core::fmt::Debug for BitVec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BitVec<")?;
        self.length.fmt(f)?;
        f.write_str(">")?;
        Ok(())
    }
}

impl BitVec {
    pub fn new() -> Self {
        BitVec {
            values: Vec::new(),
            length: 0,
        }
    }

    pub fn with_capacity(_size: usize) -> Self {
        Self::new()
    }

    pub fn from_repeated_value(val: bool, len: usize) -> Self {
        let mut result = BitVec::with_capacity(len);
        for _ in 0..len {
            result.push(val);
        }

        result
    }

    pub fn push(&mut self, value: bool) {
        let offset = self.length & 0b111;
        if offset == 0 {
            self.values.push(if value { 1 } else { 0 });
        } else {
            if value {
                let idx = self.length >> 3;
                self.values[idx] += 1 << offset;
            }
        }
        self.length += 1;
    }

    pub fn at(&self, idx: usize) -> bool {
        (self.values[idx >> 3] & (1 << (idx & 0b111))) > 0
    }

    pub fn len(&self) -> usize {
        return self.length;
    }

    pub fn iter(&self) -> BitVecIterator {
        BitVecIterator {
            bit_vec: self,
            index: 0,
        }
    }

    pub fn from_indexes(&self, indexes: &Vec<usize>) -> BitVec {
        indexes.iter().map(|idx| self.at(*idx)).collect::<BitVec>()
    }
}

impl<'a> IntoIterator for &'a BitVec {
    type Item = bool;

    type IntoIter = BitVecIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl FromIterator<bool> for BitVec {
    fn from_iter<T: IntoIterator<Item = bool>>(iter: T) -> Self {
        let mut result = BitVec::new();

        for elem in iter {
            result.push(elem);
        }

        result
    }
}

pub struct BitVecIterator<'a> {
    bit_vec: &'a BitVec,
    index: usize,
}

impl<'a> Iterator for BitVecIterator<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.bit_vec.len() {
            let result = self.bit_vec.at(self.index);
            self.index += 1;
            return Some(result);
        } else {
            return None;
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.bit_vec.len()))
    }
}

#[cfg(test)]
mod test_bit_vec {
    use super::*;

    #[test]
    fn test_basic() {
        let mut vec = BitVec::new();
        assert_eq!(vec.len(), 0);

        vec.push(true);

        assert_eq!(vec.len(), 1);
        assert_eq!(vec.at(0), true);

        vec.push(false);

        assert_eq!(vec.len(), 2);
        assert_eq!(vec.at(0), true);
        assert_eq!(vec.at(1), false);

        vec.push(true);

        assert_eq!(vec.len(), 3);
        assert_eq!(vec.at(0), true);
        assert_eq!(vec.at(1), false);
        assert_eq!(vec.at(2), true);

        assert_eq!(vec.iter().collect::<Vec<_>>(), vec![true, false, true]);

        vec.push(false);
        vec.push(false);
        vec.push(true);
        vec.push(true);
        vec.push(true);
        vec.push(true);
        vec.push(false);

        assert_eq!(
            vec.iter().collect::<Vec<_>>(),
            vec![true, false, true, false, false, true, true, true, true, false]
        );
    }
}
