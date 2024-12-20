use std::rc::Rc;

use crate::bit_vec::BitVec;

struct Span {
    start: usize,
    len: usize,
}

#[derive(Clone)]
pub struct Partition {
    rc: Rc<InnerPartition>,
}

impl core::fmt::Debug for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<Partition ");
        self.rc.len().fmt(f);
        f.write_str(">")
    }
}

impl Partition {
    pub fn new_single_partition(len: usize) -> Self {
        Partition {
            rc: Rc::new(InnerPartition {
                spans: vec![Span { start: 0, len }],
                row_indexes: (0..len).collect::<Vec<usize>>(),
            }),
        }
    }

    pub fn len(&self) -> usize {
        self.rc.len()
    }

    pub fn filter_indexes(&self, row_indexes: &[usize]) -> Partition {
        Partition {
            rc: Rc::new(self.rc.filter_indexes(row_indexes)),
        }
    }
}

struct InnerPartition {
    spans: Vec<Span>,
    row_indexes: Vec<usize>,
}

impl InnerPartition {
    fn len(&self) -> usize {
        self.row_indexes.len()
    }

    fn filter_indexes(&self, row_indexes: &[usize]) -> InnerPartition {
        let mut valid_indexes = BitVec::from_repeated_value(false, self.len());
        for idx in row_indexes {
            valid_indexes.set(*idx, true);
        }
        let mut new_spans = Vec::<Span>::new();
        let mut new_row_indexes = Vec::<usize>::with_capacity(row_indexes.len());
        for span in &self.spans {
            let new_start = new_row_indexes.len();
            let mut count: usize = 0;
            for idx in span.start..(span.start + span.len) {
                if valid_indexes.at(idx) {
                    new_row_indexes.push(idx);
                    count += 1;
                }
            }

            if count > 0 {
                new_spans.push(Span {
                    start: new_start,
                    len: count,
                });
            }
        }

        assert_eq!(new_row_indexes.len(), row_indexes.len());

        InnerPartition {
            spans: new_spans,
            row_indexes: new_row_indexes,
        }
    }
}
