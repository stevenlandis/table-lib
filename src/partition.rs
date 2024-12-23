use std::rc::Rc;

use crate::bit_vec::BitVec;

#[derive(Clone)]
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
        f.write_str("<Partition ")?;
        self.rc.len().fmt(f)?;

        for span in self {
            f.write_str(" (")?;
            for (idx, row_idx) in span.iter().cloned().enumerate() {
                if idx > 0 {
                    f.write_str(",")?;
                }
                row_idx.fmt(f)?;
            }
            f.write_str(")")?;
        }

        f.write_str(">")?;

        Ok(())
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

    /// Returns the reset row indexes of this [`Partition`].
    /// This just replaces row indexes with ascending integers.
    pub fn reset_row_indexes(&self) -> Partition {
        Partition {
            rc: Rc::new(InnerPartition {
                spans: self.rc.spans.clone(),
                row_indexes: (0..self.rc.row_indexes.len()).collect(),
            }),
        }
    }

    pub fn get_single_value_partition(&self) -> Partition {
        Partition {
            rc: Rc::new(self.rc.get_single_value_partition()),
        }
    }

    pub fn undo_group_by(
        original: &Partition,
        grouped: &Partition,
        aggregated: &Partition,
    ) -> Partition {
        /*
        This function is intended to do the reverse of group_by
        */

        let original = original.rc.as_ref();
        let grouped = grouped.rc.as_ref();
        let aggregated = aggregated.rc.as_ref();

        assert_eq!(grouped.len(), aggregated.len());

        let mut result = PartitionBuilder::new();
        let mut span_idx: usize = 0;
        for span in &original.spans {
            let target_row_count = span.len;
            let mut row_count: usize = 0;
            while row_count < target_row_count {
                let group_span = &grouped.spans[span_idx];
                row_count += group_span.len;

                let agg_span = &aggregated.spans[span_idx];
                for idx in &aggregated.row_indexes[agg_span.start..agg_span.start + agg_span.len] {
                    result.add_row_idx(*idx);
                }

                span_idx += 1;
            }
            assert_eq!(row_count, target_row_count);
            result.finish_span();
        }

        result.to_partition()
    }

    pub fn iter(&self) -> PartitionIter {
        self.into_iter()
    }
}

struct InnerPartition {
    spans: Vec<Span>,
    row_indexes: Vec<usize>,
}

impl InnerPartition {
    fn len(&self) -> usize {
        self.spans.len()
    }

    fn filter_indexes(&self, row_indexes: &[usize]) -> InnerPartition {
        let mut valid_indexes = BitVec::from_repeated_value(false, self.len());
        for idx in row_indexes {
            valid_indexes.set(*idx, true);
        }
        let mut new_spans = Vec::<Span>::with_capacity(self.spans.len());
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

            new_spans.push(Span {
                start: new_start,
                len: count,
            });
        }

        assert_eq!(new_spans.len(), self.spans.len());

        InnerPartition {
            spans: new_spans,
            row_indexes: new_row_indexes,
        }
    }

    fn get_single_value_partition(&self) -> InnerPartition {
        // Reduce ever span down to a single row
        InnerPartition {
            spans: (0..self.spans.len())
                .map(|idx| Span { start: idx, len: 1 })
                .collect(),
            row_indexes: (0..self.spans.len()).collect(),
        }
    }
}

pub struct PartitionIter<'a> {
    partition: &'a InnerPartition,
    idx: usize,
}

impl<'a> IntoIterator for &'a Partition {
    type Item = &'a [usize];

    type IntoIter = PartitionIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        PartitionIter {
            partition: &self.rc,
            idx: 0,
        }
    }
}

impl<'a> Iterator for PartitionIter<'a> {
    type Item = &'a [usize];

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.partition.spans.len() {
            None
        } else {
            let span = &self.partition.spans[self.idx];
            let result = &self.partition.row_indexes[span.start..span.start + span.len];
            self.idx += 1;
            Some(result)
        }
    }
}

pub struct PartitionBuilder {
    spans: Vec<Span>,
    row_indexes: Vec<usize>,
}

impl PartitionBuilder {
    pub fn new() -> Self {
        PartitionBuilder {
            spans: Vec::new(),
            row_indexes: Vec::new(),
        }
    }

    pub fn add_row_idx(&mut self, idx: usize) {
        self.row_indexes.push(idx);
    }

    pub fn finish_span(&mut self) {
        match self.spans.last() {
            None => {
                self.spans.push(Span {
                    start: 0,
                    len: self.row_indexes.len(),
                });
            }
            Some(prev_span) => {
                let prev_end = prev_span.start + prev_span.len;
                self.spans.push(Span {
                    start: prev_end,
                    len: self.row_indexes.len() - prev_end,
                });
            }
        }
    }

    pub fn to_partition(self) -> Partition {
        Partition {
            rc: Rc::new(InnerPartition {
                spans: self.spans,
                row_indexes: self.row_indexes,
            }),
        }
    }
}
