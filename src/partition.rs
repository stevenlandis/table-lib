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
            for (idx, row_idx) in span.enumerate() {
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
            }),
        }
    }

    pub fn n_spans(&self) -> usize {
        self.rc.len()
    }

    pub fn n_rows(&self) -> usize {
        self.rc.spans.iter().fold(0, |acc, span| acc + span.len)
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
            let mut agg_row_count: usize = 0;
            while row_count < target_row_count {
                let group_span = &grouped.spans[span_idx];
                row_count += group_span.len;

                let agg_span = &aggregated.spans[span_idx];
                agg_row_count += agg_span.len;
                // for idx in &aggregated.row_indexes[agg_span.start..agg_span.start + agg_span.len] {
                //     // result.add_row_idx(*idx);
                // }

                span_idx += 1;
            }
            assert_eq!(row_count, target_row_count);
            // result.finish_span();
            result.add_span(agg_row_count);
        }

        result.to_partition()
    }

    pub fn iter(&self) -> PartitionIter {
        self.into_iter()
    }

    pub fn get_limit_row_indexes(&self, limit: usize) -> Vec<usize> {
        self.iter().map(|span| span.take(limit)).flatten().collect()
    }

    pub fn from_row_indexes(&self, row_indexes: &[usize]) -> Partition {
        // let partition = self.rc.as_ref();
        let mut included_indexes = vec![false; self.n_rows()];
        for idx in row_indexes {
            included_indexes[*idx] = true;
        }

        let mut result_builder = PartitionBuilder::new();
        for span in self {
            result_builder.add_span(span.filter(|idx| included_indexes[*idx]).count());
        }

        result_builder.to_partition()
    }

    // pub fn limit(&self, limit: usize) -> Partition {
    //     let mut n_rows: usize = 0;
    //     let mut spans = Vec::<Span>::with_capacity(self.rc.spans.len());
    //     for span in &self.rc.spans {
    //         let len = span.len.min(limit);
    //         spans.push(Span { start: n_rows, len });
    //         n_rows += len;
    //     }

    //     Partition {
    //         rc: Rc::new(InnerPartition { spans }),
    //     }
    // }
}

struct InnerPartition {
    spans: Vec<Span>,
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

        InnerPartition { spans: new_spans }
    }

    fn get_single_value_partition(&self) -> InnerPartition {
        // Reduce ever span down to a single row
        InnerPartition {
            spans: (0..self.spans.len())
                .map(|idx| Span { start: idx, len: 1 })
                .collect(),
        }
    }
}

pub struct PartitionIter<'a> {
    partition: &'a InnerPartition,
    idx: usize,
}

impl<'a> IntoIterator for &'a Partition {
    type Item = std::ops::Range<usize>;

    type IntoIter = PartitionIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        PartitionIter {
            partition: &self.rc,
            idx: 0,
        }
    }
}

impl<'a> Iterator for PartitionIter<'a> {
    type Item = std::ops::Range<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.partition.spans.len() {
            None
        } else {
            let span = &self.partition.spans[self.idx];
            let result = span.start..span.start + span.len;
            self.idx += 1;
            Some(result)
        }
    }
}

pub struct PartitionBuilder {
    spans: Vec<Span>,
}

impl PartitionBuilder {
    pub fn new() -> Self {
        PartitionBuilder { spans: Vec::new() }
    }

    pub fn add_span(&mut self, len: usize) {
        match self.spans.last() {
            None => {
                self.spans.push(Span { start: 0, len });
            }
            Some(prev_span) => {
                self.spans.push(Span {
                    start: prev_span.start + prev_span.len,
                    len,
                });
            }
        }
    }

    pub fn to_partition(self) -> Partition {
        Partition {
            rc: Rc::new(InnerPartition { spans: self.spans }),
        }
    }
}
