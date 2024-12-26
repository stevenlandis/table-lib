use std::{borrow::BorrowMut, cell::UnsafeCell, rc::Rc};

pub struct ImmutableAppendList<T> {
    data: UnsafeCell<Vec<Box<T>>>,
}

impl<T> ImmutableAppendList<T> {
    pub fn new() -> Self {
        ImmutableAppendList {
            data: UnsafeCell::new(Vec::new()),
        }
    }

    pub fn push(&self, val: T) {
        let mut_data = unsafe { &mut *self.data.get() };
        mut_data.push(Box::new(val));
    }

    pub fn at(&self, idx: usize) -> &T {
        let data = unsafe { &*self.data.get() };
        data[idx].as_ref()
    }

    pub fn len(&self) -> usize {
        let data = unsafe { &*self.data.get() };
        data.len()
    }
}

// unsafe fn get_mutable_ref<T>(in_ptr: &T) -> &mut T {
//     let const_ptr = in_ptr as *const T;
//     let mut_ptr = const_ptr as *mut T;
//     let mut_ptr = UnsafeCell::new()
//     &mut *mut_ptr
// }

#[cfg(test)]
mod test {
    use super::ImmutableAppendList;

    #[test]
    fn test_basic() {
        let list = ImmutableAppendList::<String>::new();
        list.push("stuff".to_string());

        let first_val = list.at(0);
        assert_eq!(first_val.as_str(), "stuff");

        list.push("and".to_string());
        list.push("things".to_string());

        assert_eq!(list.len(), 3);

        assert_eq!(first_val.as_str(), "stuff");

        for _ in 0..1000 {
            list.push("value".to_string());
        }

        assert_eq!(first_val.as_str(), "stuff");
    }
}
