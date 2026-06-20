use rustc_hash::FxHashMap;
use std::any::TypeId;
use std::cell::{BorrowError, BorrowMutError, Cell, Ref, RefCell, RefMut, UnsafeCell};
use std::fmt;
use std::ops::{Deref, DerefMut};

trait AppDataValue: downcast::Any {
    fn clone_to_boxed(&self) -> Box<dyn AppDataValue>;
}

impl<T: Clone + 'static> AppDataValue for RefCell<T> {
    fn clone_to_boxed(&self) -> Box<dyn AppDataValue> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn AppDataValue> {
    fn clone(&self) -> Self {
        self.clone_to_boxed()
    }
}

downcast::downcast!(dyn AppDataValue);

#[derive(Default)]
pub(crate) struct AppData {
    container: UnsafeCell<FxHashMap<TypeId, Box<dyn AppDataValue>>>,
    borrows: Cell<usize>,
}

impl Clone for AppData {
    fn clone(&self) -> Self {
        if self.borrows.get() != 0 {
            panic!("cannot mutably borrow app data container");
        }

        Self {
            container: UnsafeCell::new(unsafe { &*self.container.get() }.clone()),
            borrows: Default::default(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        if self.borrows.get() != 0 {
            panic!("cannot mutably borrow app data container");
        }

        unsafe { &mut *self.container.get() }.clone_from(unsafe { &*source.container.get() })
    }
}

impl AppData {
    #[track_caller]
    pub fn try_insert<T: Clone + 'static>(&self, mut data: T) -> Result<Option<T>, T> {
        if self.borrows.get() != 0 {
            return Err(data);
        }

        let container = unsafe { &mut *self.container.get() };

        match container.entry(TypeId::of::<T>()) {
            std::collections::hash_map::Entry::Occupied(mut occupied_entry) => {
                let cell = occupied_entry.get_mut();
                let cell = cell.downcast_mut::<RefCell<T>>().unwrap();

                let Ok(mut existing_data) = cell.try_borrow_mut() else {
                    return Err(data);
                };

                std::mem::swap(&mut *existing_data, &mut data);

                Ok(Some(data))
            }
            std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(Box::new(RefCell::new(data)));
                Ok(None)
            }
        }
    }

    pub fn try_get<T: 'static>(&self) -> Result<Option<AppDataRef<'_, T>>, BorrowError> {
        let container = unsafe { &*self.container.get() };

        container
            .get(&TypeId::of::<T>())
            .map(|cell| {
                let cell = cell.downcast_ref::<RefCell<T>>().unwrap();
                let data = cell.try_borrow()?;

                let data_ref = AppDataRef {
                    data,
                    borrows: &self.borrows,
                };

                self.borrows.set(self.borrows.get() + 1);

                Ok(data_ref)
            })
            .transpose()
    }

    pub fn try_get_mut<T: 'static>(&self) -> Result<Option<AppDataRefMut<'_, T>>, BorrowMutError> {
        let container = unsafe { &*self.container.get() };

        container
            .get(&TypeId::of::<T>())
            .map(|cell| {
                let cell = cell.downcast_ref::<RefCell<T>>().unwrap();
                let data = cell.try_borrow_mut()?;

                let data_ref = AppDataRefMut {
                    data,
                    borrows: &self.borrows,
                };

                self.borrows.set(self.borrows.get() + 1);

                Ok(data_ref)
            })
            .transpose()
    }

    #[track_caller]
    pub fn remove<T: Clone + 'static>(&self) -> Option<T> {
        if self.borrows.get() != 0 {
            panic!("cannot mutably borrow app data container");
        }

        let container = unsafe { &mut *self.container.get() };

        container
            .remove(&TypeId::of::<T>())
            .map(|cell| cell.downcast::<RefCell<T>>().unwrap().into_inner())
    }
}

/// A wrapper type for an immutably borrowed value from an app data container.
///
/// This type is similar to [`Ref`].
pub struct AppDataRef<'a, T: ?Sized + 'a> {
    data: Ref<'a, T>,
    borrows: &'a Cell<usize>,
}

impl<'a, T: ?Sized> Drop for AppDataRef<'a, T> {
    fn drop(&mut self) {
        self.borrows.set(self.borrows.get() - 1);
    }
}

impl<T: ?Sized> Deref for AppDataRef<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for AppDataRef<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (**self).fmt(f)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for AppDataRef<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (**self).fmt(f)
    }
}

/// A wrapper type for a mutably borrowed value from an app data container.
///
/// This type is similar to [`RefMut`].
pub struct AppDataRefMut<'a, T: ?Sized + 'a> {
    data: RefMut<'a, T>,
    borrows: &'a Cell<usize>,
}

impl<'a, T: ?Sized> Drop for AppDataRefMut<'a, T> {
    fn drop(&mut self) {
        self.borrows.set(self.borrows.get() - 1);
    }
}

impl<T: ?Sized> Deref for AppDataRefMut<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T: ?Sized> DerefMut for AppDataRefMut<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for AppDataRefMut<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (**self).fmt(f)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for AppDataRefMut<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (**self).fmt(f)
    }
}
