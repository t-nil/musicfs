pub struct Inode(u64);

mod file {
    use std::{ops::Deref, path::Path};

    use anyhow::{ensure, Result};

    /// # Panic
    /// If `inner` was file and suddenly turns into non-file
    pub struct File<P>
    where
        P: AsRef<Path>,
    {
        inner: P,
    }

    impl<P> File<P>
    where
        P: AsRef<Path>,
    {
        pub fn new(inner: P) -> Result<Self> {
            ensure!(inner.as_ref().is_file());
            Ok(Self { inner })
        }
    }

    impl<P> Deref for File<P>
    where
        P: AsRef<Path>,
    {
        type Target = P;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    impl<P> AsRef<Path> for File<P>
    where
        P: AsRef<Path>,
    {
        fn as_ref(&self) -> &Path {
            self.inner.as_ref()
        }
    }
}
pub use file::File;
