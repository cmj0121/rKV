use std::path::Path;

use super::error::{Error, Result};
use super::IoModel;

#[allow(dead_code)]
pub(crate) trait IoBackend: Send + Sync {
    fn read(&self, path: &Path, offset: u64, buf: &mut [u8]) -> Result<usize>;
    fn write(&self, path: &Path, offset: u64, data: &[u8]) -> Result<usize>;
    fn sync(&self, path: &Path) -> Result<()>;
    fn size(&self, path: &Path) -> Result<u64>;
}

pub(crate) struct BufferedIo;

impl IoBackend for BufferedIo {
    fn read(&self, _path: &Path, _offset: u64, _buf: &mut [u8]) -> Result<usize> {
        Err(Error::NotImplemented("buffered_io".into()))
    }

    fn write(&self, _path: &Path, _offset: u64, _data: &[u8]) -> Result<usize> {
        Err(Error::NotImplemented("buffered_io".into()))
    }

    fn sync(&self, _path: &Path) -> Result<()> {
        Err(Error::NotImplemented("buffered_io".into()))
    }

    fn size(&self, _path: &Path) -> Result<u64> {
        Err(Error::NotImplemented("buffered_io".into()))
    }
}

pub(crate) struct DirectIo;

impl IoBackend for DirectIo {
    fn read(&self, _path: &Path, _offset: u64, _buf: &mut [u8]) -> Result<usize> {
        Err(Error::NotImplemented("direct_io".into()))
    }

    fn write(&self, _path: &Path, _offset: u64, _data: &[u8]) -> Result<usize> {
        Err(Error::NotImplemented("direct_io".into()))
    }

    fn sync(&self, _path: &Path) -> Result<()> {
        Err(Error::NotImplemented("direct_io".into()))
    }

    fn size(&self, _path: &Path) -> Result<u64> {
        Err(Error::NotImplemented("direct_io".into()))
    }
}

pub(crate) struct MmapIo;

impl IoBackend for MmapIo {
    fn read(&self, _path: &Path, _offset: u64, _buf: &mut [u8]) -> Result<usize> {
        Err(Error::NotImplemented("mmap_io".into()))
    }

    fn write(&self, _path: &Path, _offset: u64, _data: &[u8]) -> Result<usize> {
        Err(Error::NotImplemented("mmap_io".into()))
    }

    fn sync(&self, _path: &Path) -> Result<()> {
        Err(Error::NotImplemented("mmap_io".into()))
    }

    fn size(&self, _path: &Path) -> Result<u64> {
        Err(Error::NotImplemented("mmap_io".into()))
    }
}

pub(crate) fn create_backend(io_model: &IoModel) -> Box<dyn IoBackend> {
    match io_model {
        IoModel::None => Box::new(BufferedIo),
        IoModel::DirectIO => Box::new(DirectIo),
        IoModel::Mmap => Box::new(MmapIo),
    }
}
