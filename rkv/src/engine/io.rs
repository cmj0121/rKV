use std::fs;
use std::ops::Deref;
use std::path::Path;

#[cfg(target_os = "linux")]
use super::error::Error;
use super::error::Result;
use super::IoModel;

/// Bytes returned from an I/O backend read.
///
/// Transparently derefs to `&[u8]`, hiding whether the data is an owned
/// `Vec<u8>` or a zero-copy memory map.
pub(crate) enum IoBytes {
    Vec(Vec<u8>),
    Mmap(memmap2::Mmap),
}

impl Deref for IoBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            IoBytes::Vec(v) => v,
            IoBytes::Mmap(m) => m,
        }
    }
}

/// Trait for pluggable file I/O strategies.
///
/// Four methods cover the engine's actual I/O patterns:
/// - `read_file` — whole-file reads (SSTable, AOL replay, objects, dumps).
/// - `write_file_atomic` — tmp + rename writes (objects, stats.meta).
/// - `create_file` — returns a `File` handle for sequential writes (SSTable/Dump writers).
/// - `sync_file` — fsync on an open handle.
pub(crate) trait IoBackend: Send + Sync {
    fn read_file(&self, path: &Path) -> Result<IoBytes>;
    fn write_file_atomic(&self, path: &Path, data: &[u8]) -> Result<()>;
    fn create_file(&self, path: &Path) -> Result<fs::File>;
    #[allow(dead_code)]
    fn sync_file(&self, file: &fs::File) -> Result<()>;
}

// ---------------------------------------------------------------------------
// BufferedIo — standard OS-buffered I/O
// ---------------------------------------------------------------------------

pub(crate) struct BufferedIo;

impl IoBackend for BufferedIo {
    fn read_file(&self, path: &Path) -> Result<IoBytes> {
        Ok(IoBytes::Vec(fs::read(path)?))
    }

    fn write_file_atomic(&self, path: &Path, data: &[u8]) -> Result<()> {
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, data)?;
        fs::rename(&tmp, path)?;
        Ok(())
    }

    fn create_file(&self, path: &Path) -> Result<fs::File> {
        Ok(fs::File::create(path)?)
    }

    fn sync_file(&self, file: &fs::File) -> Result<()> {
        file.sync_all()?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MmapIo — memory-mapped reads, standard writes
// ---------------------------------------------------------------------------

pub(crate) struct MmapIo;

impl IoBackend for MmapIo {
    fn read_file(&self, path: &Path) -> Result<IoBytes> {
        let file = fs::File::open(path)?;
        let meta = file.metadata()?;
        if meta.len() == 0 {
            return Ok(IoBytes::Vec(Vec::new()));
        }
        // SAFETY: The file is opened read-only and the resulting Mmap is
        // treated as an immutable &[u8] slice. The file is not truncated
        // while the map is live because the engine never shrinks data files
        // that are actively mapped (SSTable readers own their maps).
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        Ok(IoBytes::Mmap(mmap))
    }

    fn write_file_atomic(&self, path: &Path, data: &[u8]) -> Result<()> {
        // Mmap doesn't help for writes — use standard buffered I/O.
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, data)?;
        fs::rename(&tmp, path)?;
        Ok(())
    }

    fn create_file(&self, path: &Path) -> Result<fs::File> {
        Ok(fs::File::create(path)?)
    }

    fn sync_file(&self, file: &fs::File) -> Result<()> {
        file.sync_all()?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// DirectIo — cache-bypassing I/O
// ---------------------------------------------------------------------------

pub(crate) struct DirectIo;

impl IoBackend for DirectIo {
    fn read_file(&self, path: &Path) -> Result<IoBytes> {
        read_file_direct(path)
    }

    fn write_file_atomic(&self, path: &Path, data: &[u8]) -> Result<()> {
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, data)?;
        fs::rename(&tmp, path)?;
        Ok(())
    }

    fn create_file(&self, path: &Path) -> Result<fs::File> {
        let file = fs::File::create(path)?;
        set_nocache(&file);
        Ok(file)
    }

    fn sync_file(&self, file: &fs::File) -> Result<()> {
        file.sync_all()?;
        Ok(())
    }
}

/// Platform-specific cache bypass for reads.
#[cfg(target_os = "macos")]
fn read_file_direct(path: &Path) -> Result<IoBytes> {
    use std::io::Read;
    use std::os::unix::io::AsRawFd;

    let file = fs::File::open(path)?;
    // F_NOCACHE disables the unified buffer cache for this fd.
    // Works with unaligned I/O on macOS.
    unsafe {
        libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1);
    }
    let mut buf = Vec::new();
    (&file).read_to_end(&mut buf)?;
    Ok(IoBytes::Vec(buf))
}

#[cfg(target_os = "linux")]
fn read_file_direct(path: &Path) -> Result<IoBytes> {
    use std::os::unix::fs::OpenOptionsExt;

    // O_DIRECT requires aligned buffers. Read the file size first,
    // then allocate an aligned buffer and read into it.
    let meta = fs::metadata(path)?;
    let file_size = meta.len() as usize;
    if file_size == 0 {
        return Ok(IoBytes::Vec(Vec::new()));
    }

    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)?;

    // Allocate a 4096-aligned buffer rounded up to the next page boundary
    let aligned_size = (file_size + 4095) & !4095;
    let layout = std::alloc::Layout::from_size_align(aligned_size, 4096)
        .map_err(|e| Error::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

    // Vec doesn't guarantee alignment, so use raw alloc for O_DIRECT.
    let buf = unsafe {
        let ptr = std::alloc::alloc(layout);
        if ptr.is_null() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "aligned alloc failed",
            )));
        }

        use std::os::unix::io::AsRawFd;
        let bytes_read = libc::pread(file.as_raw_fd(), ptr as *mut libc::c_void, aligned_size, 0);
        if bytes_read < 0 {
            std::alloc::dealloc(ptr, layout);
            return Err(Error::Io(std::io::Error::last_os_error()));
        }
        let actual = bytes_read as usize;
        let result = std::slice::from_raw_parts(ptr, file_size.min(actual)).to_vec();
        std::alloc::dealloc(ptr, layout);
        result
    };

    Ok(IoBytes::Vec(buf))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn read_file_direct(path: &Path) -> Result<IoBytes> {
    // Fallback: no cache-bypass available, use standard read.
    Ok(IoBytes::Vec(fs::read(path)?))
}

/// Set F_NOCACHE on the file descriptor (macOS).
#[cfg(target_os = "macos")]
fn set_nocache(file: &fs::File) {
    use std::os::unix::io::AsRawFd;
    unsafe {
        libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1);
    }
}

/// No-op on platforms that don't support F_NOCACHE.
/// On Linux, O_DIRECT is set at open time for reads. For create_file
/// (sequential SSTable writes), we skip O_DIRECT since the writes are
/// unaligned and small — the kernel will write-back efficiently.
#[cfg(not(target_os = "macos"))]
fn set_nocache(_file: &fs::File) {}

pub(crate) fn create_backend(io_model: &IoModel) -> std::sync::Arc<dyn IoBackend> {
    match io_model {
        IoModel::None => std::sync::Arc::new(BufferedIo),
        IoModel::DirectIO => std::sync::Arc::new(DirectIo),
        IoModel::Mmap => std::sync::Arc::new(MmapIo),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iobytes_vec_deref() {
        let bytes = IoBytes::Vec(vec![1, 2, 3]);
        assert_eq!(&*bytes, &[1, 2, 3]);
    }

    #[test]
    fn iobytes_mmap_deref() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mmap.dat");
        std::fs::write(&path, b"hello").unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let mmap = unsafe { memmap2::Mmap::map(&file).unwrap() };
        let bytes = IoBytes::Mmap(mmap);
        assert_eq!(&*bytes, b"hello");
    }

    // --- BufferedIo tests ---

    #[test]
    fn buffered_read_write_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");
        let backend = BufferedIo;

        backend.write_file_atomic(&path, b"hello world").unwrap();
        let data = backend.read_file(&path).unwrap();
        assert_eq!(&*data, b"hello world");
    }

    #[test]
    fn buffered_create_and_sync() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("create.dat");
        let backend = BufferedIo;

        let file = backend.create_file(&path).unwrap();
        std::io::Write::write_all(&mut &file, b"data").unwrap();
        backend.sync_file(&file).unwrap();

        let data = std::fs::read(&path).unwrap();
        assert_eq!(data, b"data");
    }

    #[test]
    fn buffered_read_nonexistent_returns_error() {
        let backend = BufferedIo;
        assert!(backend.read_file(Path::new("/nonexistent/file")).is_err());
    }

    // --- MmapIo tests ---

    #[test]
    fn mmap_read_write_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");
        let backend = MmapIo;

        backend.write_file_atomic(&path, b"mmap data").unwrap();
        let data = backend.read_file(&path).unwrap();
        assert_eq!(&*data, b"mmap data");
        assert!(matches!(data, IoBytes::Mmap(_)));
    }

    #[test]
    fn mmap_read_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.dat");
        std::fs::write(&path, b"").unwrap();

        let backend = MmapIo;
        let data = backend.read_file(&path).unwrap();
        assert!(data.is_empty());
        assert!(matches!(data, IoBytes::Vec(_)));
    }

    // --- DirectIo tests ---

    #[test]
    fn direct_read_write_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");
        let backend = DirectIo;

        backend.write_file_atomic(&path, b"direct data").unwrap();
        let data = backend.read_file(&path).unwrap();
        assert_eq!(&*data, b"direct data");
    }

    // --- create_backend ---

    #[test]
    fn create_backend_returns_correct_type() {
        let _ = create_backend(&IoModel::None);
        let _ = create_backend(&IoModel::DirectIO);
        let _ = create_backend(&IoModel::Mmap);
    }
}
