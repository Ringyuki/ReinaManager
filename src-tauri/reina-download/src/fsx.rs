//! Filesystem helpers: positioned writes, sparse preallocation, atomic replace.

use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

/// Writes the whole buffer at `offset` without touching a shared cursor.
///
/// Safe to call concurrently from several threads on the same `File` as long
/// as the ranges do not overlap.
pub(crate) fn write_all_at(file: &File, offset: u64, buf: &[u8]) -> io::Result<()> {
    let mut written = 0usize;
    while written < buf.len() {
        let position = offset + written as u64;
        match write_at(file, position, &buf[written..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "positioned write returned zero bytes",
                ));
            }
            Ok(n) => written += n,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn write_at(file: &File, offset: u64, buf: &[u8]) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.write_at(buf, offset)
}

#[cfg(windows)]
fn write_at(file: &File, offset: u64, buf: &[u8]) -> io::Result<usize> {
    use std::os::windows::fs::FileExt;
    file.seek_write(buf, offset)
}

/// Opens (or creates) the target file and reserves `size` bytes.
///
/// On Windows the file is marked sparse first so that reserving space does not
/// force NTFS to zero-fill the whole file on the first write near the end.
pub(crate) fn open_target(path: &Path, size: u64, preallocate: bool) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    if let Err(error) = mark_sparse(&file) {
        log::debug!("could not mark {} sparse: {error}", path.display());
    }
    if preallocate && file.metadata()?.len() != size {
        file.set_len(size)?;
    }
    Ok(file)
}

#[cfg(windows)]
fn mark_sparse(file: &File) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::System::IO::DeviceIoControl;
    use windows_sys::Win32::System::Ioctl::FSCTL_SET_SPARSE;

    let mut returned: u32 = 0;
    // SAFETY: the handle is valid for the lifetime of `file`; no in/out buffers
    // are passed and `returned` outlives the call.
    let ok = unsafe {
        DeviceIoControl(
            file.as_raw_handle() as _,
            FSCTL_SET_SPARSE,
            std::ptr::null(),
            0,
            std::ptr::null_mut(),
            0,
            &mut returned,
            std::ptr::null_mut(),
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(windows))]
fn mark_sparse(_file: &File) -> io::Result<()> {
    Ok(())
}

/// Writes `bytes` to `path` by way of a temporary sibling and a rename, so a
/// crash never leaves a half-written file behind.
pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let temp = temp_path(path);
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp)?;
        io::Write::write_all(&mut file, bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(&temp, path)
}

fn temp_path(path: &Path) -> PathBuf {
    let mut value: OsString = path.as_os_str().to_owned();
    value.push(".tmp");
    PathBuf::from(value)
}

/// Removes a file, treating "not found" as success.
pub(crate) fn remove_if_exists(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positioned_writes_do_not_interfere() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.bin");
        let file = open_target(&path, 8, true).unwrap();
        write_all_at(&file, 4, b"wxyz").unwrap();
        write_all_at(&file, 0, b"abcd").unwrap();
        drop(file);
        assert_eq!(std::fs::read(&path).unwrap(), b"abcdwxyz");
    }

    #[test]
    fn atomic_write_replaces_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.json");
        atomic_write(&path, b"one").unwrap();
        atomic_write(&path, b"two").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"two");
        assert!(!temp_path(&path).exists());
    }
}
