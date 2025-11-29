use rayon::prelude::*;
use std::{fs, io, path::{Path, PathBuf}, time::SystemTime};

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub path: PathBuf,
    pub size: Option<u64>,
    pub is_dir: bool,
    pub modified: Option<SystemTime>,
    pub created: Option<SystemTime>,
    pub item_count: Option<usize>,
}

pub fn list_dir(path: &Path) -> io::Result<Vec<FileEntry>> {
    let read_dir = fs::read_dir(path)?;
    let entries: Vec<_> = read_dir.filter_map(|e| e.ok()).collect();

    let mut files: Vec<FileEntry> = entries
        .into_par_iter()
        .filter_map(|entry| {
            let meta = entry.metadata().ok()?;
            let is_dir = meta.is_dir();
            let size = if is_dir { None } else { Some(meta.len()) };
            let modified = meta.modified().ok();
            let created = meta.created().ok();
            
            let item_count = if is_dir {
                fs::read_dir(entry.path()).ok().map(|rd| rd.count())
            } else {
                None
            };

            Some(FileEntry {
                path: entry.path(),
                size,
                is_dir,
                modified,
                created,
                item_count,
            })
        })
        .collect();

    files.par_sort_by_key(|f| (!f.is_dir, std::cmp::Reverse(f.size.unwrap_or(0))));
    Ok(files)
}

pub fn dir_size(path: &Path) -> u64 {
    dir_size_parallel(path)
}

fn dir_size_parallel(path: &Path) -> u64 {
    let entries: Vec<_> = match fs::read_dir(path) {
        Ok(rd) => rd.filter_map(|e| e.ok()).collect(),
        Err(_) => return 0,
    };
    
    entries.par_iter().map(|entry| {
        match entry.metadata() {
            Ok(meta) => {
                if meta.is_dir() {
                    dir_size_parallel(&entry.path())
                } else if meta.is_file() {
                    meta.len()
                } else {
                    0
                }
            }
            Err(_) => 0,
        }
    }).sum()
}

pub fn scan_directory_fast<F>(root: &Path, mut callback: F) 
where
    F: FnMut(FileEntry) + Send,
{
    use std::sync::mpsc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    
    let (tx, rx) = mpsc::channel();
    let active_workers = Arc::new(AtomicUsize::new(1));
    
    let root = root.to_path_buf();
    let tx_clone = tx.clone();
    let workers = active_workers.clone();
    
    rayon::spawn(move || {
        scan_dir_recursive(&root, &tx_clone, &workers);
        workers.fetch_sub(1, Ordering::SeqCst);
    });
    
    drop(tx);
    
    for entry in rx {
        callback(entry);
    }
}

fn scan_dir_recursive(
    path: &Path, 
    tx: &std::sync::mpsc::Sender<FileEntry>,
    active_workers: &std::sync::atomic::AtomicUsize,
) {
    let entries: Vec<_> = match fs::read_dir(path) {
        Ok(rd) => rd.filter_map(|e| e.ok()).collect(),
        Err(_) => return,
    };
    
    entries.par_iter().for_each(|entry| {
        let meta = match entry.metadata() {
            Ok(m) => m,
            Err(_) => return,
        };
        
        let path = entry.path();
        let is_dir = meta.is_dir();
        
        let file_entry = FileEntry {
            path: path.clone(),
            size: if is_dir { None } else { Some(meta.len()) },
            is_dir,
            modified: meta.modified().ok(),
            created: meta.created().ok(),
            item_count: None,
        };
        
        let _ = tx.send(file_entry);
        
        if is_dir {
            use std::sync::atomic::Ordering;
            active_workers.fetch_add(1, Ordering::SeqCst);
            scan_dir_recursive(&path, tx, active_workers);
            active_workers.fetch_sub(1, Ordering::SeqCst);
        }
    });
}
