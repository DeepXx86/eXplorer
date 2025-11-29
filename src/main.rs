#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::{
    path::{Path, PathBuf},
    sync::mpsc::{self, Receiver, Sender, TryRecvError},
    thread,
    collections::{HashMap, HashSet},
    fs,
    process::Command,
};

use eframe::egui;
use egui_extras::{Column, TableBuilder};
use folder::{dir_size, list_dir, FileEntry};
use sysinfo::Disks;

const MAX_HOME_RESULTS: usize = 2000;
const MAX_INDEX_UPDATES_PER_FRAME: usize = 500;
const MAX_SIZE_UPDATES_PER_FRAME: usize = 5;
const MAX_INDEX_SIZE: usize = 500_000;

#[derive(Clone, Debug)]
struct DriveInfo {
    name: String,
    mount_point: PathBuf,
    total_space: u64,
    available_space: u64,
    used_space: u64,
    file_system: String,
}

struct ExplorerApp {
    current_path: String,
    home_path: PathBuf,
    home_canonical: Option<PathBuf>,
    entries: Vec<FileEntry>,
    error: Option<String>,
    size_updates: Option<Receiver<(PathBuf, u64)>>,
    size_cache: HashMap<PathBuf, u64>,
    search_query: String,
    filtered_entries: Vec<FileEntry>,
    show_properties: Option<FileEntry>,
    confirm_delete: Option<FileEntry>,
    home_index: Vec<HomeIndexedEntry>,
    home_index_rx: Option<Receiver<HomeIndexMessage>>,
    home_index_scanning: bool,
    home_last_query: String,
    home_results_cache: Vec<FileEntry>,
    local_index: Vec<HomeIndexedEntry>,
    local_index_rx: Option<Receiver<HomeIndexMessage>>,
    local_index_scanning: bool,
    local_index_path: Option<PathBuf>,
    local_last_query: String,
    local_results_cache: Vec<FileEntry>,
    drives: Vec<DriveInfo>,
    show_drive_panel: bool,
    search_files: bool,
    search_folders: bool,
    data_folder: PathBuf,
    index_loaded_from_cache: bool,
    index_total_count: usize,
    show_indexing_progress: bool,
    selected_entries: HashSet<PathBuf>,
    confirm_delete_multi: bool,
}

#[derive(Clone, Debug)]
struct HomeIndexedEntry {
    name_lower: String,
    entry: FileEntry,
}

impl ExplorerApp {
    fn detect_home_path() -> PathBuf {
        if let Some(path) = dirs::home_dir() {
            return path;
        }

        #[cfg(target_os = "windows")]
        {
            return PathBuf::from("C:\\");
        }

        #[cfg(not(target_os = "windows"))]
        {
            PathBuf::from("/")
        }
    }

    fn get_drives() -> Vec<DriveInfo> {
        let disks = Disks::new_with_refreshed_list();
        disks.iter().map(|disk| {
            let total = disk.total_space();
            let available = disk.available_space();
            DriveInfo {
                name: disk.name().to_string_lossy().to_string(),
                mount_point: disk.mount_point().to_path_buf(),
                total_space: total,
                available_space: available,
                used_space: total.saturating_sub(available),
                file_system: disk.file_system().to_string_lossy().to_string(),
            }
        }).collect()
    }

    fn refresh_drives(&mut self) {
        self.drives = Self::get_drives();
    }

    fn get_data_folder() -> PathBuf {
        let data_folder = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|p| p.to_path_buf()))
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
        
        let data_path = data_folder.join("eXplorer_data");
        
        if !data_path.exists() {
            let _ = fs::create_dir_all(&data_path);
        }
        
        data_path
    }

    fn cache_file_path(data_folder: &Path) -> PathBuf {
        data_folder.join("size_cache.dat")
    }
    
    fn index_file_path(data_folder: &Path) -> PathBuf {
        data_folder.join("home_index.dat")
    }
    
    fn load_cache(data_folder: &Path) -> HashMap<PathBuf, u64> {
        if let Ok(content) = fs::read_to_string(Self::cache_file_path(data_folder)) {
            let mut cache = HashMap::new();
            for line in content.lines() {
                if let Some((path_str, size_str)) = line.split_once('|') {
                    if let Ok(size) = size_str.parse::<u64>() {
                        cache.insert(PathBuf::from(path_str), size);
                    }
                }
            }
            cache
        } else {
            HashMap::new()
        }
    }
    
    fn save_cache(&self) {
        let mut content = String::new();
        for (path, size) in &self.size_cache {
            content.push_str(&format!("{}|{}\n", path.display(), size));
        }
        let _ = fs::write(Self::cache_file_path(&self.data_folder), content);
    }
    
    fn save_home_index(&self) {
        let mut content = String::new();
        for indexed in &self.home_index {
            let entry = &indexed.entry;
            let size_str = entry.size.map(|s| s.to_string()).unwrap_or_default();
            let modified_str = entry.modified
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs().to_string())
                .unwrap_or_default();
            let created_str = entry.created
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs().to_string())
                .unwrap_or_default();
            let item_count_str = entry.item_count.map(|c| c.to_string()).unwrap_or_default();
            
            content.push_str(&format!(
                "{}|{}|{}|{}|{}|{}\n",
                entry.path.display(),
                if entry.is_dir { "1" } else { "0" },
                size_str,
                modified_str,
                created_str,
                item_count_str
            ));
        }
        let _ = fs::write(Self::index_file_path(&self.data_folder), content);
    }
    
    fn load_home_index(data_folder: &Path) -> Option<Vec<HomeIndexedEntry>> {
        let content = fs::read_to_string(Self::index_file_path(data_folder)).ok()?;
        let mut index = Vec::new();
        
        for line in content.lines() {
            let parts: Vec<&str> = line.splitn(6, '|').collect();
            if parts.len() < 2 {
                continue;
            }
            
            let path = PathBuf::from(parts[0]);
            let is_dir = parts.get(1).map(|s| *s == "1").unwrap_or(false);
            let size = parts.get(2).and_then(|s| s.parse().ok());
            let modified = parts.get(3)
                .and_then(|s| s.parse::<u64>().ok())
                .map(|secs| std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs));
            let created = parts.get(4)
                .and_then(|s| s.parse::<u64>().ok())
                .map(|secs| std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs));
            let item_count = parts.get(5).and_then(|s| s.parse().ok());
            
            let name_lower = path.file_name()
                .and_then(|s| s.to_str())
                .map(|s| s.to_lowercase())
                .unwrap_or_default();
            
            if name_lower.is_empty() {
                continue;
            }
            
            index.push(HomeIndexedEntry {
                name_lower,
                entry: FileEntry {
                    path,
                    size: if is_dir { None } else { size },
                    is_dir,
                    modified,
                    created,
                    item_count,
                },
            });
        }
        
        if index.is_empty() {
            None
        } else {
            Some(index)
        }
    }

    fn is_path_home(&self, candidate: &Path) -> bool {
        if let Some(home_can) = &self.home_canonical {
            if let Ok(candidate_can) = candidate.canonicalize() {
                return candidate_can == *home_can;
            }
        }
        candidate == self.home_path
    }

    fn is_at_home(&self) -> bool {
        let current = PathBuf::from(&self.current_path);
        self.is_path_home(&current)
    }
    fn start_home_indexing(&mut self) {
        if self.home_index_scanning || self.home_index_rx.is_some() {
            return;
        }

        if !self.index_loaded_from_cache {
            self.home_index.clear();
            self.home_index.shrink_to_fit();
        }

        let (tx, rx) = mpsc::channel();
        self.home_index_rx = Some(rx);
        self.home_index_scanning = true;
        let root = self.home_path.clone();
        let max_entries = MAX_INDEX_SIZE;
        
        thread::spawn(move || {
            Self::build_home_index_fast(root, tx, max_entries);
        });
    }

    fn build_home_index_fast(root: PathBuf, tx: Sender<HomeIndexMessage>, max_entries: usize) {
        use rayon::prelude::*;
        use std::sync::Mutex;
        use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
        
        let tx = Mutex::new(tx);
        let entry_count = AtomicUsize::new(0);
        let should_stop = AtomicBool::new(false);
        
        fn scan_parallel(
            path: &Path, 
            tx: &Mutex<Sender<HomeIndexMessage>>,
            visited: &Mutex<HashSet<PathBuf>>,
            entry_count: &AtomicUsize,
            should_stop: &AtomicBool,
            max_entries: usize,
        ) {
            if should_stop.load(Ordering::Relaxed) {
                return;
            }
            
            let key = path.to_path_buf();
            {
                let mut v = visited.lock().unwrap();
                if !v.insert(key) {
                    return;
                }
            }
            
            let entries: Vec<_> = match fs::read_dir(path) {
                Ok(rd) => rd.filter_map(|e| e.ok()).collect(),
                Err(_) => return,
            };
            
            entries.par_iter().for_each(|entry| {
                if should_stop.load(Ordering::Relaxed) {
                    return;
                }
                
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
                
                let name_lower = file_entry
                    .path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_lowercase());
                
                if let Some(name_lower) = name_lower {
                    let current = entry_count.fetch_add(1, Ordering::Relaxed);
                    if current >= max_entries {
                        should_stop.store(true, Ordering::Relaxed);
                        return;
                    }
                    
                    let entry_pkg = HomeIndexedEntry {
                        name_lower,
                        entry: file_entry,
                    };
                    
                    let _ = tx.lock().unwrap().send(HomeIndexMessage::Entry(entry_pkg));
                }
                
                if is_dir && !should_stop.load(Ordering::Relaxed) {
                    scan_parallel(&path, tx, visited, entry_count, should_stop, max_entries);
                }
            });
        }
        
        let visited: Mutex<HashSet<PathBuf>> = Mutex::new(HashSet::with_capacity(50_000));
        scan_parallel(&root, &tx, &visited, &entry_count, &should_stop, max_entries);
        
        drop(visited);
        
        let _ = tx.lock().unwrap().send(HomeIndexMessage::Finished);
    }

    fn poll_home_index(&mut self) {
        let mut drop_receiver = false;
        let mut updated = false;
        let mut updates_count = 0;

        if let Some(rx) = &self.home_index_rx {
            loop {
                match rx.try_recv() {
                    Ok(HomeIndexMessage::Entry(entry)) => {
                        self.home_index.push(entry);
                        updated = true;
                        updates_count += 1;
                        if updates_count >= MAX_INDEX_UPDATES_PER_FRAME {
                            break;
                        }
                    }
                    Ok(HomeIndexMessage::Finished) => {
                        drop_receiver = true;
                        break;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        drop_receiver = true;
                        break;
                    }
                }
            }
        }

        if drop_receiver {
            self.home_index_rx = None;
            self.home_index_scanning = false;
            self.show_indexing_progress = false;
            self.index_total_count = self.home_index.len();
            self.home_index.shrink_to_fit();
            self.index_loaded_from_cache = false;
            self.save_home_index();
        }
        
        if updated {
            self.index_total_count = self.home_index.len();
        }

        if updated && self.is_at_home() && !self.home_last_query.is_empty() {
            if drop_receiver || self.home_index.len() % 1000 < MAX_INDEX_UPDATES_PER_FRAME {
                self.home_results_cache = self.filter_home_index(&self.home_last_query);
                self.filtered_entries = self.home_results_cache.clone();
                Self::sort_entries(&mut self.filtered_entries, &self.size_cache);
            }
        }
    }

    fn filter_home_index(&self, query_lower: &str) -> Vec<FileEntry> {
        let mut results = Vec::new();
        for indexed in &self.home_index {
            if indexed.name_lower.contains(query_lower) {
                let dominated = (indexed.entry.is_dir && self.search_folders)
                    || (!indexed.entry.is_dir && self.search_files);
                if !dominated {
                    continue;
                }
                results.push(indexed.entry.clone());
                if results.len() >= MAX_HOME_RESULTS {
                    break;
                }
            }
        }
        results
    }

    fn start_local_indexing(&mut self, path: PathBuf) {
        self.local_index.clear();
        self.local_index.shrink_to_fit();
        self.local_results_cache.clear();
        self.local_results_cache.shrink_to_fit();
        self.local_last_query.clear();
        self.local_index_path = Some(path.clone());
        
        let (tx, rx) = mpsc::channel();
        self.local_index_rx = Some(rx);
        self.local_index_scanning = true;
        let max_entries = MAX_INDEX_SIZE / 2; 
        
        thread::spawn(move || {
            Self::build_home_index_fast(path, tx, max_entries);
        });
    }

    fn poll_local_index(&mut self) {
        let mut drop_receiver = false;
        let mut updated = false;
        let mut updates_count = 0;

        if let Some(rx) = &self.local_index_rx {
            loop {
                match rx.try_recv() {
                    Ok(HomeIndexMessage::Entry(entry)) => {
                        self.local_index.push(entry);
                        updated = true;
                        updates_count += 1;
                        if updates_count >= MAX_INDEX_UPDATES_PER_FRAME {
                            break;
                        }
                    }
                    Ok(HomeIndexMessage::Finished) => {
                        drop_receiver = true;
                        break;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        drop_receiver = true;
                        break;
                    }
                }
            }
        }

        if drop_receiver {
            self.local_index_rx = None;
            self.local_index_scanning = false;
            self.local_index.shrink_to_fit();
        }

        if updated && !self.is_at_home() && !self.local_last_query.is_empty() {
            if drop_receiver || self.local_index.len() % 1000 < MAX_INDEX_UPDATES_PER_FRAME {
                self.local_results_cache = self.filter_local_index(&self.local_last_query);
                self.filtered_entries = self.local_results_cache.clone();
                Self::sort_entries(&mut self.filtered_entries, &self.size_cache);
            }
        }
    }

    fn filter_local_index(&self, query_lower: &str) -> Vec<FileEntry> {
        let mut results = Vec::new();
        for indexed in &self.local_index {
            if indexed.name_lower.contains(query_lower) {
                let dominated = (indexed.entry.is_dir && self.search_folders)
                    || (!indexed.entry.is_dir && self.search_files);
                if !dominated {
                    continue;
                }
                results.push(indexed.entry.clone());
                if results.len() >= MAX_HOME_RESULTS {
                    break;
                }
            }
        }
        results
    }

    fn sort_entries(entries: &mut Vec<FileEntry>, cache: &HashMap<PathBuf, u64>) {
        entries.sort_by(|a, b| match (a.is_dir, b.is_dir) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => {
                let size_a = a.size.or_else(|| cache.get(&a.path).copied()).unwrap_or(0);
                let size_b = b.size.or_else(|| cache.get(&b.path).copied()).unwrap_or(0);
                size_b.cmp(&size_a)
            }
        });
    }

    fn new() -> Self {
        let home_path = Self::detect_home_path();
        let home_canonical = home_path.canonicalize().ok();
        let default_path = home_path.to_string_lossy().to_string();
        let drives = Self::get_drives();
        let mut app = ExplorerApp {
            current_path: default_path,
            home_path,
            home_canonical,
            entries: Vec::new(),
            error: None,
            size_updates: None,
            size_cache: HashMap::new(),
            search_query: String::new(),
            filtered_entries: Vec::new(),
            show_properties: None,
            confirm_delete: None,
            home_index: Vec::new(),
            home_index_rx: None,
            home_index_scanning: false,
            home_last_query: String::new(),
            home_results_cache: Vec::new(),
            local_index: Vec::new(),
            local_index_rx: None,
            local_index_scanning: false,
            local_index_path: None,
            local_last_query: String::new(),
            local_results_cache: Vec::new(),
            drives,
            show_drive_panel: true,
            search_files: true,
            search_folders: true,
            data_folder: PathBuf::new(),
            index_loaded_from_cache: false,
            index_total_count: 0,
            show_indexing_progress: false,
            selected_entries: HashSet::new(),
            confirm_delete_multi: false,
        };
        
        app.data_folder = Self::get_data_folder();
        
        app.size_cache = Self::load_cache(&app.data_folder);
        
        if let Some(cached_index) = Self::load_home_index(&app.data_folder) {
            app.home_index = cached_index;
            app.index_loaded_from_cache = true;
            app.index_total_count = app.home_index.len();
            app.start_home_indexing();
        } else {
            app.show_indexing_progress = true;
            app.start_home_indexing();
        }
        
        app.refresh();
        app
    }

    fn refresh(&mut self) {
        let path_buf = PathBuf::from(&self.current_path);
        
        if self.local_index_path.as_ref() != Some(&path_buf) {
            self.local_index.clear();
            self.local_index.shrink_to_fit();
            self.local_index_rx = None;
            self.local_index_scanning = false;
            self.local_index_path = None;
            self.local_last_query.clear();
            self.local_results_cache.clear();
            self.local_results_cache.shrink_to_fit();
        }
        
        match list_dir(&path_buf) {
            Ok(list) => {
                self.entries = list;
                self.error = None;
                self.update_filtered_entries();
                self.start_size_worker();
            }
            Err(e) => {
                self.entries.clear();
                self.filtered_entries.clear();
                self.error = Some(e.to_string());
                self.size_updates = None;
            }
        }
    }

    fn update_filtered_entries(&mut self) {
        let query = self.search_query.trim();
        if query.is_empty() {
            self.home_last_query.clear();
            self.home_results_cache.clear();
            self.filtered_entries = self.entries.clone();
        } else {
            let query_lower = query.to_lowercase();
            if self.is_at_home() {
                if self.home_last_query != query_lower {
                    self.home_last_query = query_lower.clone();
                    self.home_results_cache = self.filter_home_index(&query_lower);
                }
                self.filtered_entries = self.home_results_cache.clone();
            } else {
                self.home_last_query.clear();
                self.home_results_cache.clear();
                
                let current = PathBuf::from(&self.current_path);
                if self.local_index_path.as_ref() != Some(&current) {
                    self.start_local_indexing(current);
                }
                
                if self.local_last_query != query_lower {
                    self.local_last_query = query_lower.clone();
                    self.local_results_cache = self.filter_local_index(&query_lower);
                }
                self.filtered_entries = self.local_results_cache.clone();
            }
        }
        
        Self::sort_entries(&mut self.filtered_entries, &self.size_cache);
    }

    fn go_to(&mut self, path: PathBuf) {
        let current_dir = PathBuf::from(&self.current_path);
        if current_dir.is_dir() {
            let all_sizes_known = self.entries.iter().all(|e| e.size.is_some());
            if all_sizes_known {
                let total_size: u64 = self
                    .entries
                    .iter()
                    .filter_map(|e| e.size)
                    .sum();
                self.size_cache.insert(current_dir.clone(), total_size);
            }
        }

        if self.size_updates.is_none() {
            self.save_cache();
        }
        
        self.local_index.clear();
        self.local_index.shrink_to_fit();
        self.local_index_rx = None;
        self.local_index_scanning = false;
        self.local_index_path = None;
        self.local_last_query.clear();
        self.local_results_cache.clear();
        self.local_results_cache.shrink_to_fit();
        
        self.selected_entries.clear();
        
        self.current_path = path.to_string_lossy().to_string();
        self.refresh();
    }

    fn start_size_worker(&mut self) {
        let mut dirs: Vec<PathBuf> = Vec::new();
        let mut cached_updates: Vec<(PathBuf, u64)> = Vec::new();
        
        for entry in &self.entries {
            if entry.is_dir {
                if let Some(&cached_size) = self.size_cache.get(&entry.path) {
                    cached_updates.push((entry.path.clone(), cached_size));
                } else {
                    dirs.push(entry.path.clone());
                }
            }
        }
        
        for (path, size) in cached_updates {
            if let Some(e) = self.entries.iter_mut().find(|e| e.path == path) {
                e.size = Some(size);
            }
        }
        
        Self::sort_entries(&mut self.entries, &self.size_cache);

        if !dirs.is_empty() {
            let (tx, rx) = mpsc::channel();
            thread::spawn(move || {
                use rayon::prelude::*;
                dirs.par_iter().for_each(|path| {
                    let size = dir_size(path);
                    let _ = tx.send((path.clone(), size));
                });
            });

            self.size_updates = Some(rx);
        } else {
            self.size_updates = None;
        }
    }

    fn apply_size_updates(&mut self) {
        let mut resort_needed = false;
        let mut receiver_gone = false;
        let mut updates_count = 0;

        if let Some(rx) = &self.size_updates {
            loop {
                match rx.try_recv() {
                    Ok((path, size)) => {
                        self.size_cache.insert(path.clone(), size);
                        
                        if let Some(entry) = self.entries.iter_mut().find(|e| e.path == path) {
                            entry.size = Some(size);
                            resort_needed = true;
                            updates_count += 1;
                        }
                        
                        if let Some(entry) = self.filtered_entries.iter_mut().find(|e| e.path == path) {
                            entry.size = Some(size);
                        }
                        
                        if updates_count >= MAX_SIZE_UPDATES_PER_FRAME {
                            break;
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        receiver_gone = true;
                        break;
                    }
                }
            }
        }

        if receiver_gone {
            self.size_updates = None;
            self.save_cache();
        }

        if resort_needed {
            Self::sort_entries(&mut self.entries, &self.size_cache);
            Self::sort_entries(&mut self.filtered_entries, &self.size_cache);
        }

        if self.entries.iter().all(|e| e.size.is_some()) {
            let total_size: u64 = self
                .entries
                .iter()
                .filter_map(|e| e.size)
                .sum();
            let current_path = PathBuf::from(&self.current_path);
            let already_cached = self
                .size_cache
                .get(&current_path)
                .map(|value| *value == total_size)
                .unwrap_or(false);
            if !already_cached {
                self.size_cache.insert(current_path, total_size);
                if self.size_updates.is_none() {
                    self.save_cache();
                }
            }
        }
    }
}

#[derive(Debug)]
enum HomeIndexMessage {
    Entry(HomeIndexedEntry),
    Finished,
}

impl eframe::App for ExplorerApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.apply_size_updates();
        self.poll_home_index();
        self.poll_local_index();
        
        if self.size_updates.is_some() || self.home_index_rx.is_some() || self.local_index_rx.is_some() {
            ctx.request_repaint_after(std::time::Duration::from_millis(16));
        }
        
        if ctx.input(|i| i.key_pressed(egui::Key::Delete)) && !self.selected_entries.is_empty() {
            self.confirm_delete_multi = true;
        }
        
        if ctx.input(|i| i.modifiers.ctrl && i.key_pressed(egui::Key::A)) {
            for entry in &self.filtered_entries {
                self.selected_entries.insert(entry.path.clone());
            }
        }
        
        if ctx.input(|i| i.key_pressed(egui::Key::Escape)) {
            self.selected_entries.clear();
        }

        egui::TopBottomPanel::top("top_bar").show(ctx, |ui| {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("📂 Path").strong().size(14.0));
                let path_edit = egui::TextEdit::singleline(&mut self.current_path)
                    .desired_width(ui.available_width() - 110.0)
                    .font(egui::TextStyle::Monospace);
                let resp = ui.add(path_edit);
                if resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                    self.refresh();
                }
                if ui.button("Go").clicked() {
                    self.refresh();
                }
                if ui.button("⬆ Up").clicked() {
                    if let Some(parent) = PathBuf::from(&self.current_path).parent() {
                        self.current_path = parent.to_string_lossy().to_string();
                        self.refresh();
                    }
                }
            });
            
            ui.add_space(2.0);
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("🔍 Search").strong().size(14.0));
                let search_edit = egui::TextEdit::singleline(&mut self.search_query)
                    .desired_width(ui.available_width() - 220.0)
                    .hint_text("Type to search files and folders...");
                let search_resp = ui.add(search_edit);
                if search_resp.changed() {
                    self.update_filtered_entries();
                }
                
                let file_icon = if self.search_files { "✅📄" } else { "❌📄" };
                if ui.small_button(file_icon).on_hover_text("Toggle file search").clicked() {
                    self.search_files = !self.search_files;
                    self.home_last_query.clear();
                    self.local_last_query.clear();
                    self.update_filtered_entries();
                }
                
                let folder_icon = if self.search_folders { "✅📁" } else { "❌📁" };
                if ui.small_button(folder_icon).on_hover_text("Toggle folder search").clicked() {
                    self.search_folders = !self.search_folders;
                    self.home_last_query.clear();
                    self.local_last_query.clear();
                    self.update_filtered_entries();
                }
                
                if ui.button("Clear").clicked() {
                    self.search_query.clear();
                    self.update_filtered_entries();
                }
            });
            ui.add_space(4.0);

            if let Some(err) = &self.error {
                ui.colored_label(egui::Color32::RED, err);
            }
        });

        let mut navigate_to: Option<PathBuf> = None;

        egui::SidePanel::left("drive_panel")
            .resizable(true)
            .default_width(200.0)
            .min_width(150.0)
            .max_width(400.0)
            .show_animated(ctx, self.show_drive_panel, |ui| {
                ui.add_space(4.0);
                ui.horizontal(|ui| {
                    ui.heading("💾 Drives");
                    if ui.small_button("🔄").on_hover_text("Refresh drives").clicked() {
                        self.refresh_drives();
                    }
                });
                ui.separator();
                
                egui::ScrollArea::vertical().show(ui, |ui| {
                    let drives_clone = self.drives.clone();
                    for drive in &drives_clone {
                        let drive_label = if drive.name.is_empty() {
                            drive.mount_point.to_string_lossy().to_string()
                        } else {
                            format!("{} ({})", drive.name, drive.mount_point.display())
                        };
                        
                        ui.group(|ui| {
                            ui.set_width(ui.available_width());
                            
                            let resp = ui.selectable_label(false, egui::RichText::new(&drive_label).strong());
                            if resp.clicked() {
                                navigate_to = Some(drive.mount_point.clone());
                            }
                            
                            let used_percent = if drive.total_space > 0 {
                                drive.used_space as f32 / drive.total_space as f32
                            } else {
                                0.0
                            };
                            
                            let bar_color = if used_percent > 0.9 {
                                egui::Color32::from_rgb(220, 50, 50)  
                            } else if used_percent > 0.75 {
                                egui::Color32::from_rgb(220, 180, 50)  
                            } else {
                                egui::Color32::from_rgb(50, 150, 220)  
                            };
                            
                            let bar_height = 8.0;
                            let bar_width = ui.available_width();
                            
                            let (rect, _response) = ui.allocate_exact_size(
                                egui::vec2(bar_width, bar_height),
                                egui::Sense::hover()
                            );
                            
                            ui.painter().rect_filled(
                                rect,
                                2.0,
                                egui::Color32::from_gray(60)
                            );
                            
                            let used_rect = egui::Rect::from_min_size(
                                rect.min,
                                egui::vec2(bar_width * used_percent, bar_height)
                            );
                            ui.painter().rect_filled(used_rect, 2.0, bar_color);
                            
                            ui.label(format!(
                                "{} / {} free",
                                human_readable_size(drive.used_space),
                                human_readable_size(drive.available_space)
                            ));
                            ui.label(format!("Total: {}", human_readable_size(drive.total_space)));
                            
                            if !drive.file_system.is_empty() {
                                ui.label(egui::RichText::new(&drive.file_system).small().weak());
                            }
                        });
                        ui.add_space(4.0);
                    }
                    
                    ui.add_space(8.0);
                    ui.separator();
                    
                    ui.label(egui::RichText::new("Quick Access").strong());
                    
                    if ui.selectable_label(false, "🏠 Home").clicked() {
                        navigate_to = Some(self.home_path.clone());
                    }
                    
                    #[cfg(target_os = "windows")]
                    {
                        if let Some(desktop) = dirs::desktop_dir() {
                            if ui.selectable_label(false, "🖥️ Desktop").clicked() {
                                navigate_to = Some(desktop);
                            }
                        }
                        if let Some(docs) = dirs::document_dir() {
                            if ui.selectable_label(false, "📄 Documents").clicked() {
                                navigate_to = Some(docs);
                            }
                        }
                        if let Some(downloads) = dirs::download_dir() {
                            if ui.selectable_label(false, "⬇️ Downloads").clicked() {
                                navigate_to = Some(downloads);
                            }
                        }
                    }
                });
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            egui::Frame::group(ui.style())
                .inner_margin(egui::Margin::symmetric(12.0, 8.0))
                .show(ui, |ui| {
                    ui.horizontal(|ui| {
                        let panel_icon = if self.show_drive_panel { "◀" } else { "▶" };
                        if ui.button(panel_icon).on_hover_text("Toggle drive panel").clicked() {
                            self.show_drive_panel = !self.show_drive_panel;
                        }
                        ui.heading("eXplorer");
                        ui.separator();
                        ui.label(format!("{} ({} items)", &self.current_path, self.entries.len()));
                        if !self.search_query.is_empty() {
                            ui.separator();
                            ui.label(format!("Found: {} items", self.filtered_entries.len()));
                        }
                        
                        if !self.selected_entries.is_empty() {
                            ui.separator();
                            ui.label(format!("Selected: {}", self.selected_entries.len()));
                            if ui.button("🗑 Delete Selected").clicked() {
                                self.confirm_delete_multi = true;
                            }
                            if ui.button("✖ Clear Selection").clicked() {
                                self.selected_entries.clear();
                            }
                        }
                    });
                    ui.add_space(6.0);
                    TableBuilder::new(ui)
                        .striped(true)
                        .resizable(true)
                        .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                        .column(Column::auto().at_least(60.0).resizable(true))
                        .column(Column::initial(250.0).at_least(100.0).resizable(true))
                        .column(Column::auto().at_least(70.0).resizable(true))
                        .column(Column::auto().at_least(80.0).resizable(true))
                        .column(Column::auto().at_least(140.0).resizable(true))
                        .column(Column::remainder().at_least(100.0).resizable(true))
                        .header(24.0, |mut header| {
                            header.col(|ui| {
                                ui.strong("Type");
                            });
                            header.col(|ui| {
                                ui.strong("Name");
                            });
                            header.col(|ui| {
                                ui.strong("Items");
                            });
                            header.col(|ui| {
                                ui.strong("Size");
                            });
                            header.col(|ui| {
                                ui.strong("Date Modified");
                            });
                            header.col(|ui| {
                                ui.strong("Path");
                            });
                        })
                        .body(|mut body| {
                            for entry in &self.filtered_entries {
                                let name = entry
                                    .path
                                    .file_name()
                                    .and_then(|s| s.to_str())
                                    .unwrap_or("<unknown>");
                                
                                let display_name = if name.len() > 100 {
                                    let mut truncate_at = 97;
                                    while truncate_at > 0 && !name.is_char_boundary(truncate_at) {
                                        truncate_at -= 1;
                                    }
                                    format!("{}...", &name[..truncate_at])
                                } else {
                                    name.to_string()
                                };
                                
                                let entry_path = entry.path.clone();
                                let is_dir = entry.is_dir;
                                let is_selected = self.selected_entries.contains(&entry_path);
                                let size_text = match entry.size {
                                    Some(size) => human_readable_size(size),
                                    None if is_dir => {
                                        if let Some(cached_size) = self.size_cache.get(&entry.path) {
                                            human_readable_size(*cached_size)
                                        } else {
                                            "…".to_string()
                                        }
                                    }
                                    None => "0 B".to_string(),
                                };
                                let date_text = entry.modified
                                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                                    .map(|d| format_timestamp(d.as_secs()))
                                    .unwrap_or_else(|| "-".to_string());

                                let mut single_clicked = false;
                                let mut double_clicked = false;
                                let mut show_context_menu = false;
                                let ctrl_held = ctx.input(|i| i.modifiers.ctrl);
                                
                                body.row(22.0, |mut row| {
                                    row.col(|ui| {
                                        let label = if is_dir { "📁 Folder" } else { "📄 File" };
                                        let resp = grey_selectable_label(ui, is_selected, label);
                                        if resp.clicked() {
                                            single_clicked = true;
                                        }
                                        if resp.double_clicked() {
                                            double_clicked = true;
                                        }
                                        if resp.secondary_clicked() {
                                            show_context_menu = true;
                                        }
                                    });
                                    row.col(|ui| {
                                        let resp = grey_selectable_label(ui, is_selected, &display_name);
                                        if resp.clicked() {
                                            single_clicked = true;
                                        }
                                        if resp.double_clicked() {
                                            double_clicked = true;
                                        }
                                        if resp.secondary_clicked() {
                                            show_context_menu = true;
                                        }
                                        if name.len() > 100 {
                                            resp.on_hover_text(name);
                                        }
                                    });
                                    row.col(|ui| {
                                        let items_text = if is_dir {
                                            match entry.item_count {
                                                Some(count) => format!("{} items", count),
                                                None => "…".to_string(),
                                            }
                                        } else {
                                            "-".to_string()
                                        };
                                        let resp = grey_selectable_label(ui, is_selected, &items_text);
                                        if resp.clicked() {
                                            single_clicked = true;
                                        }
                                        if resp.double_clicked() {
                                            double_clicked = true;
                                        }
                                        if resp.secondary_clicked() {
                                            show_context_menu = true;
                                        }
                                    });
                                    row.col(|ui| {
                                        let resp = grey_selectable_label(ui, is_selected, &size_text);
                                        if resp.clicked() {
                                            single_clicked = true;
                                        }
                                        if resp.double_clicked() {
                                            double_clicked = true;
                                        }
                                        if resp.secondary_clicked() {
                                            show_context_menu = true;
                                        }
                                    });
                                    row.col(|ui| {
                                        let resp = grey_selectable_label(ui, is_selected, &date_text);
                                        if resp.clicked() {
                                            single_clicked = true;
                                        }
                                        if resp.double_clicked() {
                                            double_clicked = true;
                                        }
                                        if resp.secondary_clicked() {
                                            show_context_menu = true;
                                        }
                                    });
                                    row.col(|ui| {
                                        let path_str = entry_path.display().to_string();
                                        let short_path = if path_str.chars().count() > 30 {
                                            let skip = path_str.chars().count().saturating_sub(27);
                                            format!("...{}", path_str.chars().skip(skip).collect::<String>())
                                        } else {
                                            path_str.clone()
                                        };
                                        let resp = grey_selectable_label(ui, is_selected, &short_path);
                                        if resp.clicked() {
                                            single_clicked = true;
                                        }
                                        if resp.double_clicked() {
                                            double_clicked = true;
                                        }
                                        if resp.secondary_clicked() {
                                            show_context_menu = true;
                                        }
                                        resp.on_hover_text(&path_str);
                                    });
                                });

                                if show_context_menu {
                                    self.show_properties = Some(entry.clone());
                                }

                                if single_clicked && !double_clicked {
                                    if ctrl_held {
                                        if is_selected {
                                            self.selected_entries.remove(&entry_path);
                                        } else {
                                            self.selected_entries.insert(entry_path.clone());
                                        }
                                    } else {
                                        self.selected_entries.clear();
                                        self.selected_entries.insert(entry_path.clone());
                                    }
                                }
                                
                                if double_clicked {
                                    if is_dir {
                                        navigate_to = Some(entry_path);
                                    } else if let Err(err) = open_entry(&entry_path) {
                                        self.error = Some(format!(
                                            "Failed to open file: {}",
                                            err
                                        ));
                                    }
                                }
                            }
                        });
                });
        });

        if let Some(path) = navigate_to {
            self.go_to(path);
        }
        
        if let Some(entry) = &self.show_properties.clone() {
            let mut close_dialog = false;
            let mut delete_file = false;
            
            egui::Window::new("Properties")
                .collapsible(false)
                .resizable(false)
                .show(ctx, |ui| {
                    ui.vertical(|ui| {
                        ui.heading(entry.path.file_name().and_then(|n| n.to_str()).unwrap_or("Unknown"));
                        ui.separator();
                        
                        ui.horizontal(|ui| {
                            ui.label("Type:");
                            ui.label(if entry.is_dir { "Folder" } else { "File" });
                        });
                        
                        ui.horizontal(|ui| {
                            ui.label("Location:");
                            ui.label(entry.path.parent().and_then(|p| p.to_str()).unwrap_or(""));
                        });
                        
                        ui.horizontal(|ui| {
                            ui.label("Size:");
                            let size = entry.size.or_else(|| self.size_cache.get(&entry.path).copied()).unwrap_or(0);
                            ui.label(human_readable_size(size));
                        });
                        
                        if let Some(modified) = entry.modified {
                            if let Ok(duration) = modified.duration_since(std::time::UNIX_EPOCH) {
                                ui.horizontal(|ui| {
                                    ui.label("Modified:");
                                    ui.label(format_timestamp(duration.as_secs()));
                                });
                            }
                        }
                        
                        if let Some(created) = entry.created {
                            if let Ok(duration) = created.duration_since(std::time::UNIX_EPOCH) {
                                ui.horizontal(|ui| {
                                    ui.label("Created:");
                                    ui.label(format_timestamp(duration.as_secs()));
                                });
                            }
                        }
                        
                        ui.horizontal(|ui| {
                            ui.label("Full Path:");
                        });
                        ui.label(entry.path.display().to_string());
                        
                        ui.separator();
                        ui.horizontal(|ui| {
                            if ui.button("🗑 Delete").clicked() {
                                delete_file = true;
                            }
                            if ui.button("Close").clicked() {
                                close_dialog = true;
                            }
                        });
                    });
                });
            
            if delete_file {
                self.confirm_delete = Some(entry.clone());
                close_dialog = true;
            }
            
            if close_dialog {
                self.show_properties = None;
            }
        }
        
        if let Some(entry) = &self.confirm_delete.clone() {
            let mut close_confirm = false;
            let mut do_delete = false;
            
            egui::Window::new("Confirm Delete")
                .collapsible(false)
                .resizable(false)
                .show(ctx, |ui| {
                    ui.vertical(|ui| {
                        ui.label(format!(
                            "Are you sure you want to delete \"{}\"?",
                            entry.path.file_name().and_then(|n| n.to_str()).unwrap_or("Unknown")
                        ));
                        if entry.is_dir {
                            ui.colored_label(egui::Color32::RED, "⚠ This will delete the folder and all its contents!");
                        }
                        ui.separator();
                        ui.horizontal(|ui| {
                            if ui.button("Yes, Delete").clicked() {
                                do_delete = true;
                            }
                            if ui.button("Cancel").clicked() {
                                close_confirm = true;
                            }
                        });
                    });
                });
            
            if do_delete {
                let path = entry.path.clone();
                let result = if entry.is_dir {
                    fs::remove_dir_all(&path)
                } else {
                    fs::remove_file(&path)
                };
                
                match result {
                    Ok(_) => {
                        self.entries.retain(|e| e.path != path);
                        self.filtered_entries.retain(|e| e.path != path);
                        self.home_index.retain(|e| e.entry.path != path);
                        self.home_results_cache.retain(|e| e.path != path);
                        self.local_index.retain(|e| e.entry.path != path);
                        self.local_results_cache.retain(|e| e.path != path);
                        self.size_cache.remove(&path);
                        
                        self.refresh();
                    }
                    Err(e) => {
                        self.error = Some(format!("Failed to delete: {}", e));
                    }
                }
                close_confirm = true;
            }
            
            if close_confirm {
                self.confirm_delete = None;
            }
        }
        
        if self.confirm_delete_multi {
            let mut close_confirm = false;
            let mut do_delete = false;
            let selected_count = self.selected_entries.len();
            
            egui::Window::new("Confirm Delete Multiple")
                .collapsible(false)
                .resizable(false)
                .show(ctx, |ui| {
                    ui.vertical(|ui| {
                        ui.label(format!(
                            "Are you sure you want to delete {} selected items?",
                            selected_count
                        ));
                        ui.colored_label(egui::Color32::RED, "⚠ This action cannot be undone!");
                        ui.add_space(8.0);
                        
                        egui::ScrollArea::vertical().max_height(150.0).show(ui, |ui| {
                            for (i, path) in self.selected_entries.iter().enumerate() {
                                if i >= 10 {
                                    ui.label(format!("... and {} more", selected_count - 10));
                                    break;
                                }
                                let name = path.file_name()
                                    .and_then(|n| n.to_str())
                                    .unwrap_or("Unknown");
                                ui.label(format!("• {}", name));
                            }
                        });
                        
                        ui.separator();
                        ui.horizontal(|ui| {
                            if ui.button("Yes, Delete All").clicked() {
                                do_delete = true;
                            }
                            if ui.button("Cancel").clicked() {
                                close_confirm = true;
                            }
                        });
                    });
                });
            
            if do_delete {
                let paths_to_delete: Vec<PathBuf> = self.selected_entries.iter().cloned().collect();
                let mut errors = Vec::new();
                
                for path in paths_to_delete {
                    let is_dir = path.is_dir();
                    let result = if is_dir {
                        fs::remove_dir_all(&path)
                    } else {
                        fs::remove_file(&path)
                    };
                    
                    match result {
                        Ok(_) => {
                            self.entries.retain(|e| e.path != path);
                            self.filtered_entries.retain(|e| e.path != path);
                            self.home_index.retain(|e| e.entry.path != path);
                            self.home_results_cache.retain(|e| e.path != path);
                            self.local_index.retain(|e| e.entry.path != path);
                            self.local_results_cache.retain(|e| e.path != path);
                            self.size_cache.remove(&path);
                        }
                        Err(e) => {
                            errors.push(format!("{}: {}", path.display(), e));
                        }
                    }
                }
                
                self.selected_entries.clear();
                
                if !errors.is_empty() {
                    self.error = Some(format!("Failed to delete some items:\n{}", errors.join("\n")));
                }
                
                self.refresh();
                close_confirm = true;
            }
            
            if close_confirm {
                self.confirm_delete_multi = false;
            }
        }
    }
}

fn human_readable_size(size: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut size = size as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{:.0} {}", size, UNITS[unit])
    } else {
        format!("{:.1} {}", size, UNITS[unit])
    }
}

fn grey_selectable_label(ui: &mut egui::Ui, selected: bool, text: impl Into<String>) -> egui::Response {
    let text = text.into();
    
    let old_bg = ui.visuals().selection.bg_fill;
    ui.visuals_mut().selection.bg_fill = egui::Color32::from_gray(60);
    
    let resp = ui.selectable_label(selected, &text);
    
    ui.visuals_mut().selection.bg_fill = old_bg;
    
    resp
}

fn format_timestamp(timestamp: u64) -> String {
    use chrono::{Local, TimeZone};
    let dt = Local.timestamp_opt(timestamp as i64, 0).single();
    match dt {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        None => "-".to_string(),
    }
}

fn open_entry(path: &Path) -> Result<(), String> {
    #[cfg(target_os = "windows")]
    {
        let parent = path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("C:\\"));
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|s| s.to_ascii_lowercase());

        match ext.as_deref() {
            Some("bat") | Some("cmd") => Command::new("cmd")
                .current_dir(parent)
                .arg("/C")
                .arg(path)
                .spawn()
                .map(|_| ())
                .map_err(|e| e.to_string()),
            Some("exe") | Some("com") => Command::new(path)
                .current_dir(parent)
                .spawn()
                .map(|_| ())
                .map_err(|e| e.to_string()),
            _ => open::that_detached(path).map_err(|e| e.to_string()),
        }
    }

    #[cfg(not(target_os = "windows"))]
    {
        open::that_detached(path).map_err(|e| e.to_string())
    }
}

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1200.0, 800.0]),
        ..Default::default()
    };
    
    eframe::run_native(
        "eXplorer",
        options,
        Box::new(|cc| {
            let mut fonts = egui::FontDefinitions::default();

            #[cfg(target_os = "windows")]
            {
                const FONT_CANDIDATES: &[(&str, &str)] = &[
                    ("LeelawUI", "C:\\Windows\\Fonts\\leelawui.ttf"),
                    ("Tahoma", "C:\\Windows\\Fonts\\tahoma.ttf"),
                    ("SegoeUI", "C:\\Windows\\Fonts\\segoeui.ttf"),
                ];

                let mut register_font = |label: String, data: Vec<u8>| {
                    if fonts.font_data.contains_key(&label) {
                        return;
                    }

                    fonts
                        .font_data
                        .insert(label.clone(), egui::FontData::from_owned(data));

                    let proportional = fonts
                        .families
                        .entry(egui::FontFamily::Proportional)
                        .or_default();
                    if !proportional.iter().any(|f| f == &label) {
                        proportional.insert(0, label.clone());
                    }

                    let monospace = fonts
                        .families
                        .entry(egui::FontFamily::Monospace)
                        .or_default();
                    if !monospace.iter().any(|f| f == &label) {
                        monospace.insert(0, label.clone());
                    }
                };

                for (label, path) in FONT_CANDIDATES {
                    if let Ok(data) = std::fs::read(path) {
                        register_font((*label).to_owned(), data);
                    }
                }

                if let Some(local_app_data) = std::env::var_os("LOCALAPPDATA") {
                    let mut user_fonts = PathBuf::from(local_app_data);
                    user_fonts.push("Microsoft");
                    user_fonts.push("Windows");
                    user_fonts.push("Fonts");

                    if let Ok(entries) = std::fs::read_dir(&user_fonts) {
                        for entry in entries.flatten() {
                            let path = entry.path();
                            if !path.is_file() {
                                continue;
                            }

                            let ext = path
                                .extension()
                                .and_then(|e| e.to_str())
                                .map(|s| s.to_ascii_lowercase());

                            match ext.as_deref() {
                                Some("ttf") | Some("ttc") | Some("otf") => {
                                    if let Ok(data) = std::fs::read(&path) {
                                        let label = entry
                                            .file_name()
                                            .to_string_lossy()
                                            .to_string();
                                        register_font(label, data);
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }

            cc.egui_ctx.set_fonts(fonts);

            Box::new(ExplorerApp::new())
        }),
    )
}
