import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
import os
import hashlib
import threading
import queue
import csv
from datetime import datetime
import platform
import subprocess
import shutil  # All imports at the top
import logging # All imports at the top

try:
    from send2trash import send2trash
    HAS_SEND2TRASH = True
except ImportError:
    HAS_SEND2TRASH = False

VERSION = "1.5.4" # Incremented for the last fix
STATUS_CLEAR_DELAY_MS = 5000 # 5 seconds
STATUS_ERROR_DELAY_MS = 10000 # 10 seconds
QUEUE_BATCH_PROCESS_LIMIT = 50 # Process this many queue items per cycle
QUEUE_POLL_INTERVAL_MS = 10 # Check the queue every 10ms

# List of hidden/junk files to ignore when checking if a folder is empty
JUNK_FILES = {'.ds_store', 'thumbs.db', 'desktop.ini'}

class FileManagementApp:
    def __init__(self, root):
        self.root = root
        self.root.title(f"File Management Toolkit v{VERSION}")
        self.root.geometry("1000x700")
        self.root.minsize(600, 400)

        # Style
        self.style = ttk.Style()
        self.style.theme_use('clam')
        self.style.configure("TNotebook.Tab", padding=[10, 5], font=('TkDefaultFont', 10, 'bold'))
        self.style.configure("Treeview.Heading", font=('TkDefaultFont', 10, 'bold'))
        self.style.configure("TButton", padding=[10, 5])
        self.style.configure("Big.TButton", padding=[10, 10], font=('TkDefaultFont', 10, 'bold'))
        self.style.configure("TLabel", padding=[5, 2])
        self.style.configure("TEntry", padding=[5, 5])
        self.style.configure("TFrame", padding=10)
        self.style.configure("Header.TLabel", font=('TkDefaultFont', 12, 'bold'))
        self.style.configure("Status.TLabel", padding=[10, 5])
        
        # Threading and Queue
        self.queue = queue.Queue()
        self.current_task = None

        # Main UI setup
        self.setup_ui()
        
        # Start the queue polling loop
        self.root.after(QUEUE_POLL_INTERVAL_MS, self.check_queue)

    def setup_ui(self):
        # Top frame for source directory
        self.top_frame = ttk.Frame(self.root, padding=10)
        self.top_frame.pack(fill=tk.X, side=tk.TOP)

        self.source_dir_label = ttk.Label(self.top_frame, text="Source Directory:")
        self.source_dir_label.pack(side=tk.LEFT, padx=(0, 5))

        self.source_dir_var = tk.StringVar()
        self.source_dir_entry = ttk.Entry(self.top_frame, textvariable=self.source_dir_var, font=('TkDefaultFont', 10))
        self.source_dir_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        self.browse_button = ttk.Button(self.top_frame, text="Browse...", command=self.browse_source_dir)
        self.browse_button.pack(side=tk.LEFT, padx=5)

        # --- Tabbed Interface ---
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=(5, 0))

        # Tab 1: Duplicate Cleaner
        self.dupe_tab = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.dupe_tab, text="Duplicate Cleaner")
        self.create_dupe_tab()

        # Tab 2: File Sorter
        self.sorter_tab = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.sorter_tab, text="File Sorter")
        self.create_sorter_tab()

        # Tab 3: File Collector
        self.collector_tab = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.collector_tab, text="File Collector")
        self.create_collector_tab()

        # Tab 4: File Finder
        self.finder_tab = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.finder_tab, text="File Finder")
        self.create_finder_tab()

        # Tab 5: Folder Analyzer
        self.analyzer_tab = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.analyzer_tab, text="Folder Analyzer")
        self.create_analyzer_tab()

        # --- Status Bar ---
        self.status_frame = ttk.Frame(self.root, relief=tk.SUNKEN, padding=0)
        self.status_frame.pack(fill=tk.X, side=tk.BOTTOM)
        
        self.status_var = tk.StringVar()
        self.status_var.set("Ready. Select a source directory to begin.")
        self.status_label = ttk.Label(self.status_frame, textvariable=self.status_var, style="Status.TLabel", anchor=tk.W)
        self.status_label.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        self.progress_bar = ttk.Progressbar(self.status_frame, mode='indeterminate', length=100)
        self.progress_bar.pack(side=tk.RIGHT, padx=5, pady=5)
        
        self._status_clear_job = None # For auto-clearing the status bar

    # --- Tab Creation Methods ---

    def create_dupe_tab(self):
        # Controls Frame
        controls_frame = ttk.Frame(self.dupe_tab)
        controls_frame.pack(fill=tk.X, pady=(0, 10))

        self.scan_button = ttk.Button(controls_frame, text="Scan for Duplicates", command=self.start_scan, style="Big.TButton")
        self.scan_button.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        
        self.empty_folder_button = ttk.Button(controls_frame, text="Delete Empty Folders", command=self.start_delete_empty_folders)
        self.empty_folder_button.pack(side=tk.LEFT, padx=5)

        self.cancel_button = ttk.Button(controls_frame, text="Cancel", command=self.cancel_task, state=tk.DISABLED)
        self.cancel_button.pack(side=tk.RIGHT, padx=5)

        # Options Frame
        options_frame = ttk.Frame(self.dupe_tab)
        options_frame.pack(fill=tk.X, pady=5)
        
        self.use_hash_var = tk.BooleanVar(value=False)
        self.use_hash_check = ttk.Checkbutton(options_frame, text="Confirm with SHA-256 (Slower, 100% Accurate)", variable=self.use_hash_var)
        self.use_hash_check.pack(side=tk.LEFT, padx=5)
        
        self.export_csv_var = tk.BooleanVar(value=False)
        self.export_csv_check = ttk.Checkbutton(options_frame, text="Export CSV report on completion", variable=self.export_csv_var)
        self.export_csv_check.pack(side=tk.LEFT, padx=5)

        # Results Frame
        results_frame = ttk.Frame(self.dupe_tab)
        results_frame.pack(fill=tk.BOTH, expand=True)
        
        cols = ("Set #", "File path", "Size", "Modified")
        self.dupe_tree = ttk.Treeview(results_frame, columns=cols, show="headings")
        
        for col in cols:
            self.dupe_tree.heading(col, text=col, command=lambda _col=col: self.sort_treeview(self.dupe_tree, _col, False))
            
        self.dupe_tree.column("Set #", width=80, anchor=tk.CENTER)
        self.dupe_tree.column("File path", width=600)
        self.dupe_tree.column("Size", width=100, anchor=tk.E)
        self.dupe_tree.column("Modified", width=150, anchor=tk.W)

        # Scrollbars
        ysb = ttk.Scrollbar(results_frame, orient=tk.VERTICAL, command=self.dupe_tree.yview)
        xsb = ttk.Scrollbar(results_frame, orient=tk.HORIZONTAL, command=self.dupe_tree.xview)
        self.dupe_tree.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)
        
        ysb.pack(side=tk.RIGHT, fill=tk.Y)
        xsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.dupe_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Right-click menu
        self.dupe_tree.bind("<Button-3>", self.dupe_show_context_menu)
        self.dupe_context_menu = tk.Menu(self.root, tearoff=0)
        self.dupe_context_menu.add_command(label="Open Containing Folder", command=self.dupe_open_folder)
        self.dupe_context_menu.add_command(label=f"Delete Selected (to Recycle Bin)", command=self.dupe_delete_selected)
        if not HAS_SEND2TRASH:
            self.dupe_context_menu.entryconfig(1, label="Delete Selected (PERMANENT)")

        # Auto-Delete Frame
        delete_frame = ttk.Frame(self.dupe_tab)
        delete_frame.pack(fill=tk.X, pady=(10, 0))
        
        self.delete_strategy_var = tk.StringVar(value="keep_newest")
        strategies = [
            ("Keep Newest File", "keep_newest"),
            ("Keep Oldest File", "keep_oldest"),
            ("Keep First-Found (by path)", "keep_first_found")
        ]
        
        for text, val in strategies:
            rb = ttk.Radiobutton(delete_frame, text=text, variable=self.delete_strategy_var, value=val)
            rb.pack(side=tk.LEFT, padx=10, fill=tk.X)
        
        self.auto_delete_button = ttk.Button(delete_frame, text="Apply Auto-Delete", state=tk.DISABLED, command=self.start_auto_delete, style="Big.TButton")
        self.auto_delete_button.pack(side=tk.RIGHT, padx=5, fill=tk.X, expand=True)


    def create_sorter_tab(self):
        # --- Controls Frame ---
        controls_frame = ttk.Frame(self.sorter_tab)
        controls_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(controls_frame, text="Sorting Strategy:").pack(side=tk.LEFT, padx=(0, 5))
        
        self.sorter_strategy_var = tk.StringVar(value="by_date")
        self.sorter_strategy_combo = ttk.Combobox(controls_frame, textvariable=self.sorter_strategy_var, state="readonly", width=40)
        self.sorter_strategy_combo['values'] = (
            "By Date (e.g., .../2023/12/file.jpg)",
            "By Extension (e.g., .../PDF/file.pdf)"
        )
        self.sorter_strategy_combo.pack(side=tk.LEFT, padx=5)

        self.sorter_preview_button = ttk.Button(controls_frame, text="Preview Sort", command=self.start_sorter_preview, style="Big.TButton")
        self.sorter_preview_button.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        
        self.sorter_process_button = ttk.Button(controls_frame, text="Move Files", command=self.start_sorter_process, state=tk.DISABLED, style="Big.TButton")
        self.sorter_process_button.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

        # --- Options Frame ---
        options_frame = ttk.Frame(self.sorter_tab)
        options_frame.pack(fill=tk.X, pady=5)
        
        self.sorter_copy_var = tk.BooleanVar(value=False)
        self.sorter_copy_check = ttk.Checkbutton(options_frame, text="Copy files (instead of move)", variable=self.sorter_copy_var)
        self.sorter_copy_check.pack(side=tk.LEFT, padx=5)

        # --- Results Frame ---
        results_frame = ttk.Frame(self.sorter_tab)
        results_frame.pack(fill=tk.BOTH, expand=True)
        
        cols = ("File", "Current Path", "New Path")
        self.sorter_tree = ttk.Treeview(results_frame, columns=cols, show="headings")
        
        for col in cols:
            self.sorter_tree.heading(col, text=col, command=lambda _col=col: self.sort_treeview(self.sorter_tree, _col, False))
            
        self.sorter_tree.column("File", width=200)
        self.sorter_tree.column("Current Path", width=350)
        self.sorter_tree.column("New Path", width=350)

        # Scrollbars
        ysb = ttk.Scrollbar(results_frame, orient=tk.VERTICAL, command=self.sorter_tree.yview)
        xsb = ttk.Scrollbar(results_frame, orient=tk.HORIZONTAL, command=self.sorter_tree.xview)
        self.sorter_tree.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)
        
        ysb.pack(side=tk.RIGHT, fill=tk.Y)
        xsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.sorter_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    def create_collector_tab(self):
        # --- Controls Frame ---
        controls_frame = ttk.Frame(self.collector_tab)
        controls_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(controls_frame, text="File Extensions (csv):").pack(side=tk.LEFT, padx=(0, 5))
        
        self.collector_ext_var = tk.StringVar(value="pdf, docx, pptx, xlsx")
        self.collector_ext_entry = ttk.Entry(controls_frame, textvariable=self.collector_ext_var, width=40)
        self.collector_ext_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)

        self.collector_preview_button = ttk.Button(controls_frame, text="Find Files", command=self.start_collector_preview, style="Big.TButton")
        self.collector_preview_button.pack(side=tk.LEFT, padx=5)
        
        self.collector_process_button = ttk.Button(controls_frame, text="Move Files to...", command=self.start_collector_process, state=tk.DISABLED, style="Big.TButton")
        self.collector_process_button.pack(side=tk.LEFT, padx=5)

        # --- Options Frame ---
        options_frame = ttk.Frame(self.collector_tab)
        options_frame.pack(fill=tk.X, pady=5)
        
        self.collector_copy_var = tk.BooleanVar(value=False)
        self.collector_copy_check = ttk.Checkbutton(options_frame, text="Copy files (instead of move)", variable=self.collector_copy_var)
        self.collector_copy_check.pack(side=tk.LEFT, padx=5)
        
        # --- Results Frame ---
        results_frame = ttk.Frame(self.collector_tab)
        results_frame.pack(fill=tk.BOTH, expand=True)
        
        cols = ("File", "Current Path")
        self.collector_tree = ttk.Treeview(results_frame, columns=cols, show="headings")
        
        for col in cols:
            self.collector_tree.heading(col, text=col, command=lambda _col=col: self.sort_treeview(self.collector_tree, _col, False))
            
        self.collector_tree.column("File", width=250)
        self.collector_tree.column("Current Path", width=650)

        # Scrollbars
        ysb = ttk.Scrollbar(results_frame, orient=tk.VERTICAL, command=self.collector_tree.yview)
        xsb = ttk.Scrollbar(results_frame, orient=tk.HORIZONTAL, command=self.collector_tree.xview)
        self.collector_tree.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)
        
        ysb.pack(side=tk.RIGHT, fill=tk.Y)
        xsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.collector_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    def create_finder_tab(self):
        # --- Filters Frame ---
        filters_frame = ttk.Frame(self.finder_tab)
        filters_frame.pack(fill=tk.X, pady=(0, 10))

        # --- Size Filter ---
        size_frame = ttk.Frame(filters_frame)
        size_frame.pack(fill=tk.X, pady=2)
        
        self.finder_size_check_var = tk.BooleanVar(value=False)
        self.finder_size_check = ttk.Checkbutton(size_frame, variable=self.finder_size_check_var, command=self.toggle_finder_filters)
        self.finder_size_check.pack(side=tk.LEFT)

        ttk.Label(size_frame, text="File size is").pack(side=tk.LEFT, padx=5)
        self.finder_size_op_var = tk.StringVar(value="greater than")
        self.finder_size_op_combo = ttk.Combobox(size_frame, textvariable=self.finder_size_op_var, values=["greater than", "less than"], width=12, state="disabled")
        self.finder_size_op_combo.pack(side=tk.LEFT, padx=5)
        
        self.finder_size_var = tk.StringVar(value="100")
        self.finder_size_entry = ttk.Entry(size_frame, textvariable=self.finder_size_var, width=8, state="disabled")
        self.finder_size_entry.pack(side=tk.LEFT, padx=5)
        
        self.finder_size_unit_var = tk.StringVar(value="MB")
        self.finder_size_unit_combo = ttk.Combobox(size_frame, textvariable=self.finder_size_unit_var, values=["KB", "MB", "GB"], width=5, state="disabled")
        self.finder_size_unit_combo.pack(side=tk.LEFT, padx=5)

        # --- Date Filter ---
        date_frame = ttk.Frame(filters_frame)
        date_frame.pack(fill=tk.X, pady=2)
        
        self.finder_date_check_var = tk.BooleanVar(value=False)
        self.finder_date_check = ttk.Checkbutton(date_frame, variable=self.finder_date_check_var, command=self.toggle_finder_filters)
        self.finder_date_check.pack(side=tk.LEFT)
        
        ttk.Label(date_frame, text="Date modified is").pack(side=tk.LEFT, padx=5)
        self.finder_date_op_var = tk.StringVar(value="before")
        self.finder_date_op_combo = ttk.Combobox(date_frame, textvariable=self.finder_date_op_var, values=["before", "after"], width=12, state="disabled")
        self.finder_date_op_combo.pack(side=tk.LEFT, padx=5)
        
        self.finder_date_var = tk.StringVar(value="YYYY-MM-DD")
        self.finder_date_entry = ttk.Entry(date_frame, textvariable=self.finder_date_var, width=12, state="disabled")
        self.finder_date_entry.pack(side=tk.LEFT, padx=5)
        
        # --- Extension Filter ---
        ext_frame = ttk.Frame(filters_frame)
        ext_frame.pack(fill=tk.X, pady=2)
        
        self.finder_ext_check_var = tk.BooleanVar(value=False)
        self.finder_ext_check = ttk.Checkbutton(ext_frame, variable=self.finder_ext_check_var, command=self.toggle_finder_filters)
        self.finder_ext_check.pack(side=tk.LEFT)
        
        ttk.Label(ext_frame, text="File extensions (csv):").pack(side=tk.LEFT, padx=5)
        self.finder_ext_var = tk.StringVar(value="tmp, log, bak, nfo, swp")
        self.finder_ext_entry = ttk.Entry(ext_frame, textvariable=self.finder_ext_var, width=40, state="disabled")
        self.finder_ext_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)

        # --- Controls Frame ---
        controls_frame = ttk.Frame(self.finder_tab)
        controls_frame.pack(fill=tk.X, pady=(10, 10))
        
        self.finder_preview_button = ttk.Button(controls_frame, text="Find Files", command=self.start_find_files, style="Big.TButton")
        self.finder_preview_button.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        
        # --- Results Frame ---
        results_frame = ttk.Frame(self.finder_tab)
        results_frame.pack(fill=tk.BOTH, expand=True)
        
        cols = ("File", "Folder", "Size", "Modified")
        self.finder_tree = ttk.Treeview(results_frame, columns=cols, show="headings", selectmode="extended")
        
        for col in cols:
            self.finder_tree.heading(col, text=col, command=lambda _col=col: self.sort_treeview(self.finder_tree, _col, False))
            
        self.finder_tree.column("File", width=250)
        self.finder_tree.column("Folder", width=400)
        self.finder_tree.column("Size", width=100, anchor=tk.E)
        self.finder_tree.column("Modified", width=150, anchor=tk.W)

        # Scrollbars
        ysb = ttk.Scrollbar(results_frame, orient=tk.VERTICAL, command=self.finder_tree.yview)
        xsb = ttk.Scrollbar(results_frame, orient=tk.HORIZONTAL, command=self.finder_tree.xview)
        self.finder_tree.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)
        
        ysb.pack(side=tk.RIGHT, fill=tk.Y)
        xsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.finder_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Right-click menu
        self.finder_tree.bind("<Button-3>", self.finder_show_context_menu)
        self.finder_context_menu = tk.Menu(self.root, tearoff=0)
        self.finder_context_menu.add_command(label="Open File", command=self.finder_open_file)
        self.finder_context_menu.add_command(label="Open Containing Folder", command=self.finder_open_folder)

        # --- Actions Frame ---
        actions_frame = ttk.Frame(self.finder_tab)
        actions_frame.pack(fill=tk.X, pady=(10, 0))
        
        self.finder_delete_button = ttk.Button(actions_frame, text=f"Delete Selected", state=tk.DISABLED, command=lambda: self.start_finder_action("delete"))
        self.finder_delete_button.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        if not HAS_SEND2TRASH:
            self.finder_delete_button.config(text="Delete Selected (PERMANENT)")
        
        self.finder_move_button = ttk.Button(actions_frame, text="Move Selected to...", state=tk.DISABLED, command=lambda: self.start_finder_action("move"))
        self.finder_move_button.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        
        self.finder_copy_button = ttk.Button(actions_frame, text="Copy Selected to...", state=tk.DISABLED, command=lambda: self.start_finder_action("copy"))
        self.finder_copy_button.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)

    def create_analyzer_tab(self):
        # --- Filters Frame ---
        filters_frame = ttk.Frame(self.analyzer_tab)
        filters_frame.pack(fill=tk.X, pady=(0, 10))

        # --- Size Filter ---
        size_frame = ttk.Frame(filters_frame)
        size_frame.pack(fill=tk.X, pady=2)
        
        self.analyzer_size_check_var = tk.BooleanVar(value=False)
        self.analyzer_size_check = ttk.Checkbutton(size_frame, variable=self.analyzer_size_check_var, command=self.toggle_analyzer_filters)
        self.analyzer_size_check.pack(side=tk.LEFT)

        ttk.Label(size_frame, text="Item size is").pack(side=tk.LEFT, padx=5)
        self.analyzer_size_op_var = tk.StringVar(value="greater than")
        self.analyzer_size_op_combo = ttk.Combobox(size_frame, textvariable=self.analyzer_size_op_var, values=["greater than", "less than"], width=12, state="disabled")
        self.analyzer_size_op_combo.pack(side=tk.LEFT, padx=5)
        
        self.analyzer_size_var = tk.StringVar(value="100")
        self.analyzer_size_entry = ttk.Entry(size_frame, textvariable=self.analyzer_size_var, width=8, state="disabled")
        self.analyzer_size_entry.pack(side=tk.LEFT, padx=5)
        
        self.analyzer_size_unit_var = tk.StringVar(value="MB")
        self.analyzer_size_unit_combo = ttk.Combobox(size_frame, textvariable=self.analyzer_size_unit_var, values=["KB", "MB", "GB"], width=5, state="disabled")
        self.analyzer_size_unit_combo.pack(side=tk.LEFT, padx=5)
        
        # --- Item Count Filter ---
        items_frame = ttk.Frame(filters_frame)
        items_frame.pack(fill=tk.X, pady=2)
        
        self.analyzer_items_check_var = tk.BooleanVar(value=False)
        self.analyzer_items_check = ttk.Checkbutton(items_frame, variable=self.analyzer_items_check_var, command=self.toggle_analyzer_filters)
        self.analyzer_items_check.pack(side=tk.LEFT)

        ttk.Label(items_frame, text="Item count is").pack(side=tk.LEFT, padx=5)
        self.analyzer_items_op_var = tk.StringVar(value="greater than")
        self.analyzer_items_op_combo = ttk.Combobox(items_frame, textvariable=self.analyzer_items_op_var, values=["greater than", "less than"], width=12, state="disabled")
        self.analyzer_items_op_combo.pack(side=tk.LEFT, padx=5)
        
        self.analyzer_items_var = tk.StringVar(value="500")
        self.analyzer_items_entry = ttk.Entry(items_frame, textvariable=self.analyzer_items_var, width=8, state="disabled")
        self.analyzer_items_entry.pack(side=tk.LEFT, padx=5)
        ttk.Label(items_frame, text="items (folders only)").pack(side=tk.LEFT, padx=5)


        # --- Controls Frame ---
        controls_frame = ttk.Frame(self.analyzer_tab)
        controls_frame.pack(fill=tk.X, pady=(5, 10)) # Added a bit of padding

        self.analyzer_scan_button = ttk.Button(controls_frame, text="Scan Folder Sizes", command=self.start_analyzer_scan, style="Big.TButton")
        self.analyzer_scan_button.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        
        self.analyzer_delete_button = ttk.Button(controls_frame, text="Delete Selected", command=self.start_analyzer_delete, state=tk.DISABLED, style="Big.TButton")
        self.analyzer_delete_button.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

        # --- Options Frame ---
        options_frame = ttk.Frame(self.analyzer_tab)
        options_frame.pack(fill=tk.X, pady=5)
        
        self.analyzer_include_files_var = tk.BooleanVar(value=False)
        self.analyzer_include_files_check = ttk.Checkbutton(options_frame, text="Include individual files in list (may be slow)", variable=self.analyzer_include_files_var)
        self.analyzer_include_files_check.pack(side=tk.LEFT, padx=5)

        # --- Results Frame ---
        results_frame = ttk.Frame(self.analyzer_tab)
        results_frame.pack(fill=tk.BOTH, expand=True)
        
        cols = ("Name", "Path", "Size", "Items")
        self.analyzer_tree = ttk.Treeview(results_frame, columns=cols, show="headings", selectmode="extended")
        
        for col in cols:
            self.analyzer_tree.heading(col, text=col, command=lambda _col=col: self.sort_treeview(self.analyzer_tree, _col, False))
            
        self.analyzer_tree.column("Name", width=250)
        self.analyzer_tree.column("Path", width=400)
        self.analyzer_tree.column("Size", width=100, anchor=tk.E)
        self.analyzer_tree.column("Items", width=100, anchor=tk.E)

        # Scrollbars
        ysb = ttk.Scrollbar(results_frame, orient=tk.VERTICAL, command=self.analyzer_tree.yview)
        xsb = ttk.Scrollbar(results_frame, orient=tk.HORIZONTAL, command=self.analyzer_tree.xview)
        self.analyzer_tree.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)
        
        ysb.pack(side=tk.RIGHT, fill=tk.Y)
        xsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.analyzer_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # --- Right-click menu ---
        self.analyzer_tree.bind("<Button-3>", self.analyzer_show_context_menu)
        self.analyzer_context_menu = tk.Menu(self.root, tearoff=0)
        self.analyzer_context_menu.add_command(label="Open Folder/File Location", command=self.analyzer_open_folder)


    # --- UI Helper Methods ---

    def toggle_finder_filters(self):
        """Enable/disable finder filter entry fields based on their checkboxes."""
        self.finder_size_op_combo.config(state=tk.NORMAL if self.finder_size_check_var.get() else tk.DISABLED)
        self.finder_size_entry.config(state=tk.NORMAL if self.finder_size_check_var.get() else tk.DISABLED)
        self.finder_size_unit_combo.config(state=tk.NORMAL if self.finder_size_check_var.get() else tk.DISABLED)
        
        self.finder_date_op_combo.config(state=tk.NORMAL if self.finder_date_check_var.get() else tk.DISABLED)
        self.finder_date_entry.config(state=tk.NORMAL if self.finder_date_check_var.get() else tk.DISABLED)
        
        self.finder_ext_entry.config(state=tk.NORMAL if self.finder_ext_check_var.get() else tk.DISABLED)

    def toggle_analyzer_filters(self):
        """Enable/disable analyzer filter entry fields based on their checkboxes."""
        self.analyzer_size_op_combo.config(state=tk.NORMAL if self.analyzer_size_check_var.get() else tk.DISABLED)
        self.analyzer_size_entry.config(state=tk.NORMAL if self.analyzer_size_check_var.get() else tk.DISABLED)
        self.analyzer_size_unit_combo.config(state=tk.NORMAL if self.analyzer_size_check_var.get() else tk.DISABLED)
        
        self.analyzer_items_op_combo.config(state=tk.NORMAL if self.analyzer_items_check_var.get() else tk.DISABLED)
        self.analyzer_items_entry.config(state=tk.NORMAL if self.analyzer_items_check_var.get() else tk.DISABLED)


    def browse_source_dir(self):
        """Open a dialog to select the source directory."""
        dir_path = filedialog.askdirectory()
        if dir_path:
            self.source_dir_var.set(dir_path)
            self.update_status("Ready. Start a scan or preview.")

    def update_status(self, message, clear_after=0):
        """Update the status bar, with an optional auto-clear timer."""
        if self._status_clear_job:
            self.root.after_cancel(self._status_clear_job)
            self._status_clear_job = None

        self.status_var.set(message)
        
        if "error" in message.lower():
            self.status_label.config(foreground="red")
            delay = STATUS_ERROR_DELAY_MS
        else:
            self.status_label.config(foreground="") # Reset to default color
            delay = clear_after if clear_after > 0 else STATUS_CLEAR_DELAY_MS
            
        if "complete" in message.lower() or "error" in message.lower() or "cancelled" in message.lower():
             self._status_clear_job = self.root.after(delay, self.clear_status)
    
    def clear_status(self):
        """Reset the status bar to 'Ready'."""
        self.status_var.set("Ready.")
        self.status_label.config(foreground="")
        self._status_clear_job = None
    
    def toggle_controls(self, scanning=False):
        """Disable or enable controls based on scanning state."""
        state = tk.DISABLED if scanning else tk.NORMAL
        
        # Top-level controls
        self.browse_button.config(state=state)
        self.source_dir_entry.config(state=state)
        
        # Duplicate Tab Controls
        self.scan_button.config(state=state)
        self.empty_folder_button.config(state=state)
        self.use_hash_check.config(state=state)
        self.export_csv_check.config(state=state)
        
        # Sorter Tab Controls
        self.sorter_preview_button.config(state=state)
        self.sorter_strategy_combo.config(state=state)
        self.sorter_copy_check.config(state=state)

        # Collector Tab Controls
        self.collector_preview_button.config(state=state)
        self.collector_ext_entry.config(state=state)
        self.collector_copy_check.config(state=state)
        
        # Finder Tab Controls
        self.finder_preview_button.config(state=state)
        self.finder_size_check.config(state=state)
        self.finder_date_check.config(state=state)
        self.finder_ext_check.config(state=state)
        # (Child widgets are handled by toggle_finder_filters)
        
        # Analyzer Tab Controls
        self.analyzer_scan_button.config(state=state)
        self.analyzer_include_files_check.config(state=state)
        self.analyzer_size_check.config(state=state)
        self.analyzer_items_check.config(state=state)
        # (Child widgets are handled by toggle_analyzer_filters)

        # Cancel Button
        self.cancel_button.config(state=tk.NORMAL if scanning else tk.DISABLED)
        
        # Progress Bar
        if scanning:
            self.progress_bar.start(10)
        else:
            self.progress_bar.stop()

    def start_task(self, logic_function, *args):
        """Generic task starter for threaded operations."""
        if self.current_task:
            messagebox.showwarning("Task in Progress", "Another task is already running. Please wait or cancel it.")
            return False
            
        source_dir = self.source_dir_var.get()
        if not source_dir or not os.path.isdir(source_dir):
            messagebox.showerror("Invalid Directory", "Please select a valid source directory.")
            return False
            
        self.toggle_controls(scanning=True)
        self.current_task = threading.Event()
        
        # Pass the event and directory as the first args to the logic function
        all_args = (self.current_task, source_dir) + args
        t = threading.Thread(target=logic_function, args=all_args, daemon=True)
        t.start()
        return True

    def cancel_task(self):
        """Signal the current running task to cancel."""
        if self.current_task:
            self.current_task.set() # Set the event flag
            self.update_status("Cancelling task...")
        
    def check_queue(self):
        """Poll the queue for messages from worker threads."""
        processed_count = 0
        is_done_or_error = False
        final_message = ""
        
        try:
            while processed_count < QUEUE_BATCH_PROCESS_LIMIT:
                msg = self.queue.get_nowait()
                processed_count += 1
                
                msg_type, data = msg
                
                if msg_type == "status":
                    self.update_status(data)
                
                # --- Batch Treeview Updates ---
                elif msg_type == "dupe_results_batch":
                    for item in data:
                        self.dupe_tree.insert("", tk.END, values=item)
                elif msg_type == "sorter_results_batch":
                    for item in data:
                        self.sorter_tree.insert("", tk.END, values=item)
                elif msg_type == "collector_results_batch":
                    for item in data:
                        self.collector_tree.insert("", tk.END, values=item)
                elif msg_type == "finder_results_batch":
                    for item in data:
                        self.finder_tree.insert("", tk.END, values=item)
                elif msg_type == "analyzer_results_batch":
                    for item in data:
                        self.analyzer_tree.insert("", tk.END, values=item)

                # --- Clear Treeviews ---
                elif msg_type == "clear_dupe_tree":
                    self.dupe_tree.delete(*self.dupe_tree.get_children())
                elif msg_type == "clear_sorter_tree":
                    self.sorter_tree.delete(*self.sorter_tree.get_children())
                elif msg_type == "clear_collector_tree":
                    self.collector_tree.delete(*self.collector_tree.get_children())
                elif msg_type == "clear_finder_tree":
                    self.finder_tree.delete(*self.finder_tree.get_children())
                elif msg_type == "clear_analyzer_tree":
                    self.analyzer_tree.delete(*self.analyzer_tree.get_children())

                # --- Remove Specific Items (post-action) ---
                elif msg_type == "remove_dupe_iids":
                    self.dupe_tree.delete(*data)
                elif msg_type == "remove_finder_items":
                    for iid in data:
                        try:
                            # This message is now shared by Finder and Analyzer
                            self.finder_tree.delete(iid)
                        except tk.TclError:
                            try:
                                self.analyzer_tree.delete(iid)
                            except tk.TclError:
                                pass # Item already gone from both

                # --- "Preview Done" messages (enables action buttons) ---
                elif msg_type == "dupe_scan_done":
                    message, file_count = data # Unpack (message, count)
                    if file_count > 0:
                        self.auto_delete_button.config(state=tk.NORMAL)
                    is_done_or_error = True
                    final_message = message

                elif msg_type == "sorter_preview_done":
                    message, file_count = data # Unpack (message, count)
                    if file_count > 0:
                        self.sorter_process_button.config(state=tk.NORMAL)
                    is_done_or_error = True
                    final_message = message
                
                elif msg_type == "collector_preview_done":
                    message, file_count = data # Unpack (message, count)
                    if file_count > 0:
                        self.collector_process_button.config(state=tk.NORMAL)
                    is_done_or_error = True
                    final_message = message
                
                elif msg_type == "finder_preview_done":
                    message, file_count = data # Unpack (message, count)
                    if file_count > 0:
                        # Enable action buttons
                        self.finder_delete_button.config(state=tk.NORMAL)
                        self.finder_move_button.config(state=tk.NORMAL)
                        self.finder_copy_button.config(state=tk.NORMAL)
                    is_done_or_error = True
                    final_message = message

                elif msg_type == "analyzer_scan_done":
                    message, item_count = data # Unpack (message, count)
                    if item_count > 0:
                        self.analyzer_delete_button.config(state=tk.NORMAL)
                        # Sort by size by default
                        self.sort_treeview(self.analyzer_tree, "Size", True)
                    is_done_or_error = True
                    final_message = message
                
                # --- FIXED: Re-enable buttons based on remaining items ---
                elif msg_type == "finder_action_done":
                    message, remaining_count = data
                    if remaining_count > 0:
                        self.finder_delete_button.config(state=tk.NORMAL)
                        self.finder_move_button.config(state=tk.NORMAL)
                        self.finder_copy_button.config(state=tk.NORMAL)
                    is_done_or_error = True
                    final_message = message
                
                elif msg_type == "analyzer_action_done":
                    message, remaining_count = data
                    if remaining_count > 0:
                        self.analyzer_delete_button.config(state=tk.NORMAL)
                    is_done_or_error = True
                    final_message = message
                # --- END FIX ---

                # --- Final "Done" or "Error" messages ---
                elif msg_type == "done":
                    is_done_or_error = True
                    final_message = data
                
                elif msg_type == "error":
                    is_done_or_error = True
                    final_message = data
                
                elif msg_type == "cancelled":
                    is_done_or_error = True
                    final_message = "Task cancelled by user."

        except queue.Empty:
            pass # No more messages in queue
            
        except Exception as e:
            # This is a safety catch for the queue processor itself
            is_done_or_error = True
            final_message = f"Critical UI Error: {e}"
            self.logger.exception("Critical error in check_queue")

        # If a task finished, update state
        if self.current_task and is_done_or_error:
            self.toggle_controls(scanning=False) # Re-enable scan buttons
            self.current_task = None
            self.update_status(final_message) # Show final message
            
        # Reschedule the queue check
        self.root.after(QUEUE_POLL_INTERVAL_MS, self.check_queue)


    # --- ============================= ---
    # --- Duplicate Cleaner Methods ---
    # --- ============================= ---
    
    def start_scan(self):
        """Start the duplicate file scan."""
        # Disable button *before* starting task
        self.auto_delete_button.config(state=tk.DISABLED) 
        self.queue.put(("clear_dupe_tree", None))
        self.update_status("Starting scan...")
        use_hash = self.use_hash_var.get()
        export_csv = self.export_csv_var.get()
        if self.start_task(self.scan_logic, use_hash, export_csv):
            self.start_time = datetime.now() # Set start time
            self.update_status("Scanning for duplicates...")
        
    def scan_logic(self, cancel_event, source_dir, use_hash, export_csv):
        """Worker thread logic for finding duplicate files."""
        try:
            files_by_size = {}
            files_by_mod_time = {}
            potential_dupes = []
            final_dupe_sets = []
            
            # --- Pass 1: Group by size ---
            self.queue.put(("status", "Scanning files and grouping by size..."))
            for root, dirs, files in os.walk(source_dir):
                if cancel_event.is_set():
                    self.queue.put(("cancelled", None))
                    return
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        file_size = os.path.getsize(file_path)
                        if file_size < 1: # Skip empty files
                            continue
                        
                        if file_size in files_by_size:
                            files_by_size[file_size].append(file_path)
                        else:
                            files_by_size[file_size] = [file_path]
                    except (IOError, OSError):
                        continue
            
            # Filter groups with more than one file
            groups_to_check = {size: paths for size, paths in files_by_size.items() if len(paths) > 1}
            
            if not groups_to_check:
                self.queue.put(("dupe_scan_done", ("Scan complete. No potential duplicates found.", 0)))
                return

            # --- Pass 2: Group by Mod Time (Fast Check) ---
            self.queue.put(("status", "Comparing modification times..."))
            if not use_hash:
                for size, paths in groups_to_check.items():
                    if cancel_event.is_set():
                        self.queue.put(("cancelled", None))
                        return
                        
                    files_by_mod_time.clear()
                    for path in paths:
                        try:
                            mod_time = os.path.getmtime(path)
                            if mod_time in files_by_mod_time:
                                files_by_mod_time[mod_time].append(path)
                            else:
                                files_by_mod_time[mod_time] = [path]
                        except (IOError, OSError):
                            continue
                    
                    for mod_time, dupe_paths in files_by_mod_time.items():
                        if len(dupe_paths) > 1:
                            potential_dupes.append(dupe_paths)
                
                final_dupe_sets = potential_dupes # With this method, potential is final
            
            # --- Pass 3: Group by Hash (Slow Check) ---
            else:
                self.queue.put(("status", f"Hashing {len(groups_to_check)} potential groups..."))
                hashes = {}
                checked_count = 0
                total_to_check = len(groups_to_check)

                for size, paths in groups_to_check.items():
                    if cancel_event.is_set():
                        self.queue.put(("cancelled", None))
                        return
                    
                    checked_count += 1
                    if checked_count % 10 == 0:
                         self.queue.put(("status", f"Hashing group {checked_count}/{total_to_check}..."))

                    hashes.clear()
                    for path in paths:
                        try:
                            file_hash = self.hash_file(path)
                            if file_hash in hashes:
                                hashes[file_hash].append(path)
                            else:
                                hashes[file_hash] = [path]
                        except (IOError, OSError):
                            continue
                    
                    for file_hash, dupe_paths in hashes.items():
                        if len(dupe_paths) > 1:
                            final_dupe_sets.append(dupe_paths)

            # --- Processing Results ---
            self.queue.put(("status", "Scan complete. Populating results..."))
            total_wasted = 0
            results_batch = []
            
            for i, dupe_set in enumerate(final_dupe_sets):
                set_id = f"Set {i+1}"
                # Get file info for sorting
                files_with_info = []
                for path in dupe_set:
                    try:
                        stat = os.stat(path)
                        files_with_info.append((path, stat.st_size, stat.st_mtime))
                    except (IOError, OSError):
                        continue
                
                # Sort by path by default to keep it consistent
                files_with_info.sort(key=lambda x: x[0])
                
                total_wasted += files_with_info[0][1] * (len(files_with_info) - 1)
                
                for path, size, mtime in files_with_info:
                    mod_time_str = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
                    size_str = self.format_size(size)
                    results_batch.append((set_id, path, size_str, mod_time_str))
                
                if len(results_batch) >= 100: # Send batch to UI
                    self.queue.put(("dupe_results_batch", results_batch))
                    results_batch = []

            if results_batch: # Send final batch
                self.queue.put(("dupe_results_batch", results_batch))

            if export_csv and final_dupe_sets:
                self.export_csv_report(final_dupe_sets, source_dir)
            
            elapsed = (datetime.now() - self.start_time).total_seconds()
            stats_msg = f"Scan complete in {elapsed:.2f}s. Found {len(final_dupe_sets)} duplicate sets. Wasted space ≈ {self.format_size(total_wasted)}"
            self.queue.put(("dupe_scan_done", (stats_msg, len(final_dupe_sets))))

        except Exception as e:
            self.logger.exception("Error in scan_logic")
            self.queue.put(("error", f"An error occurred during scan: {e}"))

    def start_auto_delete(self):
        """Start the auto-delete process based on the selected strategy."""
        strategy = self.delete_strategy_var.get()
        
        # Get all item IDs from the tree
        all_iids = self.dupe_tree.get_children()
        if not all_iids:
            messagebox.showinfo("Nothing to Delete", "No duplicates found in the list.")
            return
            
        # Group files by Set #
        files_by_set = {}
        for iid in all_iids:
            values = self.dupe_tree.item(iid, 'values')
            set_id = values[0]
            path = values[1]
            mtime_str = values[3]
            mtime = datetime.strptime(mtime_str, '%Y-%m-%d %H:%M:%S').timestamp()
            
            if set_id not in files_by_set:
                files_by_set[set_id] = []
            files_by_set[set_id].append({'iid': iid, 'path': path, 'mtime': mtime})
            
        self.update_status(f"Applying auto-delete strategy: {strategy}...")
        if self.start_task(self.auto_delete_logic, files_by_set, strategy):
            self.update_status(f"Auto-deleting files...")
            self.auto_delete_button.config(state=tk.DISABLED)

    def auto_delete_logic(self, cancel_event, source_dir, files_by_set, strategy):
        """Worker thread logic for auto-deleting files."""
        try:
            files_to_delete = []
            iids_to_remove = []
            
            for set_id, files in files_by_set.items():
                if cancel_event.is_set():
                    self.queue.put(("cancelled", None))
                    return
                    
                if not files:
                    continue
                
                # Determine which file to keep
                if strategy == "keep_newest":
                    files.sort(key=lambda x: x['mtime'], reverse=True)
                elif strategy == "keep_oldest":
                    files.sort(key=lambda x: x['mtime'])
                elif strategy == "keep_first_found":
                    files.sort(key=lambda x: x['path'])
                
                # The first file is kept, the rest are marked for deletion
                files_to_delete.extend([f['path'] for f in files[1:]])
                iids_to_remove.extend([f['iid'] for f in files[1:]])
            
            if not files_to_delete:
                self.queue.put(("done", "Auto-delete complete. No files needed deletion."))
                return

            # Perform deletion
            deleted_count = 0
            failed_count = 0
            total_to_delete = len(files_to_delete)
            
            for i, path in enumerate(files_to_delete):
                if cancel_event.is_set():
                    self.queue.put(("cancelled", None))
                    return
                
                self.queue.put(("status", f"Deleting {i+1}/{total_to_delete}: {os.path.basename(path)}"))
                if self.safe_delete(path):
                    deleted_count += 1
                else:
                    failed_count += 1
            
            # Send UI update to remove deleted items
            self.queue.put(("remove_dupe_iids", iids_to_remove))
            
            msg = f"Auto-delete complete. Deleted {deleted_count} files."
            if failed_count > 0:
                msg += f" Failed to delete {failed_count} files (see console for errors)."
            self.queue.put(("done", msg))

        except Exception as e:
            self.logger.exception("Error in auto_delete_logic")
            self.queue.put(("error", f"An error occurred during auto-delete: {e}"))

    def start_delete_empty_folders(self):
        """Start the task to find and delete empty subfolders."""
        if not messagebox.askyesno("Confirm Delete", f"Are you sure you want to find and delete all empty subfolders in '{self.source_dir_var.get()}'? This will use the Recycle Bin if possible."):
            return
            
        self.update_status("Scanning for empty folders...")
        if self.start_task(self.delete_empty_folders_logic):
            self.update_status("Deleting empty folders...")

    def delete_empty_folders_logic(self, cancel_event, source_dir):
        """Worker thread logic to find and delete empty folders from the bottom up."""
        try:
            deleted_folders = 0
            deleted_files = 0
            
            # Walk from the bottom up
            for root, dirs, files in os.walk(source_dir, topdown=False):
                if cancel_event.is_set():
                    self.queue.put(("cancelled", None))
                    return
                
                # Skip the root source directory itself
                if root == source_dir:
                    continue

                # Check files in directory
                is_empty = True
                junk_in_folder = []
                
                try:
                    for filename in os.listdir(root):
                        if filename.lower() in JUNK_FILES:
                            junk_in_folder.append(os.path.join(root, filename))
                        else:
                            is_empty = False # Found a real file or folder
                            break
                except (IOError, OSError) as e:
                    self.logger.warning(f"Could not access {root}: {e}")
                    continue

                if is_empty:
                    self.queue.put(("status", f"Found empty folder: {root}"))
                    # First, delete any junk files inside
                    for junk_path in junk_in_folder:
                        if self.safe_delete(junk_path):
                            deleted_files += 1
                        else:
                            # If we can't delete the junk file, we can't delete the folder
                            is_empty = False 
                            break
                    
                    # If it's still considered empty (i.e., we deleted all junk)
                    if is_empty:
                        try:
                            # Use safe_delete for the folder itself
                            if self.safe_delete(root):
                                deleted_folders += 1
                            else:
                                self.logger.warning(f"Failed to delete folder (safe_delete): {root}")
                        except (IOError, OSError) as e:
                            self.logger.error(f"Error deleting folder {root}: {e}")

            msg = f"Empty folder cleanup complete. Deleted {deleted_folders} folders"
            if deleted_files > 0:
                msg += f" and {deleted_files} hidden/junk files."
            self.queue.put(("done", msg))

        except Exception as e:
            self.logger.exception("Error in delete_empty_folders_logic")
            self.queue.put(("error", f"An error occurred during empty folder deletion: {e}"))

    # --- ============================= ---
    # --- File Sorter Methods ---
    # --- ============================= ---

    def start_sorter_preview(self):
        """Start the file sorter preview."""
        # Disable button *before* starting task
        self.sorter_process_button.config(state=tk.DISABLED)
        self.queue.put(("clear_sorter_tree", None))
        self.update_status("Starting sort preview...")
        strategy = self.sorter_strategy_var.get()
        if not strategy:
            messagebox.showerror("No Strategy", "Please select a sorting strategy.")
            self.toggle_controls(scanning=False) # Manually reset
            return
            
        if self.start_task(self.sorter_preview_logic, strategy):
            self.update_status(f"Previewing sort: {strategy}...")
            
    def sorter_preview_logic(self, cancel_event, source_dir, strategy):
        """Worker thread logic for previewing file sorting."""
        try:
            results = []
            # Define output folder names to avoid scanning them
            date_output_dir = os.path.join(source_dir, "Sorted by Date")
            ext_output_dir = os.path.join(source_dir, "Sorted by Extension")
            
            for root, dirs, files in os.walk(source_dir):
                if cancel_event.is_set():
                    self.queue.put(("cancelled", None))
                    return

                # --- Skip our own output directories ---
                if root.startswith(date_output_dir) or root.startswith(ext_output_dir):
                    dirs[:] = [] # Don't recurse into these
                    continue
                
                # Batch results for UI
                results_batch = []
                
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        if strategy == "By Date (e.g., .../2023/12/file.jpg)":
                            stat = os.stat(file_path)
                            mtime = datetime.fromtimestamp(stat.st_mtime)
                            year = mtime.strftime("%Y")
                            month = mtime.strftime("%m (%B)")
                            new_dir = os.path.join(date_output_dir, year, month)
                            new_path = os.path.join(new_dir, file)
                            
                        elif strategy == "By Extension (e.g., .../PDF/file.pdf)":
                            ext = os.path.splitext(file)[1]
                            if not ext:
                                ext_name = "No Extension"
                            else:
                                ext_name = ext[1:].upper() # "PDF"
                            
                            new_dir = os.path.join(ext_output_dir, ext_name)
                            new_path = os.path.join(new_dir, file)
                        
                        else:
                            continue # Should not happen

                        results.append((file, root, new_dir, new_path))
                        results_batch.append((file, root, new_path))

                    except (IOError, OSError) as e:
                        self.logger.warning(f"Could not stat file {file_path}: {e}")
                        continue
                
                if results_batch:
                    self.queue.put(("sorter_results_batch", results_batch))
                    results_batch = [] # Clear batch
            
            count = len(results)
            self.queue.put(("sorter_preview_done", (f"Preview complete. {count} files to process.", count)))

        except Exception as e:
            self.logger.exception("Error in sorter_preview_logic")
            self.queue.put(("error", f"An error occurred during preview: {e}"))

    def start_sorter_process(self):
        """Start the file sorting (move/copy) process."""
        if not self.sorter_tree.get_children():
            messagebox.showinfo("Nothing to Process", "No files found in the preview list.")
            return

        is_copy = self.sorter_copy_var.get()
        action_verb = "copy" if is_copy else "move"
        
        if not messagebox.askyesno("Confirm Action", f"Are you sure you want to {action_verb} all {len(self.sorter_tree.get_children())} files?"):
            return
        
        # Get all data from the tree
        plan = []
        for iid in self.sorter_tree.get_children():
            values = self.sorter_tree.item(iid, 'values')
            file, current_path, new_path = values
            # Reconstruct the full old path
            old_path = os.path.join(current_path, file)
            plan.append((old_path, new_path))
        
        if self.start_task(self.sorter_process_logic, plan, is_copy):
            self.update_status(f"Processing {len(plan)} files...")
            self.sorter_process_button.config(state=tk.DISABLED)

    def sorter_process_logic(self, cancel_event, source_dir, plan, is_copy):
        """Worker thread logic for sorting (move/copy) files."""
        try:
            processed_count = 0
            failed_count = 0
            total_count = len(plan)
            action_verb = "Copying" if is_copy else "Moving"
            action_verb_past = "Copied" if is_copy else "Moved"
            
            for old_path, new_path in plan:
                if cancel_event.is_set():
                    self.queue.put(("cancelled", None))
                    return
                
                self.queue.put(("status", f"{action_verb} {processed_count+1}/{total_count}: {os.path.basename(old_path)}"))
                
                try:
                    # Create destination directory
                    new_dir = os.path.dirname(new_path)
                    if not os.path.exists(new_dir):
                        os.makedirs(new_dir)
                    
                    # Handle filename conflicts
                    final_new_path = self.get_unique_filename(new_path)
                    
                    if is_copy:
                        shutil.copy2(old_path, final_new_path)
                    else:
                        shutil.move(old_path, final_new_path)
                    
                    processed_count += 1
                
                except (IOError, OSError, shutil.Error) as e:
                    self.logger.warning(f"Failed to {action_verb.lower()} {old_path} to {new_path}: {e}")
                    failed_count += 1
            
            self.queue.put(("clear_sorter_tree", None))
            msg = f"Process complete. {action_verb_past} {processed_count} files."
            if failed_count > 0:
                msg += f" Failed to process {failed_count} files (see console)."
            self.queue.put(("done", msg))

        except Exception as e:
            self.logger.exception("Error in sorter_process_logic")
            self.queue.put(("error", f"An error occurred during processing: {e}"))

    # --- ============================= ---
    # --- File Collector Methods ---
    # --- ============================= ---
    
    def start_collector_preview(self):
        """Start the file collector preview."""
        # Disable button *before* starting task
        self.collector_process_button.config(state=tk.DISABLED)
        self.queue.put(("clear_collector_tree", None))
        self.update_status("Starting file search...")
        
        ext_str = self.collector_ext_var.get()
        if not ext_str:
            messagebox.showerror("No Extensions", "Please enter one or more file extensions (e.g., pdf, jpg).")
            self.toggle_controls(scanning=False) # Manually reset
            return

        try:
            extensions = {f".{ext.strip().lower()}" for ext in ext_str.split(',') if ext.strip()}
        except Exception as e:
            messagebox.showerror("Invalid Input", f"Could not parse extensions: {e}")
            self.toggle_controls(scanning=False) # Manually reset
            return
            
        if not extensions:
            messagebox.showerror("No Extensions", "Please enter valid file extensions.")
            self.toggle_controls(scanning=False) # Manually reset
            return

        if self.start_task(self.collector_preview_logic, extensions):
            self.update_status(f"Searching for files: {', '.join(extensions)}...")
            
    def collector_preview_logic(self, cancel_event, source_dir, extensions):
        """Worker thread logic for collecting files by extension."""
        try:
            results_batch = []
            count = 0
            
            for root, dirs, files in os.walk(source_dir):
                if cancel_event.is_set():
                    self.queue.put(("cancelled", None))
                    return

                for file in files:
                    try:
                        ext = os.path.splitext(file)[1].lower()
                        if ext in extensions:
                            results_batch.append((file, root))
                            count += 1
                            
                            if len(results_batch) >= 100:
                                self.queue.put(("collector_results_batch", results_batch))
                                results_batch = []
                                
                    except (IOError, OSError) as e:
                        self.logger.warning(f"Could not access file {file}: {e}")
                        continue
                
            if results_batch: # Send final batch
                self.queue.put(("collector_results_batch", results_batch))
            
            self.queue.put(("collector_preview_done", (f"Preview complete. {count} files to process.", count)))

        except Exception as e:
            self.logger.exception("Error in collector_preview_logic")
            self.queue.put(("error", f"An error occurred during preview: {e}"))
    
    def start_collector_process(self):
        """Start the file collector (move/copy) process."""
        if not self.collector_tree.get_children():
            messagebox.showinfo("Nothing to Process", "No files found in the preview list.")
            return
            
        target_dir = filedialog.askdirectory(title="Select Target Folder to Move/Copy Files Into")
        if not target_dir:
            return
            
        source_dir = self.source_dir_var.get()
        if os.path.commonpath([source_dir, target_dir]) == source_dir:
            if not messagebox.askyesno("Confirm Target", "The target folder is inside your source directory. This is usually fine, but are you sure?"):
                return

        is_copy = self.collector_copy_var.get()
        action_verb = "copy" if is_copy else "move"
        
        if not messagebox.askyesno("Confirm Action", f"Are you sure you want to {action_verb} all {len(self.collector_tree.get_children())} files into '{target_dir}'?"):
            return
        
        # Get all data from the tree
        plan = []
        for iid in self.collector_tree.get_children():
            values = self.collector_tree.item(iid, 'values')
            file, current_path = values
            old_path = os.path.join(current_path, file)
            new_path = os.path.join(target_dir, file)
            plan.append((old_path, new_path))
        
        if self.start_task(self.collector_process_logic, plan, is_copy):
            self.update_status(f"Processing {len(plan)} files...")
            self.collector_process_button.config(state=tk.DISABLED)

    def collector_process_logic(self, cancel_event, source_dir, plan, is_copy):
        """Worker thread logic for collecting (move/copy) files."""
        try:
            processed_count = 0
            failed_count = 0
            total_count = len(plan)
            action_verb = "Copying" if is_copy else "Moving"
            action_verb_past = "Copied" if is_copy else "Moved"
            
            target_dir = os.path.dirname(plan[0][1])
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)
            
            for old_path, new_path in plan:
                if cancel_event.is_set():
                    self.queue.put(("cancelled", None))
                    return
                
                self.queue.put(("status", f"{action_verb} {processed_count+1}/{total_count}: {os.path.basename(old_path)}"))
                
                try:
                    # Handle filename conflicts
                    final_new_path = self.get_unique_filename(new_path)
                    
                    if is_copy:
                        shutil.copy2(old_path, final_new_path)
                    else:
                        shutil.move(old_path, final_new_path)
                    
                    processed_count += 1
                
                except (IOError, OSError, shutil.Error) as e:
                    self.logger.warning(f"Failed to {action_verb.lower()} {old_path} to {new_path}: {e}")
                    failed_count += 1
            
            self.queue.put(("clear_collector_tree", None))
            msg = f"Process complete. {action_verb_past} {processed_count} files."
            if failed_count > 0:
                msg += f" Failed to process {failed_count} files (see console)."
            self.queue.put(("done", msg))

        except Exception as e:
            self.logger.exception("Error in collector_process_logic")
            self.queue.put(("error", f"An error occurred during processing: {e}"))

    # --- ============================= ---
    # --- File Finder Methods ---
    # --- ============================= ---

    def start_find_files(self):
        """Start the file finder scan based on filters."""
        # Disable buttons *before* starting task
        self.finder_delete_button.config(state=tk.DISABLED)
        self.finder_move_button.config(state=tk.DISABLED)
        self.finder_copy_button.config(state=tk.DISABLED)
        self.queue.put(("clear_finder_tree", None))
        self.update_status("Starting file search...")
        
        filters = {}
        
        try:
            if self.finder_size_check_var.get():
                size = float(self.finder_size_var.get())
                unit = self.finder_size_unit_var.get()
                op = self.finder_size_op_var.get()
                
                multiplier = {"KB": 1024, "MB": 1024**2, "GB": 1024**3}
                size_bytes = size * multiplier[unit]
                filters['size'] = (op, size_bytes)
            
            if self.finder_date_check_var.get():
                date_str = self.finder_date_var.get()
                op = self.finder_date_op_var.get()
                try:
                    timestamp = datetime.strptime(date_str, "%Y-%m-%d").timestamp()
                    filters['date'] = (op, timestamp)
                except ValueError:
                    messagebox.showerror("Invalid Date", "Date must be in YYYY-MM-DD format.")
                    self.toggle_controls(scanning=False)
                    return

            if self.finder_ext_check_var.get():
                ext_str = self.finder_ext_var.get()
                if not ext_str:
                     raise ValueError("Extensions filter is enabled but no extensions are listed.")
                extensions = {f".{ext.strip().lower()}" for ext in ext_str.split(',') if ext.strip()}
                if extensions:
                    filters['ext'] = extensions
                else:
                    raise ValueError("Could not parse any valid extensions.")
            
            if not filters:
                messagebox.showerror("No Filters", "Please enable at least one filter to start the search.")
                self.toggle_controls(scanning=False)
                return

        except Exception as e:
            messagebox.showerror("Invalid Filter", f"Error in filter settings: {e}")
            self.toggle_controls(scanning=False)
            return

        if self.start_task(self.find_files_logic, filters):
            self.update_status(f"Searching for files...")
            
    def find_files_logic(self, cancel_event, source_dir, filters):
        """Worker thread logic for finding files by metadata filters."""
        try:
            results_batch = []
            count = 0
            
            for root, dirs, files in os.walk(source_dir):
                if cancel_event.is_set():
                    self.queue.put(("cancelled", None))
                    return

                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        # --- Apply Filters ---
                        stat = None
                        
                        # --- CORRECTED FILTER LOGIC ---
                        if 'size' in filters:
                            op, size_bytes = filters['size']
                            stat = os.stat(file_path)
                            if op == "greater than":
                                if not stat.st_size > size_bytes: continue
                            elif op == "less than":
                                if not stat.st_size < size_bytes: continue
                        
                        if 'date' in filters:
                            op, timestamp = filters['date']
                            if not stat: stat = os.stat(file_path)
                            if op == "before":
                                if not stat.st_mtime < timestamp: continue
                            elif op == "after":
                                if not stat.st_mtime > timestamp: continue
                        
                        if 'ext' in filters:
                            ext = os.path.splitext(file)[1].lower()
                            if ext not in filters['ext']:
                                continue
                        # --- END CORRECTED FILTER LOGIC ---
                        
                        # --- If all filters passed ---
                        if not stat: stat = os.stat(file_path)
                        
                        size_str = self.format_size(stat.st_size)
                        mod_str = datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                        
                        results_batch.append((file, root, size_str, mod_str))
                        count += 1
                        
                        if len(results_batch) >= 100:
                            self.queue.put(("finder_results_batch", results_batch))
                            results_batch = []
                                
                    except (IOError, OSError) as e:
                        self.logger.warning(f"Could not access file {file}: {e}")
                        continue
                
            if results_batch: # Send final batch
                self.queue.put(("finder_results_batch", results_batch))
            
            self.queue.put(("finder_preview_done", (f"Scan complete. Found {count} matching files.", count)))
            
        except Exception as e:
            self.logger.exception("Error in find_files_logic")
            self.queue.put(("error", f"An error occurred during scan: {e}"))

    def start_finder_action(self, action):
        """Start an action (delete, move, copy) on selected files in the finder."""
        selected_iids = self.finder_tree.selection()
        if not selected_iids:
            messagebox.showinfo("No Files Selected", "Please select one or more files from the list.")
            return

        target_dir = None
        if action == "move" or action == "copy":
            target_dir = filedialog.askdirectory(title=f"Select Target Folder to {action.title()} Files Into")
            if not target_dir:
                return # User cancelled
            
            source_dir = self.source_dir_var.get()
            if os.path.commonpath([source_dir, target_dir]) == source_dir and action == "move":
                if not messagebox.askyesno("Confirm Move", "Your target folder is inside the source directory. Moving files into it might cause confusion. Continue?"):
                    return
        
        if not messagebox.askyesno("Confirm Action", f"Are you sure you want to {action} {len(selected_iids)} selected files?"):
            return
        
        # Get all data from the tree
        plan = []
        for iid in selected_iids:
            values = self.finder_tree.item(iid, 'values')
            file, folder, _, _ = values
            old_path = os.path.join(folder, file)
            plan.append(old_path)
        
        if self.start_task(self.finder_action_logic, action, plan, target_dir, selected_iids):
            self.update_status(f"Processing {len(plan)} files...")
            # Disable all action buttons during processing
            self.finder_delete_button.config(state=tk.DISABLED)
            self.finder_move_button.config(state=tk.DISABLED)
            self.finder_copy_button.config(state=tk.DISABLED)

    def finder_action_logic(self, cancel_event, source_dir, action, plan, target_dir, iids_to_remove):
        """Worker thread logic for finder actions (delete, move, copy)."""
        try:
            processed_count = 0
            failed_count = 0
            total_count = len(plan)
            
            if target_dir and not os.path.exists(target_dir):
                os.makedirs(target_dir)
            
            for i, old_path in enumerate(plan):
                if cancel_event.is_set():
                    self.queue.put(("cancelled", None))
                    return
                
                self.queue.put(("status", f"Processing {i+1}/{total_count}: {os.path.basename(old_path)}"))
                
                try:
                    if action == "delete":
                        if not self.safe_delete(old_path):
                            raise IOError(f"safe_delete failed for {old_path}")
                    
                    elif action == "move" or action == "copy":
                        new_path = os.path.join(target_dir, os.path.basename(old_path))
                        final_new_path = self.get_unique_filename(new_path)
                        
                        if action == "move":
                            shutil.move(old_path, final_new_path)
                        else: # copy
                            shutil.copy2(old_path, final_new_path)
                    
                    processed_count += 1
                
                except (IOError, OSError, shutil.Error) as e:
                    self.logger.warning(f"Failed to {action} {old_path}: {e}")
                    failed_count += 1
            
            # Send UI update to remove processed items
            # Only remove if it wasn't a copy action
            if action != "copy":
                self.queue.put(("remove_finder_items", iids_to_remove))
            
            msg = f"Process complete. {processed_count} files {action}d."
            if failed_count > 0:
                msg += f" Failed to process {failed_count} files (see console)."
            
            # --- FIXED: Use a different queue message to re-enable buttons ---
            # This allows the UI thread to reliably count remaining items
            self.queue.put(("finder_action_done", (msg, 0))) # 0 is a placeholder

        except Exception as e:
            self.logger.exception(f"Error in finder_action_logic ({action})")
            self.queue.put(("error", f"An error occurred during {action}: {e}"))

    # --- ============================= ---
    # --- Folder Analyzer Methods ---
    # --- ============================= ---

    def start_analyzer_scan(self):
        """Start the folder size analysis."""
        
        # Disable button *before* starting task
        self.analyzer_delete_button.config(state=tk.DISABLED)
        self.queue.put(("clear_analyzer_tree", None))
        self.update_status("Starting folder size scan...")
        
        include_files = self.analyzer_include_files_var.get()
        filters = {}
        
        try:
            if self.analyzer_size_check_var.get():
                size = float(self.analyzer_size_var.get())
                unit = self.analyzer_size_unit_var.get()
                op = self.analyzer_size_op_var.get()
                
                multiplier = {"KB": 1024, "MB": 1024**2, "GB": 1024**3}
                size_bytes = size * multiplier[unit]
                filters['size'] = (op, size_bytes)
            
            if self.analyzer_items_check_var.get():
                items = int(self.analyzer_items_var.get())
                op = self.analyzer_items_op_var.get()
                filters['items'] = (op, items)

        except Exception as e:
            messagebox.showerror("Invalid Filter", f"Error in filter settings: {e}")
            self.toggle_controls(scanning=False)
            return
            
        if self.start_task(self.analyzer_scan_logic, include_files, filters):
            self.update_status(f"Scanning folder sizes...")
            
    def analyzer_scan_logic(self, cancel_event, source_dir, include_files, filters):
        """Worker thread logic for scanning folder sizes from the bottom up."""
        try:
            folder_data = {} # Stores {'size': s, 'items': i} for each path
            results_batch = []
            total_items_found = 0

            # --- Filter Check Helper ---
            def check_filters(item_size, item_count, is_file=False):
                # --- CORRECTED FILTER LOGIC ---
                if 'size' in filters:
                    op, size_bytes = filters['size']
                    if op == "greater than":
                        if not item_size > size_bytes: return False
                    elif op == "less than":
                        if not item_size < size_bytes: return False
                
                if not is_file and 'items' in filters:
                    op, count = filters['items']
                    if op == "greater than":
                        if not item_count > count: return False
                    elif op == "less than":
                        if not item_count < count: return False
                
                return True # All checks passed
                # --- END CORRECTED FILTER LOGIC ---
            # ---------------------------

            for root, dirs, files in os.walk(source_dir, topdown=False):
                if cancel_event.is_set():
                    self.queue.put(("cancelled", None))
                    return

                self.queue.put(("status", f"Scanning: {root}"))
                
                file_size_total = 0
                file_count = 0
                
                # Process files in the current directory
                for f in files:
                    file_path = os.path.join(root, f)
                    try:
                        size = os.path.getsize(file_path)
                        file_size_total += size
                        file_count += 1
                        
                        if include_files:
                            if check_filters(size, 1, is_file=True):
                                results_batch.append((f, root, self.format_size(size), "File"))
                                total_items_found += 1
                                
                    except (IOError, OSError):
                        continue # Skip inaccessible files
                
                # Get size and items from subdirectories (already processed)
                subdir_size = sum(folder_data.get(os.path.join(root, d), {'size': 0})['size'] for d in dirs)
                subdir_items = sum(folder_data.get(os.path.join(root, d), {'items': 0})['items'] for d in dirs)
                
                my_size = file_size_total + subdir_size
                my_items = file_count + subdir_items
                my_name = os.path.basename(root)
                my_parent = os.path.dirname(root)

                # Store this folder's data for its parent
                folder_data[root] = {'size': my_size, 'items': my_items}
                
                # Add this folder to the results
                # Don't add the root source_dir itself, only its children
                if root != source_dir:
                    if check_filters(my_size, my_items, is_file=False):
                        results_batch.append((my_name, my_parent, self.format_size(my_size), my_items))
                        total_items_found += 1

                if len(results_batch) >= 100:
                    self.queue.put(("analyzer_results_batch", results_batch))
                    results_batch = []
            
            if results_batch: # Send final batch
                self.queue.put(("analyzer_results_batch", results_batch))
            
            self.queue.put(("analyzer_scan_done", (f"Scan complete. Found {total_items_found} matching items.", total_items_found)))
            
        except Exception as e:
            self.logger.exception("Error in analyzer_scan_logic")
            self.queue.put(("error", f"An error occurred during folder scan: {e}"))

    def start_analyzer_delete(self):
        """Start the delete process for selected items in the analyzer."""
        selected_iids = self.analyzer_tree.selection()
        if not selected_iids:
            messagebox.showinfo("No Items Selected", "Please select one or more files or folders from the list.")
            return

        if not messagebox.askyesno("Confirm Delete", f"Are you sure you want to delete {len(selected_iids)} selected items? This will delete all files and subfolders within any selected folder."):
            return
        
        # Get all data from the tree
        plan = []
        for iid in selected_iids:
            values = self.analyzer_tree.item(iid, 'values')
            name, path, _, _ = values
            full_path = os.path.join(path, name)
            plan.append(full_path)
        
        # --- CRITICAL: Sort by path length, deepest first ---
        # This ensures we delete 'C:/A/B/file.txt' before 'C:/A/B'
        plan.sort(key=len, reverse=True)

        if self.start_task(self.analyzer_delete_logic, plan, selected_iids):
            self.update_status(f"Deleting {len(plan)} items...")
            # Disable action button during processing
            self.analyzer_delete_button.config(state=tk.DISABLED)

    def analyzer_delete_logic(self, cancel_event, source_dir, plan, iids_to_remove):
        """Worker thread logic for deleting items from the analyzer list."""
        try:
            processed_count = 0
            failed_count = 0
            total_count = len(plan)
            
            for i, path in enumerate(plan):
                if cancel_event.is_set():
                    self.queue.put(("cancelled", None))
                    return
                
                self.queue.put(("status", f"Deleting {i+1}/{total_count}: {os.path.basename(path)}"))
                
                try:
                    if not os.path.exists(path):
                        continue # Already deleted, perhaps as part of a parent
                        
                    if not self.safe_delete(path):
                        raise IOError(f"safe_delete failed for {path}")
                    
                    processed_count += 1
                
                except (IOError, OSError, shutil.Error) as e:
                    self.logger.warning(f"Failed to delete {path}: {e}")
                    failed_count += 1
            
            # Send UI update to remove processed items
            self.queue.put(("remove_finder_items", iids_to_remove))
            
            msg = f"Delete complete. {processed_count} items deleted."
            if failed_count > 0:
                msg += f" Failed to delete {failed_count} items (see console)."
            
            # --- FIXED: Use a different queue message to re-enable buttons ---
            self.queue.put(("analyzer_action_done", (msg, 0))) # 0 is a placeholder

        except Exception as e:
            self.logger.exception("Error in analyzer_delete_logic")
            self.queue.put(("error", f"An error occurred during delete: {e}"))


    # --- ============================= ---
    # --- Context Menu Callbacks ---
    # --- ============================= ---

    def dupe_show_context_menu(self, event):
        """Show context menu for duplicate finder tree."""
        if self.dupe_tree.selection():
            self.dupe_context_menu.post(event.x_root, event.y_root)

    def dupe_open_folder(self):
        """Context menu action to open the selected file's folder."""
        try:
            selected_iid = self.dupe_tree.selection()[0]
            file_path = self.dupe_tree.item(selected_iid, 'values')[1]
            self._open_path(os.path.dirname(file_path))
        except (IndexError, tk.TclError):
            pass # No item selected or item deleted

    def dupe_delete_selected(self):
        """Context menu action to delete selected files."""
        selected_iids = self.dupe_tree.selection()
        if not selected_iids:
            return
        
        if not messagebox.askyesno("Confirm Delete", f"Are you sure you want to delete {len(selected_iids)} selected files?"):
            return
            
        paths_to_delete = []
        for iid in selected_iids:
            try:
                paths_to_delete.append(self.dupe_tree.item(iid, 'values')[1])
            except tk.TclError:
                continue # Item already gone
        
        # Run this as a "mini-task"
        if self.start_task(self.generic_delete_logic, paths_to_delete, selected_iids):
            self.update_status(f"Deleting {len(paths_to_delete)} files...")
            self.auto_delete_button.config(state=tk.DISABLED) # Disable main button

    def generic_delete_logic(self, cancel_event, source_dir, paths, iids_to_remove):
        """Used by dupe_delete_selected for a simple delete task."""
        try:
            deleted_count = 0
            failed_count = 0
            total_to_delete = len(paths)
            
            for i, path in enumerate(paths):
                if cancel_event.is_set():
                    self.queue.put(("cancelled", None))
                    return
                
                self.queue.put(("status", f"Deleting {i+1}/{total_to_delete}: {os.path.basename(path)}"))
                if self.safe_delete(path):
                    deleted_count += 1
                else:
                    failed_count += 1
            
            # Send UI update to remove deleted items
            self.queue.put(("remove_dupe_iids", iids_to_remove))
            
            msg = f"Delete complete. Deleted {deleted_count} files."
            if failed_count > 0:
                msg += f" Failed to delete {failed_count} files (see console)."
            
            # Re-enable auto-delete button if items remain
            # We use dupe_scan_done, which will get the count from the tree
            self.queue.put(("dupe_action_done", (msg, 0))) # 0 is placeholder

        except Exception as e:
            self.logger.exception("Error in generic_delete_logic")
            self.queue.put(("error", f"An error occurred during delete: {e}"))
            
    # --- FIXED: Re-added dupe_action_done to check_queue ---
    def check_queue(self):
        """Poll the queue for messages from worker threads."""
        processed_count = 0
        is_done_or_error = False
        final_message = ""
        
        try:
            while processed_count < QUEUE_BATCH_PROCESS_LIMIT:
                msg = self.queue.get_nowait()
                processed_count += 1
                
                msg_type, data = msg
                
                if msg_type == "status":
                    self.update_status(data)
                
                # --- Batch Treeview Updates ---
                elif msg_type == "dupe_results_batch":
                    for item in data:
                        self.dupe_tree.insert("", tk.END, values=item)
                elif msg_type == "sorter_results_batch":
                    for item in data:
                        self.sorter_tree.insert("", tk.END, values=item)
                elif msg_type == "collector_results_batch":
                    for item in data:
                        self.collector_tree.insert("", tk.END, values=item)
                elif msg_type == "finder_results_batch":
                    for item in data:
                        self.finder_tree.insert("", tk.END, values=item)
                elif msg_type == "analyzer_results_batch":
                    for item in data:
                        self.analyzer_tree.insert("", tk.END, values=item)

                # --- Clear Treeviews ---
                elif msg_type == "clear_dupe_tree":
                    self.dupe_tree.delete(*self.dupe_tree.get_children())
                elif msg_type == "clear_sorter_tree":
                    self.sorter_tree.delete(*self.sorter_tree.get_children())
                elif msg_type == "clear_collector_tree":
                    self.collector_tree.delete(*self.collector_tree.get_children())
                elif msg_type == "clear_finder_tree":
                    self.finder_tree.delete(*self.finder_tree.get_children())
                elif msg_type == "clear_analyzer_tree":
                    self.analyzer_tree.delete(*self.analyzer_tree.get_children())

                # --- Remove Specific Items (post-action) ---
                elif msg_type == "remove_dupe_iids":
                    self.dupe_tree.delete(*data)
                elif msg_type == "remove_finder_items":
                    for iid in data:
                        try:
                            # This message is now shared by Finder and Analyzer
                            self.finder_tree.delete(iid)
                        except tk.TclError:
                            try:
                                self.analyzer_tree.delete(iid)
                            except tk.TclError:
                                pass # Item already gone from both

                # --- "Preview Done" messages (enables action buttons) ---
                elif msg_type == "dupe_scan_done":
                    message, file_count = data # Unpack (message, count)
                    if file_count > 0:
                        self.auto_delete_button.config(state=tk.NORMAL)
                    is_done_or_error = True
                    final_message = message

                elif msg_type == "sorter_preview_done":
                    message, file_count = data # Unpack (message, count)
                    if file_count > 0:
                        self.sorter_process_button.config(state=tk.NORMAL)
                    is_done_or_error = True
                    final_message = message
                
                elif msg_type == "collector_preview_done":
                    message, file_count = data # Unpack (message, count)
                    if file_count > 0:
                        self.collector_process_button.config(state=tk.NORMAL)
                    is_done_or_error = True
                    final_message = message
                
                elif msg_type == "finder_preview_done":
                    message, file_count = data # Unpack (message, count)
                    if file_count > 0:
                        # Enable action buttons
                        self.finder_delete_button.config(state=tk.NORMAL)
                        self.finder_move_button.config(state=tk.NORMAL)
                        self.finder_copy_button.config(state=tk.NORMAL)
                    is_done_or_error = True
                    final_message = message

                elif msg_type == "analyzer_scan_done":
                    message, item_count = data # Unpack (message, count)
                    if item_count > 0:
                        self.analyzer_delete_button.config(state=tk.NORMAL)
                        # Sort by size by default
                        self.sort_treeview(self.analyzer_tree, "Size", True)
                    is_done_or_error = True
                    final_message = message
                
                # --- FIXED: Re-enable buttons based on remaining items ---
                elif msg_type == "dupe_action_done": # <-- This was missing
                    message, _ = data
                    # Get a fresh, reliable count
                    remaining_count = len(self.dupe_tree.get_children())
                    if remaining_count > 0:
                        self.auto_delete_button.config(state=tk.NORMAL)
                    is_done_or_error = True
                    final_message = message
                    
                elif msg_type == "finder_action_done":
                    message, _ = data
                    # Get a fresh, reliable count
                    remaining_count = len(self.finder_tree.get_children())
                    if remaining_count > 0:
                        self.finder_delete_button.config(state=tk.NORMAL)
                        self.finder_move_button.config(state=tk.NORMAL)
                        self.finder_copy_button.config(state=tk.NORMAL)
                    is_done_or_error = True
                    final_message = message
                
                elif msg_type == "analyzer_action_done":
                    message, _ = data
                     # Get a fresh, reliable count
                    remaining_count = len(self.analyzer_tree.get_children())
                    if remaining_count > 0:
                        self.analyzer_delete_button.config(state=tk.NORMAL)
                    is_done_or_error = True
                    final_message = message
                # --- END FIX ---

                # --- Final "Done" or "Error" messages ---
                elif msg_type == "done":
                    is_done_or_error = True
                    final_message = data
                
                elif msg_type == "error":
                    is_done_or_error = True
                    final_message = data
                
                elif msg_type == "cancelled":
                    is_done_or_error = True
                    final_message = "Task cancelled by user."

        except queue.Empty:
            pass # No more messages in queue
            
        except Exception as e:
            # This is a safety catch for the queue processor itself
            is_done_or_error = True
            final_message = f"Critical UI Error: {e}"
            self.logger.exception("Critical error in check_queue")

        # If a task finished, update state
        if self.current_task and is_done_or_error:
            self.toggle_controls(scanning=False) # Re-enable scan buttons
            self.current_task = None
            self.update_status(final_message) # Show final message
            
        # Reschedule the queue check
        self.root.after(QUEUE_POLL_INTERVAL_MS, self.check_queue)
    # --- End of re-inserted check_queue ---

    def finder_show_context_menu(self, event):
        """Show context menu for file finder tree."""
        if self.finder_tree.selection():
            self.finder_context_menu.post(event.x_root, event.y_root)

    def finder_open_file(self):
        """Context menu action to open selected file(s)."""
        try:
            for selected_iid in self.finder_tree.selection():
                values = self.finder_tree.item(selected_iid, 'values')
                file_path = os.path.join(values[1], values[0])
                self._open_path(file_path)
        except (IndexError, tk.TclError):
            pass # No item selected or item deleted

    def finder_open_folder(self):
        """Context menu action to open the selected file's folder."""
        try:
            # Open the folder for the *first* selected item
            selected_iid = self.finder_tree.selection()[0] 
            folder_path = self.finder_tree.item(selected_iid, 'values')[1]
            self._open_path(folder_path)
        except (IndexError, tk.TclError):
            pass # No item selected or item deleted
    
    def analyzer_show_context_menu(self, event):
        """Show context menu for analyzer tree."""
        if self.analyzer_tree.selection():
            self.analyzer_context_menu.post(event.x_root, event.y_root)
            
    def analyzer_open_folder(self):
        """Open the folder/file path of the selected item in the analyzer tree."""
        try:
            # Open the folder for the *first* selected item
            selected_iid = self.analyzer_tree.selection()[0]
            values = self.analyzer_tree.item(selected_iid, 'values')
            name, path, _, item_type = values
            if item_type == "File":
                self._open_path(path) # Open the containing folder
            else:
                self._open_path(os.path.join(path, name)) # Open the folder itself
        except (IndexError, tk.TclError):
            pass # No item selected or item deleted


    # --- ============================= ---
    # --- Core & Utility Methods ---
    # --- ============================= ---

    def hash_file(self, path, block_size=65536):
        """Return the SHA-256 hash of a file."""
        sha256 = hashlib.sha256()
        with open(path, 'rb') as f:
            for block in iter(lambda: f.read(block_size), b''):
                sha256.update(block)
        return sha256.hexdigest()

    def format_size(self, size_bytes):
        """Convert bytes to a human-readable string (KB, MB, GB)."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024**2:
            return f"{size_bytes/1024:.2f} KB"
        elif size_bytes < 1024**3:
            return f"{size_bytes/1024**2:.2f} MB"
        else:
            return f"{size_bytes/1024**3:.2f} GB"

    def sort_treeview(self, tree, col, reverse):
        """
        Sort a treeview column when the header is clicked.
        This now correctly handles file sizes.
        """
        try:
            data_list = [(tree.set(k, col), k) for k in tree.get_children('')]
        except tk.TclError:
            return # Handle edge case where tree is modified during sort

        # --- CORRECTED LOGIC ---
        if col == "Size":
            # Special handling for 'Size' column
            def convert_size_to_bytes(size_str):
                try:
                    parts = str(size_str).split(" ")
                    if len(parts) != 2:
                        return 0
                    
                    val_str, unit = parts
                    val = float(val_str)
                    
                    if unit == "KB":
                        return val * 1024
                    elif unit == "MB":
                        return val * 1024**2
                    elif unit == "GB":
                        return val * 1024**3
                    elif unit == "B":
                        return val
                    else:
                        return 0
                except (ValueError, TypeError, AttributeError):
                    return 0 # Fallback for invalid format
            
            # Sort by the converted byte value
            data_list.sort(key=lambda x: convert_size_to_bytes(x[0]), reverse=reverse)
            
        elif col in ("Set #", "Items"):
            # Try numeric sort for 'Set #' and 'Items'
            def extract_num(val_tuple):
                try:
                    s = str(val_tuple[0]).split(" ")[0].replace(',', '')
                    if s.lower() == 'file': return -1 # Treat "File" as -1 in 'Items' col
                    return float(s)
                except (ValueError, TypeError, AttributeError):
                    return 0 # Fallback for non-numeric
            
            data_list.sort(key=extract_num, reverse=reverse)
        
        else:
            # Default string sort for all other columns (File, Path, Modified, etc.)
            data_list.sort(key=lambda x: str(x[0]).lower(), reverse=reverse)
        # --- END OF CORRECTED LOGIC ---

        for index, (val, k) in enumerate(data_list):
            tree.move(k, '', index)

        # Reverse sort direction for next click
        tree.heading(col, command=lambda: self.sort_treeview(tree, col, not reverse))

    def safe_delete(self, path):
        """
        Delete a file or folder. Uses send2trash if available.
        If not, permanently deletes.
        """
        try:
            if HAS_SEND2TRASH:
                send2trash(path) # This handles files and folders correctly
            else:
                # No send2trash, so we must permanently delete
                if not os.path.exists(path):
                    return True # Already gone
                if os.path.isfile(path):
                    os.remove(path)
                elif os.path.isdir(path):
                    shutil.rmtree(path) # PERMANENTLY delete folder and all contents
            return True
        except Exception as e:
            self.logger.error(f"Error deleting '{path}': {e}")
            return False
    
    def get_unique_filename(self, path):
        """Finds a unique filename by appending (1), (2), etc. if the path exists."""
        if not os.path.exists(path):
            return path
        
        base, ext = os.path.splitext(path)
        i = 1
        while True:
            new_path = f"{base} ({i}){ext}"
            if not os.path.exists(new_path):
                return new_path
            i += 1

    def _open_path(self, path):
        """Open a file or folder in the default system application."""
        try:
            if platform.system() == "Windows":
                os.startfile(path)
            elif platform.system() == "Darwin": # macOS
                subprocess.Popen(["open", path])
            else: # Linux
                subprocess.Popen(["xdg-open", path])
        except Exception as e:
            self.logger.warning(f"Failed to open path {path}: {e}")
            messagebox.showwarning("Open Failed", f"Could not open path: {e}")

    def export_csv_report(self, final_dupe_sets, source_dir):
        """Export the duplicate file list to a CSV file."""
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
            filename = f"Duplicate_Report_{timestamp}.csv"
            report_path = os.path.join(source_dir, filename)
            
            with open(report_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Set #", "File Path", "Size (Bytes)", "Modification Time"])
                
                for i, dupe_set in enumerate(final_dupe_sets):
                    set_id = f"Set {i+1}"
                    for path in dupe_set:
                        try:
                            stat = os.stat(path)
                            mod_time_str = datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                            writer.writerow([set_id, path, stat.st_size, mod_time_str])
                        except (IOError, OSError):
                            writer.writerow([set_id, path, "N/A", "N/A (File may be inaccessible)"])
            
            self.queue.put(("status", f"Successfully exported report to {report_path}"))
        except Exception as e:
            self.logger.exception("Failed to export CSV")
            self.queue.put(("error", f"Failed to export CSV report: {e}"))
            
    def on_closing(self):
        """Handle the window close event."""
        if self.current_task:
            if messagebox.askyesno("Task in Progress", "A task is still running. Are you sure you want to quit?"):
                self.cancel_task()
                self.root.destroy()
        else:
            self.root.destroy()

# --- Main execution ---
if __name__ == "__main__":
    
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Create a simple logger for the app
    app_logger = logging.getLogger('FileManagementApp')
    
    main_root = tk.Tk()
    app = FileManagementApp(main_root)
    
    # Add logger to app instance
    app.logger = app_logger 
    
    # Store start time on the app object for elapsed time calculation
    app.start_time = datetime.now() 
    
    # Set the close protocol
    main_root.protocol("WM_DELETE_WINDOW", app.on_closing)
    
    main_root.mainloop()