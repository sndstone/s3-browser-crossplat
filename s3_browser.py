#!/usr/bin/env python3
"""
Enhanced S3 Browser Clone - A Linux-native S3 browser with advanced features
Requirements: pip install boto3 tkinter
"""
import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog
from tkinter import font as tkfont
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.config import Config
import json
import os
from datetime import datetime
import threading
from urllib.parse import urlparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
import logging
import random
import re
import subprocess
import sys
from collections import deque

class APICallLogger:
    """Simple API call logger for debugging"""
    
    def __init__(self, debug_callback=None):
        self.debug_callback = debug_callback
        self.api_calls = []
    
    def log_api_call(self, event_name, **kwargs):
        """Log API call details"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        call_info = {
            'timestamp': timestamp,
            'event': event_name,
            'service': kwargs.get('service_id', 'unknown'),
            'operation': kwargs.get('operation_name', 'unknown'),
            'error': kwargs.get('error', None)
        }
        
        self.api_calls.append(call_info)
        
        if self.debug_callback:
            # Format for display
            display_text = f"[{timestamp}] {event_name}\n"
            display_text += f"  Service: {call_info['service']}\n"
            display_text += f"  Operation: {call_info['operation']}\n"
            if call_info['error']:
                display_text += f"  Error: {call_info['error']}\n"
            display_text += "\n"
            self.debug_callback(display_text)
    
    def get_all_calls(self):
        """Get all logged API calls"""
        return self.api_calls
    
    def clear_calls(self):
        """Clear all logged calls"""
        self.api_calls.clear()

class S3BrowserApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Enhanced S3 Browser Clone")
        self.root.geometry("1400x900")
        
        # Configuration
        self.config_file = os.path.expanduser("~/.s3browser_config.json")
        self.endpoints = {}
        self.current_endpoint = None
        self.s3_client = None
        
        # State
        self.current_bucket = None
        self.current_prefix = ""
        self.objects = []
        self.continuation_token = None
        self.loading = False
        self.total_object_count = 0
        self.total_folder_count = 0
        self.flat_view = tk.BooleanVar(value=False)
        self.lifecycle_rules = []
        self.rules_listbox = None
        self.policy_json_text = None
        
        # Settings
        self.multipart_threshold = tk.IntVar(value=64)  # MB
        self.multipart_chunksize = tk.IntVar(value=8)   # MB
        self.max_concurrent_requests = tk.IntVar(value=10)
        self.api_logging_enabled = tk.BooleanVar(value=False)
        self.max_retries = tk.IntVar(value=3)
        self.retry_base_delay = tk.DoubleVar(value=0.5)
        self.retry_max_delay = tk.DoubleVar(value=8.0)
        self.request_delay_ms = tk.IntVar(value=0)
        self.connect_timeout = tk.IntVar(value=5)
        self.read_timeout = tk.IntVar(value=60)
        
        # Threading
        self.executor = ThreadPoolExecutor(max_workers=10)
        
        # API call logging
        self.api_logger = APICallLogger(self.log_api_call_to_debug)
        
        # Theme
        self.dark_mode = tk.BooleanVar(value=False)
        self.accent_color = "#0078D4"  # Default blue accent
        self.debug_mode = tk.BooleanVar(value=False)

        # Debug log buffer
        self.debug_log_buffer = deque(maxlen=2000)

        # Status animation
        self.status_busy = False
        self.status_anim_job = None
        self.status_message_base = "Ready"
        self.status_dots = 0
        
        # Versions filtering
        self.versions_data = []
        self.versions_prefix_var = tk.StringVar()
        self.versions_filter_var = tk.StringVar()
        self.versions_regex_var = tk.BooleanVar(value=False)
        self.versions_show_delete_markers_var = tk.BooleanVar(value=True)
        self.versions_show_versions_var = tk.BooleanVar(value=True)
        
        # Initialize UI
        self.setup_styles()
        self.create_menu()
        self.create_main_ui()
        self.load_config()
        self.apply_theme()
        self.setup_responsive_layout()

    def setup_api_logging(self):
        """Setup basic API logging without monkey patching"""
        # For now, we'll use a simpler approach to avoid breaking boto3
        # This can be enhanced later with proper boto3 event system
        pass

    def setup_api_logging(self):
        """Setup safe API logging using boto3 event system"""
        if not hasattr(self, 's3_client') or not self.s3_client:
            return
            
        try:
            # Use boto3's built-in event system for safer logging
            def log_api_call(event_name, **kwargs):
                if self.api_logging_enabled.get():
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    operation = kwargs.get('operation_name', 'unknown')
                    service = kwargs.get('service_id', 's3')
                    
                    log_message = f"[{timestamp}] {event_name}: {service}.{operation}\n"
                    if 'error' in kwargs:
                        log_message += f"  Error: {kwargs['error']}\n"
                    
                    self.log_api_call_to_debug(log_message)
            
            # Register event handlers if s3_client exists
            if hasattr(self, 's3_client') and self.s3_client:
                event_system = self.s3_client.meta.events
                event_system.register('before-call.*', log_api_call)
                event_system.register('after-call.*', log_api_call)
        except Exception as e:
            # If event registration fails, just continue without API logging
            self.log_debug(f"API logging setup failed: {e}")

    def log_api_call_to_debug(self, message):
        """Callback for API call logging"""
        self.append_debug_message(message, with_timestamp=False)

    def setup_styles(self):
        """Setup ttk styles for theming"""
        self.style = ttk.Style()
        try:
            self.style.theme_use("clam")
        except tk.TclError:
            pass

        self.base_font_family = self.get_preferred_font_family()
        self.base_font = (self.base_font_family, 10)
        self.heading_font = (self.base_font_family, 10, "bold")
        self.button_padding_normal = (10, 4)
        self.button_padding_compact = (5, 2)

        self.style.configure(".", font=self.base_font)
        self.style.configure("TButton", padding=self.button_padding_normal)
        self.style.configure("TNotebook.Tab", padding=(12, 6))
        self.style.configure("Treeview", rowheight=24)
        self.style.configure("Treeview.Heading", font=self.heading_font)
        self.themes = {
            "light": {
                "bg": "#F5F6FA",
                "fg": "#1E1F24",
                "select_bg": "#E4EEFF",
                "frame_bg": "#FFFFFF",
                "entry_bg": "#FFFFFF",
                "button_bg": "#EEF0F5",
                "border": "#DADDE6"
            },
            "dark": {
                "bg": "#111418",
                "fg": "#F2F4F8",
                "select_bg": "#2D3340",
                "frame_bg": "#1B1F2A",
                "entry_bg": "#252B36",
                "button_bg": "#202636",
                "border": "#343A46"
            }
        }

    def create_menu(self):
        """Create application menu"""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        
        # File menu
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="Configure Endpoints", command=self.configure_endpoints)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)
        
        # View menu
        view_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="View", menu=view_menu)
        view_menu.add_checkbutton(label="Dark Mode", variable=self.dark_mode, command=self.apply_theme)
        view_menu.add_checkbutton(label="Debug Mode", variable=self.debug_mode, command=self.toggle_debug)
        view_menu.add_checkbutton(label="Flat View", variable=self.flat_view, command=self.toggle_flat_view)
        view_menu.add_separator()
        view_menu.add_command(label="Accent Color", command=self.choose_accent_color)
        
        # Settings menu
        settings_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Settings", menu=settings_menu)
        settings_menu.add_command(label="Transfer Settings", command=self.configure_transfer_settings)
        settings_menu.add_command(label="Safety Settings", command=self.configure_safety_settings)
        settings_menu.add_separator()
        settings_menu.add_checkbutton(label="Enable API Logging", variable=self.api_logging_enabled)
        
        # Debug menu
        debug_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Debug", menu=debug_menu)
        debug_menu.add_command(label="Export Debug Log", command=self.export_debug_log)
        debug_menu.add_command(label="Clear Debug Log", command=self.clear_debug_log)
        debug_menu.add_command(label="Export API Calls", command=self.export_api_calls)
        debug_menu.add_command(label="Clear API Calls", command=self.clear_api_calls)
        
        # Tools menu
        tools_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Tools", menu=tools_menu)
        tools_menu.add_command(label="Put Test Data...", command=self.open_put_testdata_tool)
        tools_menu.add_command(label="Delete All...", command=self.open_delete_all_tool)
        tools_menu.add_separator()
        tools_menu.add_command(label="Refresh", command=self.refresh_all)

    def configure_transfer_settings(self):
        """Configure transfer and multipart settings"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Transfer Settings")
        dialog.geometry("400x300")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Multipart settings
        multipart_frame = ttk.LabelFrame(dialog, text="Multipart Upload Settings", padding=10)
        multipart_frame.pack(fill=tk.X, padx=10, pady=10)
        
        # Multipart threshold
        ttk.Label(multipart_frame, text="Multipart Threshold (MB):").grid(row=0, column=0, sticky=tk.W, pady=5)
        threshold_spinbox = ttk.Spinbox(multipart_frame, from_=1, to=1000, width=15, textvariable=self.multipart_threshold)
        threshold_spinbox.grid(row=0, column=1, padx=(10, 0), pady=5)
        
        # Chunk size
        ttk.Label(multipart_frame, text="Chunk Size (MB):").grid(row=1, column=0, sticky=tk.W, pady=5)
        chunksize_spinbox = ttk.Spinbox(multipart_frame, from_=1, to=100, width=15, textvariable=self.multipart_chunksize)
        chunksize_spinbox.grid(row=1, column=1, padx=(10, 0), pady=5)
        
        # Concurrency settings
        concurrency_frame = ttk.LabelFrame(dialog, text="Concurrency Settings", padding=10)
        concurrency_frame.pack(fill=tk.X, padx=10, pady=10)
        
        ttk.Label(concurrency_frame, text="Max Concurrent Requests:").grid(row=0, column=0, sticky=tk.W, pady=5)
        concurrent_spinbox = ttk.Spinbox(concurrency_frame, from_=1, to=50, width=15, textvariable=self.max_concurrent_requests)
        concurrent_spinbox.grid(row=0, column=1, padx=(10, 0), pady=5)
        
        # Buttons
        button_frame = ttk.Frame(dialog)
        button_frame.pack(fill=tk.X, padx=10, pady=10)
        
        def save_settings():
            # Update executor with new max workers
            self.executor.shutdown(wait=False)
            self.executor = ThreadPoolExecutor(max_workers=self.max_concurrent_requests.get())
            self.save_config()
            messagebox.showinfo("Success", "Transfer settings saved successfully")
            dialog.destroy()
        
        ttk.Button(button_frame, text="Save", command=save_settings).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(button_frame, text="Cancel", command=dialog.destroy).pack(side=tk.LEFT)

    def configure_safety_settings(self):
        """Configure retry, timeout, and delay settings"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Safety Settings")
        dialog.geometry("420x320")
        dialog.transient(self.root)
        dialog.grab_set()

        settings_frame = ttk.LabelFrame(dialog, text="API Safety Settings", padding=10)
        settings_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(settings_frame, text="Max Retries:").grid(row=0, column=0, sticky=tk.W, pady=5)
        ttk.Spinbox(settings_frame, from_=1, to=10, width=10, textvariable=self.max_retries).grid(row=0, column=1, sticky=tk.W, pady=5)

        ttk.Label(settings_frame, text="Retry Base Delay (s):").grid(row=1, column=0, sticky=tk.W, pady=5)
        ttk.Entry(settings_frame, width=10, textvariable=self.retry_base_delay).grid(row=1, column=1, sticky=tk.W, pady=5)

        ttk.Label(settings_frame, text="Retry Max Delay (s):").grid(row=2, column=0, sticky=tk.W, pady=5)
        ttk.Entry(settings_frame, width=10, textvariable=self.retry_max_delay).grid(row=2, column=1, sticky=tk.W, pady=5)

        ttk.Label(settings_frame, text="Request Delay (ms):").grid(row=3, column=0, sticky=tk.W, pady=5)
        ttk.Spinbox(settings_frame, from_=0, to=10000, width=10, textvariable=self.request_delay_ms).grid(row=3, column=1, sticky=tk.W, pady=5)

        ttk.Label(settings_frame, text="Connect Timeout (s):").grid(row=4, column=0, sticky=tk.W, pady=5)
        ttk.Spinbox(settings_frame, from_=1, to=120, width=10, textvariable=self.connect_timeout).grid(row=4, column=1, sticky=tk.W, pady=5)

        ttk.Label(settings_frame, text="Read Timeout (s):").grid(row=5, column=0, sticky=tk.W, pady=5)
        ttk.Spinbox(settings_frame, from_=1, to=300, width=10, textvariable=self.read_timeout).grid(row=5, column=1, sticky=tk.W, pady=5)

        button_frame = ttk.Frame(dialog)
        button_frame.pack(fill=tk.X, padx=10, pady=10)

        def save_settings():
            self.save_config()
            if self.current_endpoint:
                self.endpoint_var.set(self.current_endpoint)
                self.on_endpoint_selected(None)
            messagebox.showinfo("Success", "Safety settings saved successfully")
            dialog.destroy()

        ttk.Button(button_frame, text="Save", command=save_settings).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(button_frame, text="Cancel", command=dialog.destroy).pack(side=tk.LEFT)

    def get_app_dir(self):
        """Return the application directory"""
        return os.path.dirname(os.path.abspath(__file__))

    def fit_dialog_to_screen(self, dialog, min_width=600, min_height=400, margin=200):
        """Size dialog to fit within the available screen area."""
        dialog.update_idletasks()
        screen_w = dialog.winfo_screenwidth()
        screen_h = dialog.winfo_screenheight()
        available_w = max(320, screen_w - margin)
        available_h = max(320, screen_h - margin)

        req_w = dialog.winfo_reqwidth()
        req_h = dialog.winfo_reqheight()
        width = min(max(req_w, min_width), available_w)
        height = min(max(req_h, min_height), available_h)

        dialog.geometry(f"{width}x{height}")
        dialog.minsize(min(min_width, available_w), min(min_height, available_h))
        dialog.resizable(True, True)

    def open_put_testdata_tool(self):
        """Open GUI for put-testdata.py"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Put Test Data")
        dialog.transient(self.root)
        dialog.grab_set()

        endpoint = self.endpoints.get(self.current_endpoint, {}) if self.current_endpoint else {}

        fields_frame = ttk.LabelFrame(dialog, text="Options", padding=10)
        fields_frame.pack(fill=tk.X, padx=10, pady=10)

        bucket_var = tk.StringVar(value=self.current_bucket or "")
        endpoint_var = tk.StringVar(value=endpoint.get("endpoint_url", ""))
        access_key_var = tk.StringVar(value=endpoint.get("access_key", ""))
        secret_key_var = tk.StringVar(value=endpoint.get("secret_key", ""))
        object_size_var = tk.StringVar(value="1048576")
        versions_var = tk.StringVar(value="1")
        objects_count_var = tk.StringVar(value="10")
        object_prefix_var = tk.StringVar(value="")
        threads_var = tk.StringVar(value=str(self.max_concurrent_requests.get()))
        simple_data_var = tk.BooleanVar(value=False)
        debug_var = tk.BooleanVar(value=False)
        checksum_var = tk.StringVar(value="md5")

        def add_row(row, label, widget):
            ttk.Label(fields_frame, text=label).grid(row=row, column=0, sticky=tk.W, pady=4)
            widget.grid(row=row, column=1, sticky="ew", pady=4)

        add_row(0, "Bucket Name:", ttk.Entry(fields_frame, textvariable=bucket_var))
        add_row(1, "Endpoint URL:", ttk.Entry(fields_frame, textvariable=endpoint_var))
        add_row(2, "Access Key:", ttk.Entry(fields_frame, textvariable=access_key_var))
        secret_entry = ttk.Entry(fields_frame, textvariable=secret_key_var, show="*")
        add_row(3, "Secret Key:", secret_entry)
        add_row(4, "Object Size (bytes):", ttk.Entry(fields_frame, textvariable=object_size_var))
        add_row(5, "Versions:", ttk.Entry(fields_frame, textvariable=versions_var))
        add_row(6, "Objects Count:", ttk.Entry(fields_frame, textvariable=objects_count_var))
        add_row(7, "Object Prefix:", ttk.Entry(fields_frame, textvariable=object_prefix_var))
        add_row(8, "Threads:", ttk.Entry(fields_frame, textvariable=threads_var))

        checksum_combo = ttk.Combobox(fields_frame, textvariable=checksum_var, values=["md5", "crc32", "crc32c", "sha1", "sha256", "none"], state="readonly")
        add_row(9, "Checksum:", checksum_combo)

        ttk.Checkbutton(fields_frame, text="Use Simple Data", variable=simple_data_var).grid(row=10, column=1, sticky=tk.W, pady=4)
        ttk.Checkbutton(fields_frame, text="Debug Logging", variable=debug_var).grid(row=11, column=1, sticky=tk.W, pady=4)

        fields_frame.grid_columnconfigure(1, weight=1)

        output_frame = ttk.LabelFrame(dialog, text="Output", padding=10)
        output_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        output_text = tk.Text(output_frame, height=12, wrap=tk.NONE)
        output_scroll = ttk.Scrollbar(output_frame, command=output_text.yview)
        output_text.configure(yscrollcommand=output_scroll.set)
        output_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        output_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        status_label = ttk.Label(dialog, text="Ready")
        status_label.pack(fill=tk.X, padx=10)

        button_frame = ttk.Frame(dialog)
        button_frame.pack(fill=tk.X, padx=10, pady=10)

        process_holder = {'process': None}

        def append_output(text):
            output_text.insert(tk.END, text)
            output_text.see(tk.END)

        def run_script():
            if process_holder['process'] and process_holder['process'].poll() is None:
                return

            bucket = bucket_var.get().strip()
            endpoint_url = endpoint_var.get().strip()
            access_key = access_key_var.get().strip()
            secret_key = secret_key_var.get().strip()

            if not bucket or not endpoint_url or not access_key or not secret_key:
                messagebox.showerror("Error", "Bucket, endpoint, access key, and secret key are required")
                return

            try:
                object_size = int(object_size_var.get().strip())
                versions = int(versions_var.get().strip())
                objects_count = int(objects_count_var.get().strip())
            except ValueError:
                messagebox.showerror("Error", "Object size, versions, and objects count must be integers")
                return

            cmd = [sys.executable, os.path.join(self.get_app_dir(), "put-testdata.py")]
            if bucket:
                cmd += ["--bucket_name", bucket]
            if endpoint_url:
                cmd += ["--s3_endpoint_url", endpoint_url]
            if access_key:
                cmd += ["--aws_access_key_id", access_key]
            if secret_key:
                cmd += ["--aws_secret_access_key", secret_key]
            cmd += ["--object_size", str(object_size)]
            cmd += ["--versions", str(versions)]
            cmd += ["--objects_count", str(objects_count)]

            if object_prefix_var.get().strip():
                cmd += ["--object_prefix", object_prefix_var.get().strip()]
            if threads_var.get().strip():
                cmd += ["--threads", threads_var.get().strip()]
            if simple_data_var.get():
                cmd.append("--simple_data")
            if debug_var.get():
                cmd.append("--debug")
            if checksum_var.get().strip():
                cmd += ["--checksum", checksum_var.get().strip()]

            output_text.delete(1.0, tk.END)
            status_label.config(text="Running...")
            run_button.config(state=tk.DISABLED)
            cancel_button.config(state=tk.NORMAL)

            try:
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    cwd=self.get_app_dir()
                )
            except Exception as e:
                status_label.config(text="Failed to start")
                run_button.config(state=tk.NORMAL)
                cancel_button.config(state=tk.DISABLED)
                messagebox.showerror("Error", f"Failed to start script: {e}")
                return

            process_holder['process'] = process

            def reader():
                try:
                    for line in process.stdout:
                        self.root.after(0, append_output, line)
                except Exception as e:
                    self.root.after(0, append_output, f"\nError reading output: {e}\n")
                finally:
                    returncode = process.wait()
                    self.root.after(0, lambda: status_label.config(text=f"Finished with code {returncode}"))
                    self.root.after(0, lambda: run_button.config(state=tk.NORMAL))
                    self.root.after(0, lambda: cancel_button.config(state=tk.DISABLED))

            threading.Thread(target=reader, daemon=True).start()

        def cancel_script():
            process = process_holder.get('process')
            if not process or process.poll() is not None:
                return
            status_label.config(text="Cancelling...")
            process.terminate()
            try:
                process.wait(timeout=3)
            except Exception:
                process.kill()
            status_label.config(text="Cancelled")

        run_button = ttk.Button(button_frame, text="Run", command=run_script)
        cancel_button = ttk.Button(button_frame, text="Cancel", command=cancel_script, state=tk.DISABLED)
        ttk.Button(button_frame, text="Close", command=dialog.destroy).pack(side=tk.RIGHT)
        run_button.pack(side=tk.LEFT, padx=(0, 5))
        cancel_button.pack(side=tk.LEFT)

        self.fit_dialog_to_screen(dialog, min_width=640, min_height=520)
        dialog.protocol("WM_DELETE_WINDOW", dialog.destroy)

    def open_delete_all_tool(self):
        """Open GUI for delete-all.py"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Delete All Objects")
        dialog.transient(self.root)
        dialog.grab_set()

        endpoint = self.endpoints.get(self.current_endpoint, {}) if self.current_endpoint else {}

        fields_frame = ttk.LabelFrame(dialog, text="Options", padding=10)
        fields_frame.pack(fill=tk.X, padx=10, pady=10)

        bucket_var = tk.StringVar(value=self.current_bucket or "")
        endpoint_var = tk.StringVar(value=endpoint.get("endpoint_url", ""))
        access_key_var = tk.StringVar(value=endpoint.get("access_key", ""))
        secret_key_var = tk.StringVar(value=endpoint.get("secret_key", ""))
        debug_var = tk.BooleanVar(value=False)
        checksum_var = tk.StringVar(value="")
        batch_size_var = tk.StringVar(value="1000")
        max_workers_var = tk.StringVar(value="50")
        max_retries_var = tk.StringVar(value="5")
        retry_mode_var = tk.StringVar(value="adaptive")
        max_requests_var = tk.StringVar(value="10000")
        max_connections_var = tk.StringVar(value="1000")
        pipeline_size_var = tk.StringVar(value="50")
        list_max_keys_var = tk.StringVar(value="1000")
        immediate_deletion_var = tk.BooleanVar(value=True)
        deletion_delay_var = tk.StringVar(value="0")

        def add_row(row, label, widget):
            ttk.Label(fields_frame, text=label).grid(row=row, column=0, sticky=tk.W, pady=4)
            widget.grid(row=row, column=1, sticky="ew", pady=4)

        add_row(0, "Bucket Name:", ttk.Entry(fields_frame, textvariable=bucket_var))
        add_row(1, "Endpoint URL:", ttk.Entry(fields_frame, textvariable=endpoint_var))
        add_row(2, "Access Key:", ttk.Entry(fields_frame, textvariable=access_key_var))
        secret_entry = ttk.Entry(fields_frame, textvariable=secret_key_var, show="*")
        add_row(3, "Secret Key:", secret_entry)

        checksum_combo = ttk.Combobox(fields_frame, textvariable=checksum_var, values=["", "CRC32", "CRC32C", "SHA1", "SHA256", "MD5"], state="readonly")
        add_row(4, "Checksum:", checksum_combo)

        add_row(5, "Batch Size:", ttk.Entry(fields_frame, textvariable=batch_size_var))
        add_row(6, "Max Workers:", ttk.Entry(fields_frame, textvariable=max_workers_var))
        add_row(7, "Max Retries:", ttk.Entry(fields_frame, textvariable=max_retries_var))
        retry_combo = ttk.Combobox(fields_frame, textvariable=retry_mode_var, values=["standard", "adaptive"], state="readonly")
        add_row(8, "Retry Mode:", retry_combo)
        add_row(9, "Max Requests/sec:", ttk.Entry(fields_frame, textvariable=max_requests_var))
        add_row(10, "Max Connections:", ttk.Entry(fields_frame, textvariable=max_connections_var))
        add_row(11, "Pipeline Size:", ttk.Entry(fields_frame, textvariable=pipeline_size_var))
        add_row(12, "List Max Keys:", ttk.Entry(fields_frame, textvariable=list_max_keys_var))
        add_row(13, "Deletion Delay (s):", ttk.Entry(fields_frame, textvariable=deletion_delay_var))

        ttk.Checkbutton(fields_frame, text="Immediate Deletion", variable=immediate_deletion_var).grid(row=14, column=1, sticky=tk.W, pady=4)
        ttk.Checkbutton(fields_frame, text="Debug Logging", variable=debug_var).grid(row=15, column=1, sticky=tk.W, pady=4)

        fields_frame.grid_columnconfigure(1, weight=1)

        output_frame = ttk.LabelFrame(dialog, text="Output", padding=10)
        output_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        output_text = tk.Text(output_frame, height=12, wrap=tk.NONE)
        output_scroll = ttk.Scrollbar(output_frame, command=output_text.yview)
        output_text.configure(yscrollcommand=output_scroll.set)
        output_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        output_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        status_label = ttk.Label(dialog, text="Ready")
        status_label.pack(fill=tk.X, padx=10)

        button_frame = ttk.Frame(dialog)
        button_frame.pack(fill=tk.X, padx=10, pady=10)

        process_holder = {'process': None}

        def append_output(text):
            output_text.insert(tk.END, text)
            output_text.see(tk.END)

        def run_script():
            if process_holder['process'] and process_holder['process'].poll() is None:
                return

            bucket = bucket_var.get().strip()
            endpoint_url = endpoint_var.get().strip()
            access_key = access_key_var.get().strip()
            secret_key = secret_key_var.get().strip()

            if not bucket or not endpoint_url or not access_key or not secret_key:
                messagebox.showerror("Error", "Bucket, endpoint, access key, and secret key are required")
                return

            cmd = [sys.executable, os.path.join(self.get_app_dir(), "delete-all.py")]
            if bucket:
                cmd += ["--bucket_name", bucket]
            if endpoint_url:
                cmd += ["--s3_endpoint_url", endpoint_url]
            if access_key:
                cmd += ["--aws_access_key_id", access_key]
            if secret_key:
                cmd += ["--aws_secret_access_key", secret_key]

            def add_int_arg(flag, value):
                value = value.strip()
                if value:
                    cmd.extend([flag, value])

            add_int_arg("--batch_size", batch_size_var.get())
            add_int_arg("--max_workers", max_workers_var.get())
            add_int_arg("--max_retries", max_retries_var.get())
            add_int_arg("--max_requests_per_second", max_requests_var.get())
            add_int_arg("--max_connections", max_connections_var.get())
            add_int_arg("--pipeline_size", pipeline_size_var.get())
            add_int_arg("--list_max_keys", list_max_keys_var.get())
            add_int_arg("--deletion_delay", deletion_delay_var.get())

            if retry_mode_var.get().strip():
                cmd += ["--retry_mode", retry_mode_var.get().strip()]
            if checksum_var.get().strip():
                cmd += ["--checksum", checksum_var.get().strip()]
            if immediate_deletion_var.get():
                cmd.append("--immediate_deletion")
            else:
                cmd.append("--no_immediate_deletion")
            if debug_var.get():
                cmd.append("--debug")

            output_text.delete(1.0, tk.END)
            status_label.config(text="Running...")
            run_button.config(state=tk.DISABLED)
            cancel_button.config(state=tk.NORMAL)

            try:
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    cwd=self.get_app_dir()
                )
            except Exception as e:
                status_label.config(text="Failed to start")
                run_button.config(state=tk.NORMAL)
                cancel_button.config(state=tk.DISABLED)
                messagebox.showerror("Error", f"Failed to start script: {e}")
                return

            process_holder['process'] = process

            def reader():
                try:
                    for line in process.stdout:
                        self.root.after(0, append_output, line)
                except Exception as e:
                    self.root.after(0, append_output, f"\nError reading output: {e}\n")
                finally:
                    returncode = process.wait()
                    self.root.after(0, lambda: status_label.config(text=f"Finished with code {returncode}"))
                    self.root.after(0, lambda: run_button.config(state=tk.NORMAL))
                    self.root.after(0, lambda: cancel_button.config(state=tk.DISABLED))

            threading.Thread(target=reader, daemon=True).start()

        def cancel_script():
            process = process_holder.get('process')
            if not process or process.poll() is not None:
                return
            status_label.config(text="Cancelling...")
            process.terminate()
            try:
                process.wait(timeout=3)
            except Exception:
                process.kill()
            status_label.config(text="Cancelled")

        run_button = ttk.Button(button_frame, text="Run", command=run_script)
        cancel_button = ttk.Button(button_frame, text="Cancel", command=cancel_script, state=tk.DISABLED)
        ttk.Button(button_frame, text="Close", command=dialog.destroy).pack(side=tk.RIGHT)
        run_button.pack(side=tk.LEFT, padx=(0, 5))
        cancel_button.pack(side=tk.LEFT)

        self.fit_dialog_to_screen(dialog, min_width=700, min_height=560)
        dialog.protocol("WM_DELETE_WINDOW", dialog.destroy)

    def export_debug_log(self):
        """Export debug log to file"""
        filename = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            title="Export Debug Log"
        )
        
        if filename:
            try:
                with open(filename, 'w') as f:
                    if self.debug_log_buffer:
                        f.writelines(self.debug_log_buffer)
                    elif hasattr(self, 'debug_text'):
                        f.write(self.debug_text.get(1.0, tk.END))
                messagebox.showinfo("Success", f"Debug log exported to {filename}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to export debug log: {str(e)}")

    def clear_debug_log(self):
        """Clear debug log"""
        self.debug_log_buffer.clear()
        if hasattr(self, 'debug_text'):
            self.debug_text.delete(1.0, tk.END)

    def export_api_calls(self):
        """Export API calls to JSON file"""
        filename = filedialog.asksaveasfilename(
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
            title="Export API Calls"
        )
        
        if filename:
            try:
                calls = self.api_logger.get_all_calls()
                with open(filename, 'w') as f:
                    json.dump(calls, f, indent=2, default=str)
                messagebox.showinfo("Success", f"API calls exported to {filename}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to export API calls: {str(e)}")

    def clear_api_calls(self):
        """Clear API call log"""
        self.api_logger.clear_calls()

    def create_main_ui(self):
        """Create the main UI layout"""
        # Status bar (create early so it's available for other methods)
        self.status_frame = ttk.Frame(self.root)
        self.status_frame.pack(side=tk.BOTTOM, fill=tk.X)
        self.status_bar = ttk.Label(self.status_frame, text="Ready", relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.status_progress = ttk.Progressbar(self.status_frame, mode="indeterminate", length=140)
        
        # Main container
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Split panes
        self.main_pane = ttk.Panedwindow(main_frame, orient=tk.HORIZONTAL)
        self.main_pane.pack(fill=tk.BOTH, expand=True)

        # Left panel (Endpoints and Buckets)
        self.left_panel = ttk.Frame(self.main_pane, width=280)
        self.left_panel.pack_propagate(False)

        self.left_pane = ttk.Panedwindow(self.left_panel, orient=tk.VERTICAL)
        self.left_pane.pack(fill=tk.BOTH, expand=True)

        # Endpoint selector
        self.endpoint_frame = ttk.LabelFrame(self.left_pane, text="Endpoint", padding=5)
        
        self.endpoint_var = tk.StringVar()
        self.endpoint_combo = ttk.Combobox(self.endpoint_frame, textvariable=self.endpoint_var, state="readonly")
        self.endpoint_combo.pack(fill=tk.X)
        self.endpoint_combo.bind("<<ComboboxSelected>>", self.on_endpoint_selected)
        
        # Buckets list
        self.bucket_frame = ttk.LabelFrame(self.left_pane, text="Buckets", padding=5)
        
        # Bucket toolbar
        self.bucket_toolbar = ttk.Frame(self.bucket_frame)
        self.bucket_toolbar.pack(fill=tk.X, pady=(0, 5))
        
        ttk.Button(self.bucket_toolbar, text="Create", command=self.create_bucket).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(self.bucket_toolbar, text="Delete", command=self.delete_bucket).pack(side=tk.LEFT)
        ttk.Button(self.bucket_toolbar, text="Refresh", command=self.refresh_buckets).pack(side=tk.RIGHT)
        
        # Bucket listbox with scrollbar
        bucket_scroll = ttk.Scrollbar(self.bucket_frame)
        bucket_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.bucket_listbox = tk.Listbox(
            self.bucket_frame,
            yscrollcommand=bucket_scroll.set,
            activestyle="none",
            highlightthickness=0,
            selectborderwidth=0,
            borderwidth=0,
            relief="flat"
        )
        self.bucket_listbox.pack(fill=tk.BOTH, expand=True)
        bucket_scroll.config(command=self.bucket_listbox.yview)
        self.bucket_listbox.bind("<<ListboxSelect>>", self.on_bucket_selected)
        
        # Bucket right-click menu
        self.bucket_menu = tk.Menu(self.root, tearoff=0)
        self.bucket_menu.add_command(label="Bucket Lifecycle Policy", command=self.manage_lifecycle_policy)
        self.bucket_menu.add_separator()
        self.bucket_menu.add_command(label="Enable Versioning", command=lambda: self.toggle_versioning(True))
        self.bucket_menu.add_command(label="Suspend Versioning", command=lambda: self.toggle_versioning(False))
        self.bucket_menu.add_separator()
        self.bucket_menu.add_command(label="Delete Bucket", command=self.delete_bucket)
        self.bucket_listbox.bind("<Button-3>", self.show_bucket_menu)
        
        self.left_pane.add(self.endpoint_frame, weight=0)
        self.left_pane.add(self.bucket_frame, weight=1)

        # Right panel (Objects and details)
        self.right_panel = ttk.Frame(self.main_pane)
        self.right_panel.pack_propagate(False)

        self.right_pane = ttk.Panedwindow(self.right_panel, orient=tk.VERTICAL)
        self.right_pane.pack(fill=tk.BOTH, expand=True)
        
        # Objects list
        self.objects_frame = ttk.LabelFrame(self.right_pane, text="Objects", padding=5)
        
        # Breadcrumb navigation
        self.breadcrumb_frame = ttk.Frame(self.objects_frame)
        self.breadcrumb_frame.pack(fill=tk.X, pady=(0, 5))
        
        self.breadcrumb_label = ttk.Label(self.breadcrumb_frame, text="Location: /")
        self.breadcrumb_label.pack(side=tk.LEFT)
        
        # Filter frame
        self.filter_frame = ttk.Frame(self.objects_frame)
        self.filter_frame.pack(fill=tk.X, pady=(0, 5))
        
        # Back button for navigation
        ttk.Button(self.filter_frame, text="↑ Up", command=self.navigate_up).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Label(self.filter_frame, text="Prefix:").pack(side=tk.LEFT, padx=(0, 5))
        
        self.prefix_var = tk.StringVar()
        self.prefix_entry = ttk.Entry(self.filter_frame, textvariable=self.prefix_var)
        self.prefix_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        self.prefix_entry.bind("<Return>", lambda e: self.apply_prefix_filter())
        
        ttk.Button(self.filter_frame, text="Filter", command=self.apply_prefix_filter).pack(side=tk.LEFT)
        
        # Create a frame for the treeview with grid layout
        tree_frame = ttk.Frame(self.objects_frame)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        
        # Objects treeview with hidden fullpath column
        self.objects_tree = ttk.Treeview(
            tree_frame,
            columns=("Size", "Modified", "Storage Class", "fullpath"),
            show="tree headings",
            displaycolumns=("Size", "Modified", "Storage Class"),
            selectmode="extended"
        )
        self.objects_tree.heading("#0", text="Name")
        self.objects_tree.heading("Size", text="Size")
        self.objects_tree.heading("Modified", text="Modified")
        self.objects_tree.heading("Storage Class", text="Storage Class")
        
        # Column widths
        self.objects_tree.column("#0", width=400)
        self.objects_tree.column("Size", width=100)
        self.objects_tree.column("Modified", width=200)
        self.objects_tree.column("Storage Class", width=150)
        self.objects_tree.column("fullpath", width=0, stretch=False)  # Hidden column
        
        # Scrollbars for treeview
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.objects_tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.objects_tree.xview)
        self.objects_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        self.objects_tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)
        
        # Right-click menu for objects
        self.object_menu = tk.Menu(self.root, tearoff=0)
        self.object_menu.add_command(label="Download", command=self.download_object)
        self.object_menu.add_command(label="Delete", command=self.delete_object)
        self.object_menu.add_command(label="HEAD", command=self.show_object_metadata)
        self.object_menu.add_separator()
        self.object_menu.add_command(label="Copy Key", command=self.copy_object_key)
        
        self.objects_tree.bind("<Button-3>", self.show_object_menu)
        self.objects_tree.bind("<Double-1>", self.on_object_double_click)
        self.objects_tree.bind("<<TreeviewSelect>>", self.on_object_selected)
        
        # Action bar
        self.action_frame = ttk.Frame(self.objects_frame)
        self.action_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(self.action_frame, text="Download", command=self.download_object).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(self.action_frame, text="Upload", command=self.upload_files).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(self.action_frame, text="Create Folder", command=self.create_folder).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(self.action_frame, text="Refresh", command=self.refresh_objects).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(self.action_frame, text="Load More", command=self.load_more_objects).pack(side=tk.RIGHT)
        
        # Details notebook
        self.details_container = ttk.Frame(self.right_pane)

        self.details_pane = ttk.Panedwindow(self.details_container, orient=tk.VERTICAL)
        self.details_pane.pack(fill=tk.BOTH, expand=True)

        self.details_notebook = ttk.Notebook(self.details_pane)
        self.details_pane.add(self.details_notebook, weight=3)
        
        # Metadata tab (formerly HEAD)
        self.metadata_frame = ttk.Frame(self.details_notebook)
        self.details_notebook.add(self.metadata_frame, text="Metadata")
        
        self.metadata_text = tk.Text(self.metadata_frame, height=10, wrap=tk.WORD)
        metadata_scroll = ttk.Scrollbar(self.metadata_frame, command=self.metadata_text.yview)
        self.metadata_text.configure(yscrollcommand=metadata_scroll.set)
        self.metadata_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        metadata_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Headers tab
        self.headers_frame = ttk.Frame(self.details_notebook)
        self.details_notebook.add(self.headers_frame, text="Headers")
        
        self.headers_text = tk.Text(self.headers_frame, height=10, wrap=tk.WORD)
        headers_scroll = ttk.Scrollbar(self.headers_frame, command=self.headers_text.yview)
        self.headers_text.configure(yscrollcommand=headers_scroll.set)
        self.headers_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        headers_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Tags tab
        self.tags_frame = ttk.Frame(self.details_notebook)
        self.details_notebook.add(self.tags_frame, text="Tags")
        
        self.tags_tree = ttk.Treeview(self.tags_frame, columns=("Value",), show="tree headings")
        self.tags_tree.heading("#0", text="Key")
        self.tags_tree.heading("Value", text="Value")
        self.tags_tree.pack(fill=tk.BOTH, expand=True)
        
        # Enhanced Versions tab
        self.versions_frame = ttk.Frame(self.details_notebook)
        self.details_notebook.add(self.versions_frame, text="Versions")
        
        # Versions controls
        versions_controls = ttk.Frame(self.versions_frame)
        versions_controls.pack(fill=tk.X, pady=5)
        
        ttk.Button(versions_controls, text="Show All Versions", command=self.show_all_versions).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(versions_controls, text="Refresh Versions", command=self.refresh_versions).pack(side=tk.LEFT)
        ttk.Button(versions_controls, text="Download Selected", command=self.download_selected_versions).pack(side=tk.LEFT, padx=(5, 0))
        ttk.Button(versions_controls, text="Delete Selected", command=self.delete_selected_versions).pack(side=tk.LEFT, padx=(5, 0))

        # Versions filter controls
        versions_filter_frame = ttk.Frame(self.versions_frame)
        versions_filter_frame.pack(fill=tk.X, pady=(0, 5))

        ttk.Label(versions_filter_frame, text="Prefix:").grid(row=0, column=0, sticky=tk.W)
        self.versions_prefix_entry = ttk.Entry(versions_filter_frame, textvariable=self.versions_prefix_var)
        self.versions_prefix_entry.grid(row=0, column=1, sticky="ew", padx=(5, 10))
        ttk.Label(versions_filter_frame, text="Text/Regex:").grid(row=0, column=2, sticky=tk.W)
        self.versions_filter_entry = ttk.Entry(versions_filter_frame, textvariable=self.versions_filter_var)
        self.versions_filter_entry.grid(row=0, column=3, sticky="ew", padx=(5, 10))
        ttk.Checkbutton(versions_filter_frame, text="Regex", variable=self.versions_regex_var).grid(row=0, column=4, sticky=tk.W)
        ttk.Button(versions_filter_frame, text="Apply", command=self.apply_versions_filters).grid(row=0, column=5, sticky=tk.W, padx=(5, 0))

        ttk.Checkbutton(
            versions_filter_frame,
            text="Show Versions",
            variable=self.versions_show_versions_var,
            command=self.apply_versions_filters
        ).grid(row=1, column=0, sticky=tk.W, pady=(5, 0))
        ttk.Checkbutton(
            versions_filter_frame,
            text="Show Delete Markers",
            variable=self.versions_show_delete_markers_var,
            command=self.apply_versions_filters
        ).grid(row=1, column=1, sticky=tk.W, padx=(5, 10), pady=(5, 0))
        ttk.Button(versions_filter_frame, text="Clear", command=self.clear_versions_filters).grid(row=1, column=5, sticky=tk.W, padx=(5, 0), pady=(5, 0))

        versions_filter_frame.grid_columnconfigure(1, weight=1)
        versions_filter_frame.grid_columnconfigure(3, weight=1)

        self.versions_prefix_entry.bind("<Return>", lambda e: self.apply_versions_filters())
        self.versions_filter_entry.bind("<Return>", lambda e: self.apply_versions_filters())
        
        self.versions_tree = ttk.Treeview(
            self.versions_frame,
            columns=("Size", "Modified", "Latest", "Key", "VersionId", "IsDeleteMarker"),
            show="tree headings",
            displaycolumns=("Size", "Modified", "Latest", "Key"),
            selectmode="extended"
        )
        self.versions_tree.heading("#0", text="Version ID")
        self.versions_tree.heading("Size", text="Size")
        self.versions_tree.heading("Modified", text="Modified")
        self.versions_tree.heading("Latest", text="Latest")
        self.versions_tree.heading("Key", text="Object Key")
        self.versions_tree.column("VersionId", width=0, stretch=False)
        self.versions_tree.column("IsDeleteMarker", width=0, stretch=False)
        
        # Versions scrollbar
        versions_scroll = ttk.Scrollbar(self.versions_frame, command=self.versions_tree.yview)
        self.versions_tree.configure(yscrollcommand=versions_scroll.set)
        self.versions_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        versions_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        self.versions_summary_label = ttk.Label(self.versions_frame, text="Displayed: 0 entries")
        self.versions_summary_label.pack(fill=tk.X, pady=(5, 0))

        # Versions right-click menu
        self.versions_menu = tk.Menu(self.root, tearoff=0)
        self.versions_menu.add_command(label="Download Selected", command=self.download_selected_versions)
        self.versions_menu.add_command(label="Delete Selected", command=self.delete_selected_versions)
        self.versions_menu.add_separator()
        self.versions_menu.add_command(label="Copy Key", command=self.copy_version_key)
        self.versions_menu.add_command(label="Copy Version ID", command=self.copy_version_id)

        self.versions_tree.bind("<Button-3>", self.show_versions_menu)
        
        # Presigned URL tab
        self.presigned_frame = ttk.Frame(self.details_notebook)
        self.details_notebook.add(self.presigned_frame, text="Presigned URL")
        
        presigned_controls = ttk.Frame(self.presigned_frame)
        presigned_controls.pack(fill=tk.X, pady=5)
        
        ttk.Label(presigned_controls, text="Expiration (seconds):").pack(side=tk.LEFT, padx=(5, 5))
        self.expiration_var = tk.StringVar(value="3600")
        ttk.Entry(presigned_controls, textvariable=self.expiration_var, width=10).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(presigned_controls, text="Generate", command=self.generate_presigned_url).pack(side=tk.LEFT)
        
        self.presigned_text = tk.Text(self.presigned_frame, height=5, wrap=tk.WORD)
        presigned_scroll = ttk.Scrollbar(self.presigned_frame, command=self.presigned_text.yview)
        self.presigned_text.configure(yscrollcommand=presigned_scroll.set)
        self.presigned_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)
        presigned_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Debug panel (initially hidden)
        self.debug_frame = ttk.LabelFrame(self.details_pane, text="Debug Info", padding=5)
        self.debug_text = tk.Text(self.debug_frame, height=5, wrap=tk.WORD)
        debug_scroll = ttk.Scrollbar(self.debug_frame, command=self.debug_text.yview)
        self.debug_text.configure(yscrollcommand=debug_scroll.set)
        self.debug_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        debug_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        self.right_pane.add(self.objects_frame, weight=3)
        self.right_pane.add(self.details_container, weight=2)

        self.main_pane.add(self.left_panel, weight=1)
        self.main_pane.add(self.right_panel, weight=4)

    def show_all_versions(self):
        """Show all versions of all objects in the bucket"""
        if not self.current_bucket:
            return
        prefix = self.versions_prefix_var.get().strip()
        self.load_versions(prefix=prefix or None)

    def load_versions(self, prefix=None, selected_key=None):
        """Load versions into cache, then apply filters"""
        if not self.current_bucket:
            return

        self.clear_versions_tree()
        self.versions_data = []
        self.set_status("Loading object versions...", busy=True)

        def load_versions_thread():
            try:
                params = {'Bucket': self.current_bucket}
                if prefix:
                    params['Prefix'] = prefix
                paginator = self.s3_client.get_paginator('list_object_versions')
                page_iterator = paginator.paginate(**params, PaginationConfig={'PageSize': 1000})
                versions = []

                for page in page_iterator:
                    for version in page.get('Versions', []):
                        if selected_key and version.get('Key') != selected_key:
                            continue
                        versions.append(self.build_version_entry(version, False))
                    for delete_marker in page.get('DeleteMarkers', []):
                        if selected_key and delete_marker.get('Key') != selected_key:
                            continue
                        versions.append(self.build_version_entry(delete_marker, True))
                    if self.request_delay_ms.get():
                        time.sleep(self.request_delay_ms.get() / 1000.0)

                self.root.after(0, lambda: self.set_versions_data(versions))
                self.root.after(0, lambda: self.set_status(f"Loaded {len(versions)} versions", busy=False))
            except Exception as e:
                self.root.after(0, lambda: self.set_status(f"Error loading versions: {str(e)}", busy=False))
                self.root.after(0, lambda: messagebox.showerror("Error", f"Failed to load versions: {str(e)}"))
                self.root.after(0, lambda: self.log_debug(f"Error loading versions: {e}"))

        threading.Thread(target=load_versions_thread, daemon=True).start()

    def clear_versions_tree(self):
        """Clear versions tree and summary"""
        if hasattr(self, 'versions_tree'):
            self.versions_tree.delete(*self.versions_tree.get_children())
        if hasattr(self, 'versions_summary_label'):
            self.versions_summary_label.config(text="Displayed: 0 entries")

    def set_versions_data(self, versions):
        """Set cached versions data and apply filters"""
        self.versions_data = versions
        self.apply_versions_filters()

    def build_version_entry(self, version, is_delete_marker):
        """Normalize version data for rendering"""
        return {
            'version_id': version.get('VersionId', 'null'),
            'key': version.get('Key', ''),
            'size': version.get('Size', 0),
            'last_modified': version.get('LastModified', datetime.now()),
            'is_latest': bool(version.get('IsLatest', False)),
            'is_delete_marker': bool(is_delete_marker)
        }

    def apply_versions_filters(self):
        """Apply version filters to cached data"""
        if not hasattr(self, 'versions_data'):
            return

        prefix = self.versions_prefix_var.get().strip()
        text_filter = self.versions_filter_var.get().strip()
        use_regex = self.versions_regex_var.get()
        show_versions = self.versions_show_versions_var.get()
        show_delete_markers = self.versions_show_delete_markers_var.get()

        regex = None
        if text_filter and use_regex:
            try:
                regex = re.compile(text_filter)
            except re.error as e:
                messagebox.showerror("Regex Error", f"Invalid regex: {e}")
                return

        filtered = []
        for entry in self.versions_data:
            if prefix and not entry['key'].startswith(prefix):
                continue
            if entry['is_delete_marker'] and not show_delete_markers:
                continue
            if not entry['is_delete_marker'] and not show_versions:
                continue
            if text_filter:
                haystack = f"{entry['key']} {entry['version_id']}"
                if regex:
                    if not regex.search(haystack):
                        continue
                else:
                    if text_filter not in haystack:
                        continue
            filtered.append(entry)

        self.render_versions(filtered)

    def clear_versions_filters(self):
        """Clear version filters"""
        self.versions_prefix_var.set("")
        self.versions_filter_var.set("")
        self.versions_regex_var.set(False)
        self.versions_show_delete_markers_var.set(True)
        self.versions_show_versions_var.set(True)
        self.apply_versions_filters()

    def render_versions(self, versions):
        """Render versions to the treeview in chunks"""
        self.clear_versions_tree()

        def render_chunk(start=0, chunk_size=200):
            end = min(start + chunk_size, len(versions))
            for entry in versions[start:end]:
                self.add_version_to_tree(entry)
            if end < len(versions):
                self.root.after(1, render_chunk, end, chunk_size)
            else:
                self.update_versions_summary(versions)

        render_chunk()

    def update_versions_summary(self, versions):
        """Update versions summary label"""
        total = len(versions)
        delete_markers = sum(1 for v in versions if v['is_delete_marker'])
        versions_count = total - delete_markers
        objects = len({v['key'] for v in versions})
        if hasattr(self, 'versions_summary_label'):
            self.versions_summary_label.config(
                text=f"Displayed: {total} entries | Objects: {objects} | Versions: {versions_count} | Delete markers: {delete_markers}"
            )

    def add_version_to_tree(self, entry):
        """Add a version entry to the versions tree"""
        try:
            version_id = entry.get('version_id', 'null')
            key = entry.get('key', '')
            is_delete_marker = entry.get('is_delete_marker', False)
            last_modified = entry.get('last_modified', datetime.now())
            is_latest = entry.get('is_latest', False)

            if is_delete_marker:
                display_id = f"{version_id} (Delete Marker)"
                size_display = "Delete Marker"
            else:
                display_id = version_id
                size_display = self.format_size(entry.get('size', 0))

            self.versions_tree.insert(
                "", tk.END,
                text=display_id,
                values=(
                    size_display,
                    last_modified.strftime("%Y-%m-%d %H:%M:%S"),
                    "Yes" if is_latest else "No",
                    key,
                    version_id,
                    is_delete_marker
                )
            )
        except Exception as e:
            self.log_debug(f"Error adding version to tree: {e}")

    def refresh_versions(self):
        """Refresh versions for selected object or all objects if none selected"""
        selection = self.objects_tree.selection()
        if selection:
            # Refresh for selected object
            self.show_object_versions()
        else:
            # Refresh all versions
            self.show_all_versions()

    def download_selected_versions(self):
        """Download selected versions"""
        if not self.current_bucket:
            return

        entries = self.get_selected_version_entries()
        if not entries:
            messagebox.showwarning("Warning", "Please select one or more versions")
            return

        downloadable = [e for e in entries if not e.get('is_delete_marker')]
        if not downloadable:
            messagebox.showwarning("Warning", "Delete markers cannot be downloaded")
            return
        if len(downloadable) < len(entries):
            messagebox.showinfo("Info", "Delete markers were skipped for download")

        if len(downloadable) == 1:
            self.download_single_version(downloadable[0])
        else:
            self.download_multiple_versions(downloadable)

    def download_single_version(self, entry):
        """Download a single object version"""
        key = entry['key']
        version_id = entry['version_id']
        filename = os.path.basename(key.rstrip('/')) or "object"
        default_name = self.add_version_suffix(filename, version_id)
        filepath = filedialog.asksaveasfilename(
            defaultextension="",
            initialfile=default_name,
            title="Save Object Version As"
        )
        if not filepath:
            return

        def download_thread():
            try:
                self.s3_client.download_file(
                    self.current_bucket,
                    key,
                    filepath,
                    ExtraArgs={'VersionId': version_id}
                )
                self.root.after(0, lambda: messagebox.showinfo("Success", f"Downloaded version of '{key}'"))
                self.log_debug(f"Downloaded version {version_id} of {key} to {filepath}")
            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("Error", f"Failed to download version: {str(e)}"))
                self.log_debug(f"Error downloading version {version_id} of {key}: {e}")

        self.executor.submit(download_thread)

    def download_multiple_versions(self, entries):
        """Download multiple object versions to a directory"""
        dest_dir = filedialog.askdirectory(title="Select download folder")
        if not dest_dir:
            return

        progress_dialog = tk.Toplevel(self.root)
        progress_dialog.title("Downloading Versions...")
        progress_dialog.geometry("520x220")
        progress_dialog.transient(self.root)
        progress_dialog.grab_set()

        overall_label = ttk.Label(progress_dialog, text=f"Downloading {len(entries)} versions...")
        overall_label.pack(pady=10)

        overall_progress = ttk.Progressbar(progress_dialog, maximum=len(entries))
        overall_progress.pack(fill=tk.X, padx=20, pady=5)

        current_label = ttk.Label(progress_dialog, text="")
        current_label.pack(pady=5)

        status_label = ttk.Label(progress_dialog, text="Preparing downloads...")
        status_label.pack(pady=5)

        cancel_var = tk.BooleanVar()
        ttk.Button(progress_dialog, text="Cancel", command=lambda: cancel_var.set(True)).pack(pady=5)

        def download_batch():
            errors = []
            for index, entry in enumerate(entries, start=1):
                if cancel_var.get():
                    break
                key = entry['key']
                version_id = entry['version_id']
                self.root.after(0, lambda k=key: current_label.config(text=f"Downloading: {k}"))
                safe_key = key.lstrip("/")
                dest_path = os.path.join(dest_dir, safe_key.replace("/", os.sep))
                dest_path = self.add_version_suffix(dest_path, version_id)
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                try:
                    self.s3_client.download_file(
                        self.current_bucket,
                        key,
                        dest_path,
                        ExtraArgs={'VersionId': version_id}
                    )
                    self.log_debug(f"Downloaded version {version_id} of {key} to {dest_path}")
                except Exception as e:
                    if cancel_var.get():
                        break
                    errors.append((key, str(e)))
                    self.log_debug(f"Error downloading version {version_id} of {key}: {e}")
                self.root.after(0, lambda i=index: overall_progress.config(value=i))
                self.root.after(0, lambda i=index: status_label.config(text=f"Completed {i}/{len(entries)}"))

            self.root.after(0, progress_dialog.destroy)
            if cancel_var.get():
                self.root.after(0, lambda: messagebox.showinfo("Cancelled", "Download cancelled by user"))
                return
            if errors:
                error_summary = "\n".join([f"{k}: {err}" for k, err in errors[:10]])
                if len(errors) > 10:
                    error_summary += f"\n...and {len(errors) - 10} more"
                self.root.after(0, lambda: messagebox.showerror("Download Errors", error_summary))
            else:
                self.root.after(0, lambda: messagebox.showinfo("Success", "All selected versions downloaded"))

        self.executor.submit(download_batch)

    def delete_selected_versions(self):
        """Delete selected object versions"""
        if not self.current_bucket:
            return

        entries = self.get_selected_version_entries()
        if not entries:
            messagebox.showwarning("Warning", "Please select one or more versions")
            return

        if len(entries) == 1:
            confirm = messagebox.askyesno(
                "Confirm",
                f"Delete version '{entries[0]['version_id']}' of '{entries[0]['key']}'?"
            )
        else:
            confirm = messagebox.askyesno("Confirm", f"Delete {len(entries)} selected versions?")
        if not confirm:
            return

        def delete_in_thread():
            try:
                errors = []
                for batch in self.chunk_list(entries, 1000):
                    delete_objects = [{'Key': e['key'], 'VersionId': e['version_id']} for e in batch]
                    response = self.safe_api_call(
                        self.s3_client.delete_objects,
                        Bucket=self.current_bucket,
                        Delete={'Objects': delete_objects, 'Quiet': True}
                    )
                    errors.extend(response.get('Errors', []))
                    if self.request_delay_ms.get():
                        time.sleep(self.request_delay_ms.get() / 1000.0)

                removed = {(e['key'], e['version_id']) for e in entries}
                self.versions_data = [
                    v for v in self.versions_data
                    if (v['key'], v['version_id']) not in removed
                ]
                self.root.after(0, self.apply_versions_filters)
                self.root.after(0, self.refresh_objects)

                if errors:
                    error_summary = "\n".join([f"{e.get('Key')}: {e.get('Message')}" for e in errors[:10]])
                    if len(errors) > 10:
                        error_summary += f"\n...and {len(errors) - 10} more"
                    self.root.after(0, lambda: messagebox.showerror("Delete Errors", error_summary))
                else:
                    self.root.after(0, lambda: messagebox.showinfo("Success", "Deleted selected versions"))
                self.log_debug(f"Deleted {len(entries)} versions")
            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("Error", f"Failed to delete versions: {str(e)}"))
                self.log_debug(f"Error deleting versions: {e}")

        self.executor.submit(delete_in_thread)

    def apply_theme(self):
        """Apply the selected theme"""
        theme = "dark" if self.dark_mode.get() else "light"
        colors = self.themes[theme]
        
        # Configure root window
        self.root.configure(bg=colors["bg"])
        
        # Configure ttk styles
        self.style.configure("TFrame", background=colors["frame_bg"])
        self.style.configure("TLabelFrame", background=colors["frame_bg"], foreground=colors["fg"])
        self.style.configure("TLabelFrame.Label", background=colors["frame_bg"], foreground=colors["fg"], font=self.heading_font)
        self.style.configure("TLabel", background=colors["frame_bg"], foreground=colors["fg"])
        self.style.configure("TButton", background=colors["button_bg"], foreground=colors["fg"])
        self.style.map("TButton", background=[("active", colors["select_bg"])])
        self.style.configure("TEntry", fieldbackground=colors["entry_bg"], foreground=colors["fg"])
        self.style.configure("TCombobox", fieldbackground=colors["entry_bg"], foreground=colors["fg"])
        self.style.configure("TNotebook", background=colors["frame_bg"], borderwidth=0)
        self.style.configure("TNotebook.Tab", background=colors["button_bg"], foreground=colors["fg"])
        self.style.map("TNotebook.Tab", background=[("selected", colors["frame_bg"])])
        
        # Configure Treeview
        self.style.configure("Treeview", background=colors["bg"], foreground=colors["fg"],
                           fieldbackground=colors["bg"], bordercolor=colors["border"])
        self.style.configure("Treeview.Heading", background=colors["button_bg"], foreground=colors["fg"], font=self.heading_font)
        self.style.map("Treeview", background=[("selected", self.accent_color)])

        self.style.configure("TProgressbar", background=self.accent_color, troughcolor=colors["entry_bg"])
        
        # Configure text widgets
        for widget in [self.metadata_text, self.headers_text, self.presigned_text, self.debug_text]:
            widget.configure(bg=colors["entry_bg"], fg=colors["fg"], insertbackground=colors["fg"])
        
        # Configure listbox
        self.bucket_listbox.configure(
            bg=colors["entry_bg"],
            fg=colors["fg"],
            selectbackground=self.accent_color,
            selectforeground="#FFFFFF"
        )
        
        # Configure status bar if it exists
        if self.status_bar:
            self.style.configure("TLabel", background=colors["frame_bg"], foreground=colors["fg"])

    def toggle_flat_view(self):
        """Toggle between hierarchical and flat view"""
        self.refresh_objects()

    def toggle_debug(self):
        """Toggle debug panel visibility"""
        if self.debug_mode.get():
            if self.debug_frame not in self.details_pane.panes():
                self.details_pane.add(self.debug_frame, weight=1)
            self.log_debug("Debug mode enabled")
            self.refresh_debug_text()
            self.enforce_layout_minsizes()
        else:
            if self.debug_frame in self.details_pane.panes():
                self.details_pane.forget(self.debug_frame)
            self.enforce_layout_minsizes()

    def log_debug(self, message):
        """Log debug message"""
        self.append_debug_message(message, with_timestamp=True)

    def append_debug_message(self, message, with_timestamp=True):
        """Append debug message to buffer and optionally to UI."""
        if with_timestamp:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            line = f"[{timestamp}] {message}"
        else:
            line = message.rstrip("\n")

        if not line.endswith("\n"):
            line += "\n"
        self.debug_log_buffer.append(line)

        if not (self.debug_mode.get() and hasattr(self, 'debug_text')):
            return

        def append():
            self.debug_text.insert(tk.END, line)
            self.debug_text.see(tk.END)

        if threading.current_thread() is threading.main_thread():
            append()
        else:
            self.root.after(0, append)

    def refresh_debug_text(self):
        """Refresh debug text from buffer."""
        if not (self.debug_mode.get() and hasattr(self, 'debug_text')):
            return
        self.debug_text.delete(1.0, tk.END)
        for line in self.debug_log_buffer:
            self.debug_text.insert(tk.END, line)
        self.debug_text.see(tk.END)

    def set_status(self, message, busy=False):
        """Update status bar and optional progress indicator"""
        def update():
            if hasattr(self, 'status_bar'):
                if busy:
                    self.status_message_base = message
                    self.status_bar.config(text=self.format_status_text())
                else:
                    self.status_message_base = message
                    self.status_bar.config(text=message)
            if hasattr(self, 'status_progress'):
                if busy:
                    if not self.status_progress.winfo_ismapped():
                        self.status_progress.pack(side=tk.RIGHT, padx=5)
                    self.status_progress.start(10)
                else:
                    self.status_progress.stop()
                    if self.status_progress.winfo_ismapped():
                        self.status_progress.pack_forget()
            self.status_busy = busy
            if busy:
                self.start_status_animation()
            else:
                self.stop_status_animation()

        if threading.current_thread() is threading.main_thread():
            update()
        else:
            self.root.after(0, update)

    def setup_responsive_layout(self):
        """Set minimum window size and adjust button padding on resize."""
        self.root.update_idletasks()
        base_width = max(1, self.root.winfo_width())
        base_height = max(1, self.root.winfo_height())

        self.compact_threshold = int(base_width * 0.7)
        self.compact_buttons = False
        self.enforce_layout_minsizes()
        self.root.bind("<Configure>", self.on_window_resize)

    def on_window_resize(self, event):
        """Compact buttons when window width is small."""
        if event.widget is not self.root:
            return
        compact = event.width <= self.compact_threshold
        if compact == self.compact_buttons:
            return
        self.compact_buttons = compact
        padding = self.button_padding_compact if compact else self.button_padding_normal
        self.style.configure("TButton", padding=padding)

    def enforce_layout_minsizes(self):
        """Prevent panes and window from shrinking below usable sizes."""
        self.root.update_idletasks()

        def req_size(widget, min_value):
            try:
                return max(min_value, widget.winfo_reqwidth(), 1)
            except Exception:
                return min_value

        def req_height(widget, min_value):
            try:
                return max(min_value, widget.winfo_reqheight(), 1)
            except Exception:
                return min_value

        toolbar_w = req_size(self.bucket_toolbar, 240)
        endpoint_w = req_size(self.endpoint_frame, 240)
        left_min_w = max(260, toolbar_w + 20, endpoint_w + 20)

        action_w = req_size(self.action_frame, 300)
        right_min_w = max(560, req_size(self.right_panel, 520), action_w + 40)
        min_root_w = left_min_w + right_min_w + 40

        top_min_h = max(120, req_height(self.endpoint_frame, 120))
        bucket_min_h = max(240, req_height(self.bucket_frame, 220))

        header_h = (
            req_height(self.breadcrumb_frame, 20)
            + req_height(self.filter_frame, 30)
            + req_height(self.action_frame, 30)
            + 40
        )
        objects_min_h = max(300, header_h + 140)
        details_min_h = max(240, req_height(self.details_container, 220))
        min_root_h = max(560, objects_min_h + details_min_h + 80)

        self.root.minsize(min_root_w, min_root_h)

        try:
            self.main_pane.paneconfigure(self.left_panel, minsize=left_min_w)
            self.main_pane.paneconfigure(self.right_panel, minsize=right_min_w)
        except Exception:
            pass

        try:
            self.left_pane.paneconfigure(self.endpoint_frame, minsize=top_min_h)
            self.left_pane.paneconfigure(self.bucket_frame, minsize=bucket_min_h)
        except Exception:
            pass

        try:
            self.right_pane.paneconfigure(self.objects_frame, minsize=objects_min_h)
            self.right_pane.paneconfigure(self.details_container, minsize=details_min_h)
        except Exception:
            pass

        if hasattr(self, 'details_pane'):
            try:
                self.details_pane.paneconfigure(self.details_notebook, minsize=200)
                if self.debug_frame in self.details_pane.panes():
                    self.details_pane.paneconfigure(self.debug_frame, minsize=120)
            except Exception:
                pass

    def start_status_animation(self):
        """Start animated status dots."""
        if self.status_anim_job:
            self.root.after_cancel(self.status_anim_job)
        self.status_dots = 0
        self.status_anim_job = self.root.after(500, self.animate_status)

    def stop_status_animation(self):
        """Stop animated status dots."""
        if self.status_anim_job:
            self.root.after_cancel(self.status_anim_job)
            self.status_anim_job = None
        self.status_dots = 0

    def format_status_text(self):
        """Format status text with animated dots."""
        dots = "." * self.status_dots
        return f"{self.status_message_base}{dots}"

    def animate_status(self):
        """Animate status dots when busy."""
        if not self.status_busy or not hasattr(self, 'status_bar'):
            return
        self.status_dots = (self.status_dots + 1) % 4
        self.status_bar.config(text=self.format_status_text())
        self.status_anim_job = self.root.after(500, self.animate_status)

    def is_retryable_error(self, error):
        """Return True if the error is likely retryable"""
        if not isinstance(error, ClientError):
            return True
        code = error.response.get('Error', {}).get('Code', '')
        non_retryable = {
            'AccessDenied',
            'InvalidAccessKeyId',
            'SignatureDoesNotMatch',
            'NoSuchBucket',
            'NoSuchKey',
            'InvalidBucketName',
            'InvalidObjectState'
        }
        return code not in non_retryable

    def safe_api_call(self, func, *args, **kwargs):
        """Call S3 API with retries, delays, and basic backoff"""
        max_attempts = max(1, int(self.max_retries.get()))
        base_delay = max(0.0, float(self.retry_base_delay.get()))
        max_delay = max(base_delay, float(self.retry_max_delay.get()))
        delay_ms = max(0, int(self.request_delay_ms.get()))
        for attempt in range(1, max_attempts + 1):
            try:
                if delay_ms:
                    time.sleep(delay_ms / 1000.0)
                return func(*args, **kwargs)
            except Exception as e:
                if isinstance(e, NoCredentialsError):
                    raise
                if isinstance(e, ClientError) and not self.is_retryable_error(e):
                    raise
                if attempt >= max_attempts:
                    raise
                sleep_for = min(base_delay * (2 ** (attempt - 1)), max_delay)
                jitter = random.uniform(0, sleep_for * 0.1) if sleep_for > 0 else 0.0
                self.log_debug(f"Retrying {func.__name__} after error: {e}")
                time.sleep(sleep_for + jitter)

    def choose_accent_color(self):
        """Choose accent color"""
        from tkinter import colorchooser
        color = colorchooser.askcolor(initialcolor=self.accent_color)[1]
        if color:
            self.accent_color = color
            self.apply_theme()

    def load_config(self):
        """Load saved configuration"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                    self.endpoints = config.get('endpoints', {})
                    self.dark_mode.set(config.get('dark_mode', False))
                    self.accent_color = config.get('accent_color', "#0078D4")
                    self.multipart_threshold.set(config.get('multipart_threshold', 64))
                    self.multipart_chunksize.set(config.get('multipart_chunksize', 8))
                    self.max_concurrent_requests.set(config.get('max_concurrent_requests', 10))
                    self.api_logging_enabled.set(config.get('api_logging_enabled', False))
                    self.max_retries.set(config.get('max_retries', 3))
                    self.retry_base_delay.set(config.get('retry_base_delay', 0.5))
                    self.retry_max_delay.set(config.get('retry_max_delay', 8.0))
                    self.request_delay_ms.set(config.get('request_delay_ms', 0))
                    self.connect_timeout.set(config.get('connect_timeout', 5))
                    self.read_timeout.set(config.get('read_timeout', 60))
                    for name, endpoint in list(self.endpoints.items()):
                        endpoint.setdefault("verify_ssl", True)
                        endpoint_url = endpoint.get("endpoint_url", "")
                        parsed = urlparse(endpoint_url) if endpoint_url else None
                        scheme = endpoint.get("scheme") or (parsed.scheme if parsed else "") or "https"
                        endpoint["scheme"] = scheme
                        if endpoint_url and parsed and not parsed.scheme:
                            endpoint["endpoint_url"] = f"{scheme}://{endpoint_url}"
                    self.update_endpoint_list()
                    self.log_debug(f"Loaded {len(self.endpoints)} endpoints from config")
        except Exception as e:
            self.log_debug(f"Error loading config: {e}")

    def save_config(self):
        """Save configuration"""
        try:
            config = {
                'endpoints': self.endpoints,
                'dark_mode': self.dark_mode.get(),
                'accent_color': self.accent_color,
                'multipart_threshold': self.multipart_threshold.get(),
                'multipart_chunksize': self.multipart_chunksize.get(),
                'max_concurrent_requests': self.max_concurrent_requests.get(),
                'api_logging_enabled': self.api_logging_enabled.get(),
                'max_retries': self.max_retries.get(),
                'retry_base_delay': self.retry_base_delay.get(),
                'retry_max_delay': self.retry_max_delay.get(),
                'request_delay_ms': self.request_delay_ms.get(),
                'connect_timeout': self.connect_timeout.get(),
                'read_timeout': self.read_timeout.get()
            }
            with open(self.config_file, 'w') as f:
                json.dump(config, f, indent=2)
            self.log_debug("Configuration saved")
        except Exception as e:
            self.log_debug(f"Error saving config: {e}")

    def configure_endpoints(self):
        """Configure S3 endpoints"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Configure Endpoints")
        dialog.geometry("600x400")
        
        # Endpoints list
        list_frame = ttk.Frame(dialog)
        list_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        ttk.Label(list_frame, text="Endpoints:").pack(anchor=tk.W)
        endpoints_listbox = tk.Listbox(list_frame)
        endpoints_listbox.pack(fill=tk.BOTH, expand=True)
        
        # Populate list
        for name in self.endpoints:
            endpoints_listbox.insert(tk.END, name)
        
        # Buttons
        button_frame = ttk.Frame(list_frame)
        button_frame.pack(fill=tk.X, pady=(5, 0))
        
        def add_endpoint():
            add_dialog = tk.Toplevel(dialog)
            add_dialog.title("Add Endpoint")
            add_dialog.geometry("400x360")
            
            fields = {}
            row = 0
            for field in ["Name", "Endpoint URL", "Access Key", "Secret Key", "Region"]:
                ttk.Label(add_dialog, text=f"{field}:").grid(row=row, column=0, sticky=tk.W, padx=5, pady=5)
                entry = ttk.Entry(add_dialog, width=30)
                if field == "Secret Key":
                    entry.configure(show="*")
                elif field == "Endpoint URL":
                    entry.insert(0, "s3.amazonaws.com")
                elif field == "Region":
                    entry.insert(0, "us-east-1")
                entry.grid(row=row, column=1, padx=5, pady=5)
                fields[field] = entry
                row += 1

            scheme_var = tk.StringVar(value="https")
            ttk.Label(add_dialog, text="Scheme:").grid(row=row, column=0, sticky=tk.W, padx=5, pady=5)
            scheme_combo = ttk.Combobox(add_dialog, textvariable=scheme_var, values=["https", "http"], state="readonly", width=27)
            scheme_combo.grid(row=row, column=1, padx=5, pady=5)
            row += 1

            verify_var = tk.BooleanVar(value=True)
            ttk.Checkbutton(add_dialog, text="Verify SSL", variable=verify_var).grid(row=row, column=1, sticky=tk.W, padx=5, pady=5)
            row += 1
            
            def save_endpoint():
                name = fields["Name"].get()
                if not name:
                    messagebox.showerror("Error", "Name is required")
                    return
                endpoint_url = self.normalize_endpoint_url(fields["Endpoint URL"].get(), scheme_var.get())
                if not endpoint_url:
                    messagebox.showerror("Error", "Endpoint URL is required")
                    return
                
                self.endpoints[name] = {
                    "endpoint_url": endpoint_url,
                    "access_key": fields["Access Key"].get(),
                    "secret_key": fields["Secret Key"].get(),
                    "region": fields["Region"].get(),
                    "scheme": scheme_var.get(),
                    "verify_ssl": verify_var.get()
                }
                endpoints_listbox.insert(tk.END, name)
                self.save_config()
                self.update_endpoint_list()
                add_dialog.destroy()
            
            ttk.Button(add_dialog, text="Save", command=save_endpoint).grid(row=row, column=1, pady=10)
        
        def edit_endpoint():
            selection = endpoints_listbox.curselection()
            if not selection:
                return
            
            name = endpoints_listbox.get(selection[0])
            endpoint = self.endpoints[name]
            
            edit_dialog = tk.Toplevel(dialog)
            edit_dialog.title("Edit Endpoint")
            edit_dialog.geometry("400x360")
            
            fields = {}
            row = 0
            for field, key in [("Name", "name"), ("Endpoint URL", "endpoint_url"),
                             ("Access Key", "access_key"), ("Secret Key", "secret_key"),
                             ("Region", "region")]:
                ttk.Label(edit_dialog, text=f"{field}:").grid(row=row, column=0, sticky=tk.W, padx=5, pady=5)
                entry = ttk.Entry(edit_dialog, width=30)
                if field == "Secret Key":
                    entry.configure(show="*")
                if field == "Name":
                    entry.insert(0, name)
                else:
                    if field == "Endpoint URL":
                        entry.insert(0, self.strip_endpoint_scheme(endpoint.get(key, "")))
                    else:
                        entry.insert(0, endpoint.get(key, ""))
                entry.grid(row=row, column=1, padx=5, pady=5)
                fields[field] = entry
                row += 1

            scheme_var = tk.StringVar(value=self.get_endpoint_scheme(endpoint))
            ttk.Label(edit_dialog, text="Scheme:").grid(row=row, column=0, sticky=tk.W, padx=5, pady=5)
            scheme_combo = ttk.Combobox(edit_dialog, textvariable=scheme_var, values=["https", "http"], state="readonly", width=27)
            scheme_combo.grid(row=row, column=1, padx=5, pady=5)
            row += 1

            verify_var = tk.BooleanVar(value=endpoint.get("verify_ssl", True))
            ttk.Checkbutton(edit_dialog, text="Verify SSL", variable=verify_var).grid(row=row, column=1, sticky=tk.W, padx=5, pady=5)
            row += 1
            
            def update_endpoint():
                new_name = fields["Name"].get()
                if not new_name:
                    messagebox.showerror("Error", "Name is required")
                    return
                
                # Remove old entry if name changed
                if new_name != name:
                    del self.endpoints[name]
                    endpoints_listbox.delete(selection[0])
                    endpoints_listbox.insert(selection[0], new_name)
                
                endpoint_url = self.normalize_endpoint_url(fields["Endpoint URL"].get(), scheme_var.get())
                if not endpoint_url:
                    messagebox.showerror("Error", "Endpoint URL is required")
                    return

                self.endpoints[new_name] = {
                    "endpoint_url": endpoint_url,
                    "access_key": fields["Access Key"].get(),
                    "secret_key": fields["Secret Key"].get(),
                    "region": fields["Region"].get(),
                    "scheme": scheme_var.get(),
                    "verify_ssl": verify_var.get()
                }
                self.save_config()
                self.update_endpoint_list()
                edit_dialog.destroy()
            
            ttk.Button(edit_dialog, text="Update", command=update_endpoint).grid(row=row, column=1, pady=10)
        
        def delete_endpoint():
            selection = endpoints_listbox.curselection()
            if not selection:
                return
            
            name = endpoints_listbox.get(selection[0])
            if messagebox.askyesno("Confirm", f"Delete endpoint '{name}'?"):
                del self.endpoints[name]
                endpoints_listbox.delete(selection[0])
                self.save_config()
                self.update_endpoint_list()
        
        ttk.Button(button_frame, text="Add", command=add_endpoint).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(button_frame, text="Edit", command=edit_endpoint).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(button_frame, text="Delete", command=delete_endpoint).pack(side=tk.LEFT)
        
        ttk.Button(dialog, text="Close", command=dialog.destroy).pack(pady=10)

    def normalize_endpoint_url(self, endpoint_url, scheme):
        """Normalize endpoint URL with the selected scheme"""
        endpoint_url = (endpoint_url or "").strip()
        if not endpoint_url:
            return ""
        parsed = urlparse(endpoint_url)
        if parsed.scheme:
            netloc = parsed.netloc or parsed.path
            path = parsed.path if parsed.netloc else ""
            query = f"?{parsed.query}" if parsed.query else ""
            return f"{scheme}://{netloc}{path}{query}"
        return f"{scheme}://{endpoint_url}"

    def strip_endpoint_scheme(self, endpoint_url):
        """Remove scheme from endpoint URL for display"""
        endpoint_url = endpoint_url or ""
        parsed = urlparse(endpoint_url)
        if parsed.scheme:
            query = f"?{parsed.query}" if parsed.query else ""
            return f"{parsed.netloc}{parsed.path}{query}"
        return endpoint_url

    def get_endpoint_scheme(self, endpoint):
        """Get scheme for endpoint, falling back to URL parsing"""
        if not endpoint:
            return "https"
        scheme = endpoint.get("scheme")
        if scheme:
            return scheme
        parsed = urlparse(endpoint.get("endpoint_url", ""))
        return parsed.scheme or "https"

    def get_preferred_font_family(self):
        """Pick a modern font available on the system."""
        preferred = [
            "Segoe UI",
            "SF Pro Text",
            "Inter",
            "Ubuntu",
            "Cantarell",
            "Noto Sans",
            "DejaVu Sans",
        ]
        available = set(tkfont.families(self.root))
        for family in preferred:
            if family in available:
                return family
        return "TkDefaultFont"

    def update_endpoint_list(self):
        """Update endpoint combobox"""
        self.endpoint_combo['values'] = list(self.endpoints.keys())
        if self.endpoints and not self.current_endpoint:
            self.endpoint_combo.current(0)
            self.on_endpoint_selected(None)

    def on_endpoint_selected(self, event):
        """Handle endpoint selection"""
        endpoint_name = self.endpoint_var.get()
        if not endpoint_name:
            return
        
        self.current_endpoint = endpoint_name
        endpoint = self.endpoints[endpoint_name]

        self.set_status(f"Connecting to endpoint: {endpoint_name}...", busy=True)

        def connect_in_thread():
            try:
                s3_config = Config(
                    retries={'max_attempts': self.max_retries.get(), 'mode': 'standard'},
                    connect_timeout=self.connect_timeout.get(),
                    read_timeout=self.read_timeout.get(),
                    max_pool_connections=self.max_concurrent_requests.get()
                )

                s3_client = boto3.client(
                    's3',
                    endpoint_url=endpoint['endpoint_url'],
                    aws_access_key_id=endpoint['access_key'],
                    aws_secret_access_key=endpoint['secret_key'],
                    region_name=endpoint['region'],
                    verify=endpoint.get('verify_ssl', True),
                    config=s3_config
                )

                self.root.after(0, lambda: self.finish_endpoint_connection(endpoint_name, s3_client))
            except Exception as e:
                self.root.after(0, lambda: self.handle_endpoint_error(endpoint_name, e))

        threading.Thread(target=connect_in_thread, daemon=True).start()

    def finish_endpoint_connection(self, endpoint_name, s3_client):
        """Finalize endpoint connection on main thread"""
        if endpoint_name != self.current_endpoint:
            return
        self.s3_client = s3_client
        self.setup_api_logging()
        self.log_debug(f"Connected to endpoint: {endpoint_name}")
        self.refresh_buckets()

    def handle_endpoint_error(self, endpoint_name, error):
        """Handle endpoint connection errors"""
        if endpoint_name != self.current_endpoint:
            return
        message = str(error)
        if "WRONG_VERSION_NUMBER" in message.upper() or "WRONG_VERSION" in message.lower():
            message += "\n\nTip: Verify the endpoint scheme (http/https) and SSL verification settings."
        messagebox.showerror("Connection Error", f"Failed to connect: {message}")
        self.log_debug(f"Connection error: {error}")
        self.set_status("Connection error", busy=False)

    def refresh_buckets(self):
        """Refresh bucket list"""
        if not self.s3_client:
            return
        
        self.set_status("Loading buckets...", busy=True)
        self.bucket_listbox.delete(0, tk.END)

        def load_in_thread():
            try:
                if self.api_logging_enabled.get():
                    self.api_logger.log_api_call('list_buckets', service_id='s3', operation_name='ListBuckets')

                response = self.safe_api_call(self.s3_client.list_buckets)
                buckets = response.get('Buckets', [])

                def update_ui():
                    self.bucket_listbox.delete(0, tk.END)
                    for bucket in buckets:
                        self.bucket_listbox.insert(tk.END, bucket['Name'])
                    bucket_count = len(buckets)
                    self.set_status(f"Loaded {bucket_count} buckets", busy=False)
                    self.log_debug(f"Loaded {bucket_count} buckets")

                self.root.after(0, update_ui)
            except Exception as e:
                def handle_error():
                    self.set_status(f"Error: {str(e)}", busy=False)
                    messagebox.showerror("Error", f"Failed to list buckets: {str(e)}")
                    self.log_debug(f"Error listing buckets: {e}")
                    if self.api_logging_enabled.get():
                        self.api_logger.log_api_call('list_buckets_error', service_id='s3', operation_name='ListBuckets', error=str(e))

                self.root.after(0, handle_error)

        threading.Thread(target=load_in_thread, daemon=True).start()

    def create_bucket(self):
        """Create new bucket"""
        if not self.s3_client:
            messagebox.showwarning("Warning", "Please select an endpoint first")
            return
        
        bucket_name = simpledialog.askstring("Create Bucket", "Enter bucket name:")
        if not bucket_name:
            return
        
        try:
            # Get region from endpoint config
            region = self.endpoints[self.current_endpoint]['region']
            if region == 'us-east-1':
                self.safe_api_call(self.s3_client.create_bucket, Bucket=bucket_name)
            else:
                self.safe_api_call(
                    self.s3_client.create_bucket,
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            
            self.refresh_buckets()
            self.log_debug(f"Created bucket: {bucket_name}")
            messagebox.showinfo("Success", f"Bucket '{bucket_name}' created successfully")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to create bucket: {str(e)}")
            self.log_debug(f"Error creating bucket: {e}")

    def delete_bucket(self):
        """Delete selected bucket"""
        selection = self.bucket_listbox.curselection()
        if not selection:
            return
        
        bucket_name = self.bucket_listbox.get(selection[0])
        if not messagebox.askyesno("Confirm", f"Delete bucket '{bucket_name}'?\n\nThis action cannot be undone."):
            return
        
        try:
            self.safe_api_call(self.s3_client.delete_bucket, Bucket=bucket_name)
            self.refresh_buckets()
            self.log_debug(f"Deleted bucket: {bucket_name}")
            messagebox.showinfo("Success", f"Bucket '{bucket_name}' deleted successfully")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to delete bucket: {str(e)}")
            self.log_debug(f"Error deleting bucket: {e}")

    def show_bucket_menu(self, event):
        """Show right-click menu for buckets"""
        selection = self.bucket_listbox.curselection()
        if selection:
            self.bucket_menu.post(event.x_root, event.y_root)

    def manage_lifecycle_policy(self):
        """Manage bucket lifecycle policy with enhanced features"""
        if not self.current_bucket:
            messagebox.showwarning("No Bucket Selected", "Please select a bucket first.")
            return
        
        try:
            # Create lifecycle policy management window
            lifecycle_window = tk.Toplevel(self.root)
            lifecycle_window.title(f"Lifecycle Policy - {self.current_bucket}")
            lifecycle_window.geometry("1000x800")
            lifecycle_window.transient(self.root)
            lifecycle_window.grab_set()
            
            # Create notebook for tabs
            notebook = ttk.Notebook(lifecycle_window)
            notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
            
            # Current rules tab
            rules_frame = ttk.Frame(notebook)
            notebook.add(rules_frame, text="Current Rules")
            
            # Rules listbox
            rules_list_frame = ttk.Frame(rules_frame)
            rules_list_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
            
            self.rules_listbox = tk.Listbox(rules_list_frame)
            rules_scrollbar = ttk.Scrollbar(rules_list_frame, orient=tk.VERTICAL, command=self.rules_listbox.yview)
            self.rules_listbox.config(yscrollcommand=rules_scrollbar.set)
            
            self.rules_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            rules_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            
            # Buttons frame
            buttons_frame = ttk.Frame(rules_frame)
            buttons_frame.pack(fill=tk.X, padx=5, pady=5)
            
            ttk.Button(buttons_frame, text="Refresh", command=self.load_lifecycle_rules).pack(side=tk.LEFT, padx=5)
            ttk.Button(buttons_frame, text="Add Rule", command=lambda: self.add_lifecycle_rule(lifecycle_window)).pack(side=tk.LEFT, padx=5)
            ttk.Button(buttons_frame, text="Delete Rule", command=self.delete_lifecycle_rule).pack(side=tk.LEFT, padx=5)
            ttk.Button(buttons_frame, text="Delete All Rules", command=self.delete_all_lifecycle_rules).pack(side=tk.LEFT, padx=5)
            
            # JSON view tab
            json_frame = ttk.Frame(notebook)
            notebook.add(json_frame, text="JSON Policy")
            
            # JSON text widget
            json_text_frame = ttk.Frame(json_frame)
            json_text_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
            
            self.policy_json_text = tk.Text(json_text_frame, wrap=tk.WORD)
            json_scrollbar = ttk.Scrollbar(json_text_frame, orient=tk.VERTICAL, command=self.policy_json_text.yview)
            self.policy_json_text.config(yscrollcommand=json_scrollbar.set)
            
            self.policy_json_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            json_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            
            # JSON buttons
            json_buttons_frame = ttk.Frame(json_frame)
            json_buttons_frame.pack(fill=tk.X, padx=5, pady=5)
            
            ttk.Button(json_buttons_frame, text="Apply JSON Policy", command=self.apply_json_lifecycle_policy).pack(side=tk.LEFT, padx=5)
            ttk.Button(json_buttons_frame, text="Refresh JSON", command=self.refresh_json_policy).pack(side=tk.LEFT, padx=5)
            
            # Load current rules
            self.load_lifecycle_rules()
            self.refresh_json_policy()
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to open lifecycle policy manager: {str(e)}")
            self.log_debug(f"Lifecycle policy error: {e}")

    def load_lifecycle_rules(self):
        """Load current lifecycle rules"""
        if not self.s3_client or not self.current_bucket:
            return
        
        try:
            response = self.s3_client.get_bucket_lifecycle_configuration(Bucket=self.current_bucket)
            self.lifecycle_rules = response.get('Rules', [])
            
            if hasattr(self, 'rules_listbox') and self.rules_listbox:
                self.rules_listbox.delete(0, tk.END)
                for i, rule in enumerate(self.lifecycle_rules):
                    status = rule.get('Status', 'Unknown')
                    rule_id = rule.get('Id', f'Rule {i+1}')
                    
                    # Build description
                    description_parts = []
                    if 'Expiration' in rule:
                        if 'Days' in rule['Expiration']:
                            description_parts.append(f"Delete after {rule['Expiration']['Days']} days")
                        if 'ExpiredObjectDeleteMarker' in rule['Expiration']:
                            description_parts.append("Delete expired object markers")
                    
                    if 'Transitions' in rule:
                        for transition in rule['Transitions']:
                            if 'Days' in transition:
                                description_parts.append(f"Transition to {transition['StorageClass']} after {transition['Days']} days")
                    
                    if 'NoncurrentVersionExpiration' in rule:
                        if 'NoncurrentDays' in rule['NoncurrentVersionExpiration']:
                            description_parts.append(f"Delete non-current versions after {rule['NoncurrentVersionExpiration']['NoncurrentDays']} days")
                    
                    if 'AbortIncompleteMultipartUpload' in rule:
                        if 'DaysAfterInitiation' in rule['AbortIncompleteMultipartUpload']:
                            description_parts.append(f"Abort incomplete uploads after {rule['AbortIncompleteMultipartUpload']['DaysAfterInitiation']} days")
                    
                    description = "; ".join(description_parts) if description_parts else "No actions defined"
                    
                    # Get filter info
                    filter_info = ""
                    if 'Filter' in rule:
                        if 'Prefix' in rule['Filter']:
                            filter_info = f" (Prefix: {rule['Filter']['Prefix']})"
                        elif 'And' in rule['Filter'] and 'Prefix' in rule['Filter']['And']:
                            filter_info = f" (Prefix: {rule['Filter']['And']['Prefix']})"
                    
                    self.rules_listbox.insert(tk.END, f"{rule_id} ({status}){filter_info}: {description}")
                    
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
                self.lifecycle_rules = []
                if hasattr(self, 'rules_listbox') and self.rules_listbox:
                    self.rules_listbox.delete(0, tk.END)
                    self.rules_listbox.insert(tk.END, "No lifecycle rules configured")
            else:
                messagebox.showerror("Error", f"Failed to load lifecycle rules: {str(e)}")
                self.log_debug(f"Load lifecycle rules error: {e}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load lifecycle rules: {str(e)}")
            self.log_debug(f"Load lifecycle rules error: {e}")

    def add_lifecycle_rule(self, parent_window):
        """Add a new lifecycle rule with enhanced options"""
        dialog = tk.Toplevel(parent_window)
        dialog.title("Add Lifecycle Rule")
        dialog.geometry("600x700")
        dialog.transient(parent_window)
        dialog.grab_set()
        
        # Create scrollable frame
        canvas = tk.Canvas(dialog)
        scrollbar = ttk.Scrollbar(dialog, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Rule ID
        ttk.Label(scrollable_frame, text="Rule ID:").pack(anchor=tk.W, padx=10, pady=(10, 5))
        rule_id_var = tk.StringVar()
        ttk.Entry(scrollable_frame, textvariable=rule_id_var, width=50).pack(fill=tk.X, padx=10, pady=(0, 10))
        
        # Status
        status_frame = ttk.Frame(scrollable_frame)
        status_frame.pack(fill=tk.X, padx=10, pady=(0, 10))
        ttk.Label(status_frame, text="Status:").pack(side=tk.LEFT)
        status_var = tk.StringVar(value="Enabled")
        ttk.Radiobutton(status_frame, text="Enabled", variable=status_var, value="Enabled").pack(side=tk.LEFT, padx=(10, 5))
        ttk.Radiobutton(status_frame, text="Disabled", variable=status_var, value="Disabled").pack(side=tk.LEFT)
        
        # Filter (Prefix)
        ttk.Label(scrollable_frame, text="Prefix Filter (optional):").pack(anchor=tk.W, padx=10, pady=(10, 5))
        prefix_var = tk.StringVar()
        ttk.Entry(scrollable_frame, textvariable=prefix_var, width=50).pack(fill=tk.X, padx=10, pady=(0, 10))
        
        # Expiration
        exp_frame = ttk.LabelFrame(scrollable_frame, text="Object Expiration", padding=10)
        exp_frame.pack(fill=tk.X, padx=10, pady=(0, 10))
        
        enable_exp_var = tk.BooleanVar()
        ttk.Checkbutton(exp_frame, text="Delete objects after", variable=enable_exp_var).pack(anchor=tk.W)
        
        exp_days_frame = ttk.Frame(exp_frame)
        exp_days_frame.pack(fill=tk.X, pady=(5, 0))
        exp_days_var = tk.StringVar(value="30")
        ttk.Entry(exp_days_frame, textvariable=exp_days_var, width=10).pack(side=tk.LEFT)
        ttk.Label(exp_days_frame, text="days").pack(side=tk.LEFT, padx=(5, 0))
        
        # Enhanced: Delete expired object delete markers
        enable_delete_markers_var = tk.BooleanVar()
        ttk.Checkbutton(exp_frame, text="Delete expired object delete markers", variable=enable_delete_markers_var).pack(anchor=tk.W, pady=(5, 0))
        
        # Transition
        trans_frame = ttk.LabelFrame(scrollable_frame, text="Storage Class Transition", padding=10)
        trans_frame.pack(fill=tk.X, padx=10, pady=(0, 10))
        
        enable_trans_var = tk.BooleanVar()
        ttk.Checkbutton(trans_frame, text="Transition objects to", variable=enable_trans_var).pack(anchor=tk.W)
        
        trans_controls_frame = ttk.Frame(trans_frame)
        trans_controls_frame.pack(fill=tk.X, pady=(5, 0))
        
        trans_class_var = tk.StringVar(value="STANDARD_IA")
        trans_combo = ttk.Combobox(trans_controls_frame, textvariable=trans_class_var, 
                                  values=["STANDARD_IA", "ONEZONE_IA", "REDUCED_REDUNDANCY", "GLACIER", "DEEP_ARCHIVE"],
                                  state="readonly", width=15)
        trans_combo.pack(side=tk.LEFT)
        
        ttk.Label(trans_controls_frame, text="after").pack(side=tk.LEFT, padx=(5, 5))
        trans_days_var = tk.StringVar(value="30")
        ttk.Entry(trans_controls_frame, textvariable=trans_days_var, width=10).pack(side=tk.LEFT)
        ttk.Label(trans_controls_frame, text="days").pack(side=tk.LEFT, padx=(5, 0))
        
        # Non-current version expiration
        noncur_frame = ttk.LabelFrame(scrollable_frame, text="Non-current Version Expiration", padding=10)
        noncur_frame.pack(fill=tk.X, padx=10, pady=(0, 10))
        
        enable_noncur_var = tk.BooleanVar()
        ttk.Checkbutton(noncur_frame, text="Delete non-current versions after", variable=enable_noncur_var).pack(anchor=tk.W)
        
        noncur_days_frame = ttk.Frame(noncur_frame)
        noncur_days_frame.pack(fill=tk.X, pady=(5, 0))
        noncur_days_var = tk.StringVar(value="90")
        ttk.Entry(noncur_days_frame, textvariable=noncur_days_var, width=10).pack(side=tk.LEFT)
        ttk.Label(noncur_days_frame, text="days").pack(side=tk.LEFT, padx=(5, 0))
        
        # Enhanced: Non-current version transition
        enable_noncur_trans_var = tk.BooleanVar()
        ttk.Checkbutton(noncur_frame, text="Transition non-current versions to", variable=enable_noncur_trans_var).pack(anchor=tk.W, pady=(10, 0))
        
        noncur_trans_frame = ttk.Frame(noncur_frame)
        noncur_trans_frame.pack(fill=tk.X, pady=(5, 0))
        
        noncur_trans_class_var = tk.StringVar(value="GLACIER")
        noncur_trans_combo = ttk.Combobox(noncur_trans_frame, textvariable=noncur_trans_class_var,
                                         values=["STANDARD_IA", "ONEZONE_IA", "GLACIER", "DEEP_ARCHIVE"],
                                         state="readonly", width=15)
        noncur_trans_combo.pack(side=tk.LEFT)
        
        ttk.Label(noncur_trans_frame, text="after").pack(side=tk.LEFT, padx=(5, 5))
        noncur_trans_days_var = tk.StringVar(value="30")
        ttk.Entry(noncur_trans_frame, textvariable=noncur_trans_days_var, width=10).pack(side=tk.LEFT)
        ttk.Label(noncur_trans_frame, text="days").pack(side=tk.LEFT, padx=(5, 0))
        
        # Abort incomplete multipart uploads
        multipart_frame = ttk.LabelFrame(scrollable_frame, text="Incomplete Multipart Uploads", padding=10)
        multipart_frame.pack(fill=tk.X, padx=10, pady=(0, 10))
        
        enable_multipart_var = tk.BooleanVar()
        ttk.Checkbutton(multipart_frame, text="Abort incomplete multipart uploads after", variable=enable_multipart_var).pack(anchor=tk.W)
        
        multipart_days_frame = ttk.Frame(multipart_frame)
        multipart_days_frame.pack(fill=tk.X, pady=(5, 0))
        multipart_days_var = tk.StringVar(value="7")
        ttk.Entry(multipart_days_frame, textvariable=multipart_days_var, width=10).pack(side=tk.LEFT)
        ttk.Label(multipart_days_frame, text="days").pack(side=tk.LEFT, padx=(5, 0))
        
        # Buttons
        button_frame = ttk.Frame(scrollable_frame)
        button_frame.pack(fill=tk.X, padx=10, pady=20)
        
        def save_rule():
            try:
                rule_id = rule_id_var.get().strip()
                if not rule_id:
                    messagebox.showerror("Error", "Rule ID is required")
                    return
                
                # Build rule
                rule = {
                    'ID': rule_id,
                    'Status': status_var.get()
                }
                
                # Add filter if prefix is specified
                prefix = prefix_var.get().strip()
                if prefix:
                    rule['Filter'] = {'Prefix': prefix}
                else:
                    rule['Filter'] = {}
                
                # Add expiration
                if enable_exp_var.get() or enable_delete_markers_var.get():
                    expiration = {}
                    if enable_exp_var.get():
                        try:
                            days = int(exp_days_var.get())
                            expiration['Days'] = days
                        except ValueError:
                            messagebox.showerror("Error", "Expiration days must be a number")
                            return
                    
                    if enable_delete_markers_var.get():
                        expiration['ExpiredObjectDeleteMarker'] = True
                    
                    if expiration:
                        rule['Expiration'] = expiration
                
                # Add transition
                if enable_trans_var.get():
                    try:
                        days = int(trans_days_var.get())
                        rule['Transitions'] = [{
                            'Days': days,
                            'StorageClass': trans_class_var.get()
                        }]
                    except ValueError:
                        messagebox.showerror("Error", "Transition days must be a number")
                        return
                
                # Add non-current version expiration and transition
                if enable_noncur_var.get():
                    try:
                        days = int(noncur_days_var.get())
                        rule['NoncurrentVersionExpiration'] = {'NoncurrentDays': days}
                    except ValueError:
                        messagebox.showerror("Error", "Non-current version days must be a number")
                        return
                
                if enable_noncur_trans_var.get():
                    try:
                        days = int(noncur_trans_days_var.get())
                        if 'NoncurrentVersionTransitions' not in rule:
                            rule['NoncurrentVersionTransitions'] = []
                        rule['NoncurrentVersionTransitions'].append({
                            'NoncurrentDays': days,
                            'StorageClass': noncur_trans_class_var.get()
                        })
                    except ValueError:
                        messagebox.showerror("Error", "Non-current version transition days must be a number")
                        return
                
                # Add incomplete multipart upload abortion
                if enable_multipart_var.get():
                    try:
                        days = int(multipart_days_var.get())
                        rule['AbortIncompleteMultipartUpload'] = {'DaysAfterInitiation': days}
                    except ValueError:
                        messagebox.showerror("Error", "Multipart upload days must be a number")
                        return
                
                # Get existing rules and add new one
                existing_rules = list(self.lifecycle_rules)
                existing_rules.append(rule)
                
                # Apply the lifecycle configuration
                lifecycle_config = {'Rules': existing_rules}
                self.s3_client.put_bucket_lifecycle_configuration(
                    Bucket=self.current_bucket,
                    LifecycleConfiguration=lifecycle_config
                )
                
                # Refresh the display
                self.load_lifecycle_rules()
                self.refresh_json_policy()
                
                messagebox.showinfo("Success", f"Lifecycle rule '{rule_id}' added successfully")
                self.log_debug(f"Added lifecycle rule: {rule_id}")
                dialog.destroy()
                
            except Exception as e:
                messagebox.showerror("Error", f"Failed to add rule: {str(e)}")
                self.log_debug(f"Error adding lifecycle rule: {e}")
        
        ttk.Button(button_frame, text="Save Rule", command=save_rule).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(button_frame, text="Cancel", command=dialog.destroy).pack(side=tk.LEFT)
        
        # Pack canvas and scrollbar
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

    def delete_lifecycle_rule(self):
        """Delete selected lifecycle rule"""
        if not hasattr(self, 'rules_listbox') or not self.rules_listbox:
            return
        
        selection = self.rules_listbox.curselection()
        if not selection:
            messagebox.showwarning("No Selection", "Please select a rule to delete.")
            return
        
        if not self.lifecycle_rules:
            messagebox.showwarning("No Rules", "No rules to delete.")
            return
        
        try:
            rule_index = selection[0]
            if rule_index >= len(self.lifecycle_rules):
                messagebox.showerror("Error", "Invalid rule selection.")
                return
            
            rule_to_delete = self.lifecycle_rules[rule_index]
            rule_id = rule_to_delete.get('ID', f'Rule {rule_index + 1}')
            
            if not messagebox.askyesno("Confirm", f"Delete lifecycle rule '{rule_id}'?"):
                return
            
            # Remove the rule from the list
            updated_rules = [rule for i, rule in enumerate(self.lifecycle_rules) if i != rule_index]
            
            if updated_rules:
                # Update with remaining rules
                lifecycle_config = {'Rules': updated_rules}
                self.s3_client.put_bucket_lifecycle_configuration(
                    Bucket=self.current_bucket,
                    LifecycleConfiguration=lifecycle_config
                )
            else:
                # Delete entire lifecycle configuration if no rules remain
                self.s3_client.delete_bucket_lifecycle(Bucket=self.current_bucket)
            
            # Refresh the display
            self.load_lifecycle_rules()
            self.refresh_json_policy()
            
            messagebox.showinfo("Success", f"Lifecycle rule '{rule_id}' deleted successfully")
            self.log_debug(f"Deleted lifecycle rule: {rule_id}")
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to delete rule: {str(e)}")
            self.log_debug(f"Error deleting lifecycle rule: {e}")

    def delete_all_lifecycle_rules(self):
        """Delete all lifecycle rules"""
        if not self.lifecycle_rules:
            messagebox.showwarning("No Rules", "No lifecycle rules to delete.")
            return
        
        if not messagebox.askyesno("Confirm", f"Delete all {len(self.lifecycle_rules)} lifecycle rules?"):
            return
        
        try:
            self.s3_client.delete_bucket_lifecycle(Bucket=self.current_bucket)
            
            # Refresh the display
            self.load_lifecycle_rules()
            self.refresh_json_policy()
            
            messagebox.showinfo("Success", "All lifecycle rules deleted successfully")
            self.log_debug("Deleted all lifecycle rules")
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to delete all rules: {str(e)}")
            self.log_debug(f"Error deleting all lifecycle rules: {e}")

    def refresh_json_policy(self):
        """Refresh the JSON policy display"""
        if not hasattr(self, 'policy_json_text') or not self.policy_json_text:
            return
        
        try:
            if self.lifecycle_rules:
                policy = {'Rules': self.lifecycle_rules}
                json_str = json.dumps(policy, indent=2, default=str)
            else:
                json_str = "No lifecycle policy configured"
            
            self.policy_json_text.delete(1.0, tk.END)
            self.policy_json_text.insert(1.0, json_str)
            
        except Exception as e:
            self.policy_json_text.delete(1.0, tk.END)
            self.policy_json_text.insert(1.0, f"Error displaying policy: {str(e)}")
            self.log_debug(f"Error refreshing JSON policy: {e}")

    def apply_json_lifecycle_policy(self):
        """Apply lifecycle policy from JSON"""
        if not hasattr(self, 'policy_json_text') or not self.policy_json_text:
            return
        
        try:
            json_content = self.policy_json_text.get(1.0, tk.END).strip()
            if not json_content or json_content == "No lifecycle policy configured":
                messagebox.showwarning("Warning", "No JSON policy to apply.")
                return
            
            # Parse JSON
            policy = json.loads(json_content)
            
            # Validate that it has Rules
            if 'Rules' not in policy:
                messagebox.showerror("Error", "JSON policy must contain 'Rules' array.")
                return
            
            # Apply the policy
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=self.current_bucket,
                LifecycleConfiguration=policy
            )
            
            # Refresh the display
            self.load_lifecycle_rules()
            
            messagebox.showinfo("Success", "JSON lifecycle policy applied successfully")
            self.log_debug("Applied JSON lifecycle policy")
            
        except json.JSONDecodeError as e:
            messagebox.showerror("Error", f"Invalid JSON: {str(e)}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to apply policy: {str(e)}")
            self.log_debug(f"Error applying JSON lifecycle policy: {e}")

    def toggle_versioning(self, enable):
        """Toggle bucket versioning"""
        if not self.current_bucket:
            messagebox.showwarning("No Bucket Selected", "Please select a bucket first.")
            return
        
        try:
            status = "Enabled" if enable else "Suspended"
            self.s3_client.put_bucket_versioning(
                Bucket=self.current_bucket,
                VersioningConfiguration={'Status': status}
            )
            messagebox.showinfo("Success", f"Bucket versioning {status.lower()}")
            self.log_debug(f"Bucket versioning {status.lower()}: {self.current_bucket}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to toggle versioning: {str(e)}")
            self.log_debug(f"Error toggling versioning: {e}")

    def on_bucket_selected(self, event):
        """Handle bucket selection"""
        selection = self.bucket_listbox.curselection()
        if not selection:
            return
        
        self.current_bucket = self.bucket_listbox.get(selection[0])
        self.current_prefix = ""
        self.prefix_var.set("")
        self.continuation_token = None
        self.log_debug(f"Selected bucket: {self.current_bucket}")
        self.refresh_objects()
        # Auto-load all versions if no object is selected
        self.show_all_versions()

    def refresh_objects(self):
        """Refresh object list"""
        if not self.current_bucket:
            return
        
        self.objects_tree.delete(*self.objects_tree.get_children())
        self.objects = []
        self.continuation_token = None
        self.total_object_count = 0
        self.total_folder_count = 0
        
        # Update breadcrumb
        if self.breadcrumb_label:
            if self.current_prefix:
                self.breadcrumb_label.config(text=f"Location: {self.current_bucket}/{self.current_prefix}")
            else:
                self.breadcrumb_label.config(text=f"Location: {self.current_bucket}/")
        
        self.load_objects()

    def load_objects(self):
        """Load objects with pagination"""
        if not self.current_bucket or self.loading:
            return
        
        self.loading = True
        self.set_status("Loading objects...", busy=True)
        self.log_debug(f"Loading objects from bucket: {self.current_bucket}, prefix: {self.current_prefix}, flat_view: {self.flat_view.get()}")
        
        def load_in_thread():
            try:
                params = {
                    'Bucket': self.current_bucket,
                    'MaxKeys': 1000,
                    'Prefix': self.current_prefix
                }
                
                # In flat view, don't use delimiter to get all objects
                if not self.flat_view.get():
                    params['Delimiter'] = '/'  # This makes S3 return folders as CommonPrefixes
                
                if self.continuation_token:
                    params['ContinuationToken'] = self.continuation_token
                
                response = self.safe_api_call(self.s3_client.list_objects_v2, **params)
                
                # Process results in main thread
                self.root.after(0, self.process_objects, response)
            except Exception as e:
                self.root.after(0, lambda: self.set_status(f"Error: {str(e)}", busy=False))
                self.root.after(0, lambda: messagebox.showerror("Error", f"Failed to list objects: {str(e)}"))
                self.root.after(0, lambda: self.log_debug(f"Error listing objects: {e}"))
            finally:
                self.loading = False
        
        threading.Thread(target=load_in_thread, daemon=True).start()

    def process_objects(self, response):
        """Process loaded objects"""
        objects_added = 0
        folders_added = 0
        
        # Track items to add and sort them
        items_to_add = []
        
        # In flat view, skip common prefixes as we'll see all objects
        if not self.flat_view.get():
            # Get common prefixes (folders)
            common_prefixes = response.get('CommonPrefixes', [])
            for prefix_info in common_prefixes:
                prefix = prefix_info['Prefix']
                # Skip if it's the current prefix itself
                if prefix == self.current_prefix:
                    continue
                # Get display name (remove current prefix)
                display_name = prefix[len(self.current_prefix):] if self.current_prefix else prefix
                # Remove trailing slash for display
                display_name = display_name.rstrip('/')
                if display_name:  # Only add if there's actually a name
                    items_to_add.append({
                        'type': 'folder',
                        'display_name': display_name,
                        'values': ("", "", "Folder", prefix),
                        'key': prefix
                    })
        
        # Process objects
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                # Skip if this is exactly the current prefix (folder placeholder)
                if key == self.current_prefix and self.current_prefix.endswith('/'):
                    continue
                
                self.objects.append(obj)
                
                # Format values
                size = self.format_size(obj['Size'])
                modified = obj['LastModified'].strftime("%Y-%m-%d %H:%M:%S")
                storage_class = obj.get('StorageClass', 'STANDARD')
                
                # Get display name
                if self.flat_view.get():
                    # In flat view, show full path
                    display_name = key
                else:
                    # In hierarchical view, remove current prefix
                    display_name = key[len(self.current_prefix):] if self.current_prefix else key
                    # In hierarchical view, if the display name contains slashes, 
                    # it means it's a nested object, skip it (will be shown when navigating into folders)
                    if '/' in display_name and not key.endswith('/'):
                        continue
                
                # Skip if no display name (shouldn't happen)
                if not display_name:
                    continue
                
                # Determine if it's a folder (ends with / and has no more content)
                is_folder = key.endswith('/') and not self.flat_view.get()
                
                if is_folder:
                    # Remove trailing slash for display
                    display_name = display_name.rstrip('/')
                
                items_to_add.append({
                    'type': 'folder' if is_folder else 'file',
                    'display_name': display_name,
                    'values': (size if not is_folder else "", modified, storage_class, key),
                    'key': key
                })
        
        # Sort items: folders first, then files, both alphabetically
        # Only sort if this is the first load (no existing items)
        if len(self.objects_tree.get_children()) == 0:
            items_to_add.sort(key=lambda x: (x['type'] != 'folder', x['display_name'].lower()))
        
        # Add sorted items to tree
        for item in items_to_add:
            item_id = self.objects_tree.insert(
                "", tk.END,
                text=item['display_name'],
                values=item['values'],
                tags=(item['type'],),
                open=False
            )
            if item['type'] == 'folder':
                folders_added += 1
            else:
                objects_added += 1
        
        # Update continuation token
        self.continuation_token = response.get('NextContinuationToken')
        
        # Update totals
        self.total_object_count += objects_added
        self.total_folder_count += folders_added
        
        total_items = len(self.objects_tree.get_children())
        if total_items == 0 and not self.continuation_token:
            status_msg = "No objects found in this location"
        else:
            if self.flat_view.get():
                status_msg = f"Flat View - Displayed: {total_items} files"
            else:
                status_msg = f"Displayed: {total_items} items ({self.total_object_count} files, {self.total_folder_count} folders)"
            if self.continuation_token:
                status_msg += " - More available, click 'Load More'"
        
        self.set_status(status_msg, busy=False)
        self.log_debug(f"Added {objects_added} objects and {folders_added} folders, has more: {self.continuation_token is not None}")

    def load_more_objects(self):
        """Load more objects"""
        if self.continuation_token and not self.loading:
            self.load_objects()

    def navigate_up(self):
        """Navigate up one folder level"""
        if self.flat_view.get():
            # In flat view, just clear the prefix
            self.current_prefix = ""
            self.prefix_var.set(self.current_prefix)
            self.refresh_objects()
        elif self.current_prefix:
            # Remove trailing slash for processing
            prefix = self.current_prefix.rstrip('/')
            # Find the last slash
            last_slash = prefix.rfind('/')
            if last_slash >= 0:
                # Go up one level
                self.current_prefix = prefix[:last_slash + 1]
            else:
                # Go to root
                self.current_prefix = ""
            self.prefix_var.set(self.current_prefix)
            self.refresh_objects()

    def apply_prefix_filter(self):
        """Apply prefix filter"""
        self.current_prefix = self.prefix_var.get()
        self.refresh_objects()

    def format_size(self, size):
        """Format file size"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} PB"

    def chunk_list(self, items, size):
        """Yield successive chunks from a list"""
        for i in range(0, len(items), size):
            yield items[i:i + size]

    def sanitize_version_id(self, version_id):
        """Sanitize version id for filenames"""
        safe = re.sub(r'[^A-Za-z0-9._-]+', '_', str(version_id or ""))
        return safe or "null"

    def add_version_suffix(self, filename, version_id):
        """Add version suffix to filename or path"""
        if not version_id or version_id == "null":
            return filename
        safe_version = self.sanitize_version_id(version_id)
        base, ext = os.path.splitext(filename)
        return f"{base}.__version__{safe_version}{ext}"

    def show_object_menu(self, event):
        """Show right-click menu for objects"""
        item = self.objects_tree.identify_row(event.y)
        if item:
            self.objects_tree.selection_set(item)
            self.object_menu.post(event.x_root, event.y_root)

    def show_versions_menu(self, event):
        """Show right-click menu for versions"""
        if not hasattr(self, 'versions_tree'):
            return
        item = self.versions_tree.identify_row(event.y)
        if item:
            if item not in self.versions_tree.selection():
                self.versions_tree.selection_set(item)
            self.versions_menu.post(event.x_root, event.y_root)

    def copy_version_key(self):
        """Copy version key to clipboard"""
        entries = self.get_selected_version_entries()
        if entries:
            self.root.clipboard_clear()
            self.root.clipboard_append(entries[0]['key'])
            self.log_debug(f"Copied version key to clipboard: {entries[0]['key']}")

    def copy_version_id(self):
        """Copy version id to clipboard"""
        entries = self.get_selected_version_entries()
        if entries:
            self.root.clipboard_clear()
            self.root.clipboard_append(entries[0]['version_id'])
            self.log_debug(f"Copied version id to clipboard: {entries[0]['version_id']}")

    def on_object_double_click(self, event):
        """Handle double-click on object"""
        selection = self.objects_tree.selection()
        if not selection:
            return
        
        item = selection[0]
        tags = self.objects_tree.item(item)['tags']
        
        if 'folder' in tags and not self.flat_view.get():
            # Navigate into folder only in hierarchical view
            item_values = self.objects_tree.item(item)['values']
            if len(item_values) >= 4:
                folder_path = item_values[3]  # fullpath is in the 4th column
                if folder_path:
                    self.current_prefix = folder_path
                    self.prefix_var.set(self.current_prefix)
                    self.refresh_objects()
        else:
            # Show object details
            self.show_object_details()

    def on_object_selected(self, event):
        """Handle object selection"""
        selection = self.objects_tree.selection()
        if len(selection) == 1:
            self.show_object_details()

    def get_selected_object_key(self):
        """Get the full key of selected object"""
        keys = self.get_selected_object_keys()
        return keys[0] if keys else None

    def get_selected_object_keys(self):
        """Get full keys for selected objects (files only)"""
        selection = self.objects_tree.selection()
        keys = []
        for item in selection:
            tags = self.objects_tree.item(item).get('tags', ())
            if 'folder' in tags:
                continue
            item_values = self.objects_tree.item(item).get('values', ())
            if len(item_values) >= 4 and item_values[3]:
                keys.append(item_values[3])
        return keys

    def get_selected_version_entries(self):
        """Get selected version entries from versions tree"""
        selection = self.versions_tree.selection() if hasattr(self, 'versions_tree') else ()
        entries = []
        for item in selection:
            item_values = self.versions_tree.item(item).get('values', ())
            if len(item_values) < 6:
                continue
            key = item_values[3]
            version_id = item_values[4]
            is_delete_marker = item_values[5]
            if isinstance(is_delete_marker, str):
                is_delete_marker = is_delete_marker.lower() == 'true'
            entries.append({
                'key': key,
                'version_id': version_id,
                'is_delete_marker': bool(is_delete_marker)
            })
        return entries

    def show_object_details(self):
        """Show details for selected object"""
        key = self.get_selected_object_key()
        if not key:
            return
        
        # Update metadata
        self.show_object_metadata()
        # Update tags
        self.show_object_tags()
        # Update versions if versioning is enabled
        self.show_object_versions()

    def show_object_metadata(self):
        """Show metadata for selected object"""
        key = self.get_selected_object_key()
        if not key or not self.current_bucket:
            return
        
        try:
            response = self.safe_api_call(self.s3_client.head_object, Bucket=self.current_bucket, Key=key)
            
            # Format metadata response
            self.metadata_text.delete(1.0, tk.END)
            # Remove ResponseMetadata for cleaner display
            response.pop('ResponseMetadata', None)
            
            for field, value in response.items():
                if isinstance(value, datetime):
                    value = value.strftime("%Y-%m-%d %H:%M:%S UTC")
                self.metadata_text.insert(tk.END, f"{field}: {value}\n")
            
            # Show headers in headers tab
            self.headers_text.delete(1.0, tk.END)
            for field, value in response.items():
                if isinstance(value, datetime):
                    value = value.strftime("%Y-%m-%d %H:%M:%S UTC")
                self.headers_text.insert(tk.END, f"{field}: {value}\n")
            
            self.log_debug(f"Retrieved metadata for object: {key}")
        except Exception as e:
            self.metadata_text.delete(1.0, tk.END)
            self.metadata_text.insert(tk.END, f"Error: {str(e)}")
            self.headers_text.delete(1.0, tk.END)
            self.headers_text.insert(tk.END, f"Error: {str(e)}")
            self.log_debug(f"Error getting metadata: {e}")

    def show_object_tags(self):
        """Show tags for selected object"""
        key = self.get_selected_object_key()
        if not key or not self.current_bucket:
            return
        
        # Clear existing tags
        for item in self.tags_tree.get_children():
            self.tags_tree.delete(item)
        
        try:
            response = self.safe_api_call(self.s3_client.get_object_tagging, Bucket=self.current_bucket, Key=key)
            for tag in response.get('TagSet', []):
                self.tags_tree.insert("", tk.END, text=tag['Key'], values=(tag['Value'],))
            self.log_debug(f"Retrieved {len(response.get('TagSet', []))} tags for object: {key}")
        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchTagSet':
                self.log_debug(f"Error getting tags: {e}")

    def show_object_versions(self):
        """Show versions for selected object"""
        key = self.get_selected_object_key()
        if not key or not self.current_bucket:
            return

        self.load_versions(prefix=key, selected_key=key)

    def generate_presigned_url(self):
        """Generate presigned URL for selected object"""
        key = self.get_selected_object_key()
        if not key or not self.current_bucket:
            return
        
        try:
            expiration = int(self.expiration_var.get())
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.current_bucket, 'Key': key},
                ExpiresIn=expiration
            )
            
            # Generate curl command
            curl_command = f'curl -O "{url}"'
            
            # Display in text widget
            self.presigned_text.delete(1.0, tk.END)
            self.presigned_text.insert(tk.END, f"URL:\n{url}\n\n")
            self.presigned_text.insert(tk.END, f"Curl command:\n{curl_command}")
            
            self.log_debug(f"Generated presigned URL for: {key}")
        except Exception as e:
            self.presigned_text.delete(1.0, tk.END)
            self.presigned_text.insert(tk.END, f"Error: {str(e)}")
            self.log_debug(f"Error generating presigned URL: {e}")

    def download_object(self):
        """Download selected object(s) with progress tracking"""
        if not self.current_bucket:
            return

        keys = self.get_selected_object_keys()
        if not keys:
            messagebox.showwarning("Warning", "Please select one or more objects (not folders)")
            return

        if len(keys) == 1:
            self.download_single_object(keys[0])
        else:
            self.download_multiple_objects(keys)

    def download_single_object(self, key):
        """Download a single object with progress tracking"""
        filename = os.path.basename(key.rstrip('/')) or "object"
        filepath = filedialog.asksaveasfilename(
            defaultextension="",
            initialfile=filename,
            title="Save Object As"
        )

        if not filepath:
            return

        progress_dialog = tk.Toplevel(self.root)
        progress_dialog.title("Downloading...")
        progress_dialog.geometry("420x160")
        progress_dialog.transient(self.root)
        progress_dialog.grab_set()

        progress_label = ttk.Label(progress_dialog, text=f"Downloading: {filename}")
        progress_label.pack(pady=10)

        progress_var = tk.DoubleVar()
        progress_bar = ttk.Progressbar(progress_dialog, variable=progress_var, maximum=100)
        progress_bar.pack(fill=tk.X, padx=20, pady=10)

        status_label = ttk.Label(progress_dialog, text="Starting download...")
        status_label.pack(pady=5)

        cancel_var = tk.BooleanVar()
        ttk.Button(progress_dialog, text="Cancel", command=lambda: cancel_var.set(True)).pack(pady=5)

        def download_with_progress():
            try:
                response = self.safe_api_call(self.s3_client.head_object, Bucket=self.current_bucket, Key=key)
                total_size = response['ContentLength']
                downloaded = 0

                def progress_callback(bytes_transferred):
                    nonlocal downloaded
                    downloaded += bytes_transferred
                    if total_size > 0:
                        percent = (downloaded / total_size) * 100
                        self.root.after(0, lambda: progress_var.set(percent))
                        self.root.after(
                            0,
                            lambda: status_label.config(
                                text=f"Downloaded: {self.format_size(downloaded)} / {self.format_size(total_size)}"
                            )
                        )
                    if cancel_var.get():
                        raise Exception("Download cancelled by user")

                transfer_config = boto3.s3.transfer.TransferConfig(
                    multipart_threshold=self.multipart_threshold.get() * 1024 * 1024,
                    max_concurrency=self.max_concurrent_requests.get(),
                    multipart_chunksize=self.multipart_chunksize.get() * 1024 * 1024,
                    use_threads=True
                )

                self.s3_client.download_file(
                    self.current_bucket,
                    key,
                    filepath,
                    Config=transfer_config,
                    Callback=progress_callback
                )

                self.root.after(0, progress_dialog.destroy)
                self.root.after(0, lambda: messagebox.showinfo("Success", f"Downloaded '{filename}' successfully"))
                self.log_debug(f"Downloaded object: {key} to {filepath}")
            except Exception as e:
                if cancel_var.get() and os.path.exists(filepath):
                    try:
                        os.remove(filepath)
                    except OSError:
                        pass
                self.root.after(0, progress_dialog.destroy)
                if not cancel_var.get():
                    self.root.after(0, lambda: messagebox.showerror("Error", f"Failed to download: {str(e)}"))
                self.log_debug(f"Error downloading object: {e}")

        self.executor.submit(download_with_progress)

    def download_multiple_objects(self, keys):
        """Download multiple objects to a selected directory"""
        dest_dir = filedialog.askdirectory(title="Select download folder")
        if not dest_dir:
            return

        progress_dialog = tk.Toplevel(self.root)
        progress_dialog.title("Downloading Objects...")
        progress_dialog.geometry("520x220")
        progress_dialog.transient(self.root)
        progress_dialog.grab_set()

        overall_label = ttk.Label(progress_dialog, text=f"Downloading {len(keys)} objects...")
        overall_label.pack(pady=10)

        overall_progress = ttk.Progressbar(progress_dialog, maximum=len(keys))
        overall_progress.pack(fill=tk.X, padx=20, pady=5)

        current_label = ttk.Label(progress_dialog, text="")
        current_label.pack(pady=5)

        status_label = ttk.Label(progress_dialog, text="Preparing downloads...")
        status_label.pack(pady=5)

        cancel_var = tk.BooleanVar()
        ttk.Button(progress_dialog, text="Cancel", command=lambda: cancel_var.set(True)).pack(pady=5)

        transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=self.multipart_threshold.get() * 1024 * 1024,
            max_concurrency=self.max_concurrent_requests.get(),
            multipart_chunksize=self.multipart_chunksize.get() * 1024 * 1024,
            use_threads=True
        )

        def download_batch():
            errors = []

            def cancel_callback(_bytes_transferred):
                if cancel_var.get():
                    raise Exception("Download cancelled by user")

            for index, key in enumerate(keys, start=1):
                if cancel_var.get():
                    break
                self.root.after(0, lambda k=key: current_label.config(text=f"Downloading: {k}"))
                safe_key = key.lstrip("/")
                dest_path = os.path.join(dest_dir, safe_key.replace("/", os.sep))
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                try:
                    self.s3_client.download_file(
                        self.current_bucket,
                        key,
                        dest_path,
                        Config=transfer_config,
                        Callback=cancel_callback
                    )
                    self.log_debug(f"Downloaded object: {key} to {dest_path}")
                except Exception as e:
                    if cancel_var.get():
                        break
                    errors.append((key, str(e)))
                    self.log_debug(f"Error downloading object {key}: {e}")
                self.root.after(0, lambda i=index: overall_progress.config(value=i))
                self.root.after(0, lambda i=index: status_label.config(text=f"Completed {i}/{len(keys)}"))

            self.root.after(0, progress_dialog.destroy)
            if cancel_var.get():
                self.root.after(0, lambda: messagebox.showinfo("Cancelled", "Download cancelled by user"))
                return
            if errors:
                error_summary = "\n".join([f"{k}: {err}" for k, err in errors[:10]])
                if len(errors) > 10:
                    error_summary += f"\n...and {len(errors) - 10} more"
                self.root.after(0, lambda: messagebox.showerror("Download Errors", error_summary))
            else:
                self.root.after(0, lambda: messagebox.showinfo("Success", "All selected objects downloaded"))

        self.executor.submit(download_batch)

    def delete_object(self):
        """Delete selected object(s)"""
        if not self.current_bucket:
            return

        keys = self.get_selected_object_keys()
        if not keys:
            messagebox.showwarning("Warning", "Please select one or more objects (not folders)")
            return

        if len(keys) == 1:
            confirm = messagebox.askyesno("Confirm", f"Delete object '{keys[0]}'?")
        else:
            confirm = messagebox.askyesno("Confirm", f"Delete {len(keys)} selected objects?")
        if not confirm:
            return

        def delete_in_thread():
            try:
                errors = []
                for batch in self.chunk_list(keys, 1000):
                    response = self.safe_api_call(
                        self.s3_client.delete_objects,
                        Bucket=self.current_bucket,
                        Delete={'Objects': [{'Key': k} for k in batch], 'Quiet': True}
                    )
                    errors.extend(response.get('Errors', []))
                    if self.request_delay_ms.get():
                        time.sleep(self.request_delay_ms.get() / 1000.0)

                self.root.after(0, self.refresh_objects)
                if errors:
                    error_summary = "\n".join([f"{e.get('Key')}: {e.get('Message')}" for e in errors[:10]])
                    if len(errors) > 10:
                        error_summary += f"\n...and {len(errors) - 10} more"
                    self.root.after(0, lambda: messagebox.showerror("Delete Errors", error_summary))
                else:
                    self.root.after(0, lambda: messagebox.showinfo("Success", "Deleted selected objects"))
                self.log_debug(f"Deleted {len(keys)} objects")
            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("Error", f"Failed to delete: {str(e)}"))
                self.log_debug(f"Error deleting objects: {e}")

        self.executor.submit(delete_in_thread)

    def copy_object_key(self):
        """Copy object key to clipboard"""
        key = self.get_selected_object_key()
        if key:
            self.root.clipboard_clear()
            self.root.clipboard_append(key)
            self.log_debug(f"Copied key to clipboard: {key}")

    def upload_files(self):
        """Upload files to current bucket/prefix with concurrent uploads"""
        if not self.current_bucket:
            messagebox.showwarning("Warning", "Please select a bucket first")
            return
        
        files = filedialog.askopenfilenames(title="Select files to upload")
        if not files:
            return
        
        # Create progress dialog
        progress_dialog = tk.Toplevel(self.root)
        progress_dialog.title("Uploading Files...")
        progress_dialog.geometry("500x300")
        progress_dialog.transient(self.root)
        progress_dialog.grab_set()
        
        # Overall progress
        overall_label = ttk.Label(progress_dialog, text=f"Uploading {len(files)} files...")
        overall_label.pack(pady=10)
        
        overall_progress = ttk.Progressbar(progress_dialog, maximum=len(files))
        overall_progress.pack(fill=tk.X, padx=20, pady=5)
        
        # Current file progress
        current_label = ttk.Label(progress_dialog, text="")
        current_label.pack(pady=5)
        
        current_progress = ttk.Progressbar(progress_dialog, maximum=100)
        current_progress.pack(fill=tk.X, padx=20, pady=5)
        
        # Status area
        status_frame = ttk.Frame(progress_dialog)
        status_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)
        
        status_text = tk.Text(status_frame, height=8, wrap=tk.WORD)
        status_scroll = ttk.Scrollbar(status_frame, command=status_text.yview)
        status_text.configure(yscrollcommand=status_scroll.set)
        status_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        status_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        cancel_var = tk.BooleanVar()
        ttk.Button(progress_dialog, text="Cancel", command=lambda: cancel_var.set(True)).pack(pady=5)
        
        def upload_file(filepath, file_index):
            """Upload a single file"""
            filename = os.path.basename(filepath)
            key = self.current_prefix + filename
            
            try:
                if cancel_var.get():
                    return False
                
                # Update current file label
                self.root.after(0, lambda: current_label.config(text=f"Uploading: {filename}"))
                
                # Get file size
                file_size = os.path.getsize(filepath)
                uploaded = 0
                
                def progress_callback(bytes_transferred):
                    nonlocal uploaded
                    uploaded += bytes_transferred
                    if file_size > 0:
                        percent = (uploaded / file_size) * 100
                        self.root.after(0, lambda: current_progress.config(value=percent))
                    self.root.update_idletasks()
                
                # Configure transfer
                transfer_config = boto3.s3.transfer.TransferConfig(
                    multipart_threshold=self.multipart_threshold.get() * 1024 * 1024,
                    max_concurrency=self.max_concurrent_requests.get(),
                    multipart_chunksize=self.multipart_chunksize.get() * 1024 * 1024,
                    use_threads=True
                )
                
                self.s3_client.upload_file(
                    filepath, 
                    self.current_bucket, 
                    key,
                    Config=transfer_config,
                    Callback=progress_callback
                )
                
                # Update status
                self.root.after(0, lambda: status_text.insert(tk.END, f"✓ {filename} uploaded successfully\n"))
                self.root.after(0, lambda: status_text.see(tk.END))
                self.log_debug(f"Uploaded file: {filepath} as {key}")
                return True
                
            except Exception as e:
                self.root.after(0, lambda: status_text.insert(tk.END, f"✗ {filename} failed: {str(e)}\n"))
                self.root.after(0, lambda: status_text.see(tk.END))
                self.log_debug(f"Error uploading {filename}: {e}")
                return False
        
        def upload_all_files():
            """Upload all files concurrently"""
            success_count = 0
            
            # Use ThreadPoolExecutor for concurrent uploads
            futures = []
            for i, filepath in enumerate(files):
                if cancel_var.get():
                    break
                future = self.executor.submit(upload_file, filepath, i)
                futures.append(future)
            
            # Process completed uploads
            for i, future in enumerate(as_completed(futures)):
                if cancel_var.get():
                    break
                
                try:
                    if future.result():
                        success_count += 1
                except Exception as e:
                    self.log_debug(f"Upload future error: {e}")
                
                # Update overall progress
                self.root.after(0, lambda: overall_progress.config(value=i + 1))
            
            # Cleanup
            self.root.after(0, lambda: progress_dialog.destroy())
            if success_count > 0:
                self.root.after(0, lambda: messagebox.showinfo("Success", f"Uploaded {success_count}/{len(files)} file(s) successfully"))
                self.root.after(0, self.refresh_objects)
        
        # Start upload in thread
        threading.Thread(target=upload_all_files, daemon=True).start()

    def create_folder(self):
        """Create a folder (prefix) in the current location"""
        if not self.current_bucket:
            messagebox.showwarning("Warning", "Please select a bucket first")
            return
        
        folder_name = simpledialog.askstring("Create Folder", "Enter folder name:")
        if not folder_name:
            return
        
        # Remove any slashes from the folder name input
        folder_name = folder_name.strip('/')
        
        # Create the full key with current prefix
        key = self.current_prefix + folder_name + '/'
        
        try:
            # Create empty object with folder key
            self.safe_api_call(self.s3_client.put_object, Bucket=self.current_bucket, Key=key, Body=b'')
            self.refresh_objects()
            messagebox.showinfo("Success", f"Created folder '{folder_name}' successfully")
            self.log_debug(f"Created folder: {key}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to create folder: {str(e)}")
            self.log_debug(f"Error creating folder: {e}")

    def refresh_all(self):
        """Refresh all data"""
        if self.current_endpoint:
            self.refresh_buckets()
            if self.current_bucket:
                self.refresh_objects()

if __name__ == "__main__":
    root = tk.Tk()
    app = S3BrowserApp(root)
    root.mainloop()
