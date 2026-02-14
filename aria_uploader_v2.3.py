#!/usr/bin/env python3
"""
Combined VRS processor (via Meta MPS service) and Google Cloud Uploader.
Processes Meta Aria VRS recordings using the MPS (Machine Perception
Services) pipeline (via the aria_mps CLI / Project Aria SDK) to produce
processed outputs (SLAM, gaze, hand-tracking, etc.), then optionally
uploads those outputs to Google Cloud Storage.

This script runs on Windows and uses the aria_mps CLI for processing.

Features:
- Graphical user interface (Tkinter)
- Save/load Aria credentials locally
- File selection dialog for VRS files
- Folder selection dialog for processed output location
- Google Cloud Storage integration
- Progress bar with real-time status updates
- Automatic upload after processing completion
- Background process handling with polling buffer
"""

import argparse
import json
import logging
import os
import shutil
import site
import subprocess
import sys
import threading
import time
import math
from pathlib import Path
from typing import Optional, Tuple
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import webbrowser

from google.cloud import storage


class CredentialsManager:
    """Manages Aria credentials storage and retrieval."""
    
    CONFIG_DIR = Path.home() / '.aria_uploader'
    CONFIG_FILE = CONFIG_DIR / 'credentials.json'
    GCLOUD_CONFIG_FILE = CONFIG_DIR / 'gcloud_settings.json'
    
    @classmethod
    def ensure_config_dir(cls):
        """Create config directory if it doesn't exist."""
        cls.CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def save_credentials(cls, username: str, password: str):
        """
        Save Aria credentials to local config file.
        
        Args:
            username: Aria username
            password: Aria password
        """
        try:
            cls.ensure_config_dir()
            credentials = {
                'username': username,
                'password': password,
                'saved_at': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            with open(cls.CONFIG_FILE, 'w') as f:
                json.dump(credentials, f, indent=2)
            
            logging.info(f"Credentials saved to {cls.CONFIG_FILE}")
            return True
        except Exception as e:
            logging.error(f"Failed to save credentials: {str(e)}")
            return False
    
    @classmethod
    def load_credentials(cls) -> Tuple[Optional[str], Optional[str]]:
        """
        Load Aria credentials from local config file.
        
        Returns:
            Tuple of (username, password) or (None, None) if not found
        """
        try:
            if not cls.CONFIG_FILE.exists():
                logging.info("No saved credentials found")
                return None, None
            
            with open(cls.CONFIG_FILE, 'r') as f:
                credentials = json.load(f)
            
            username = credentials.get('username')
            password = credentials.get('password')
            
            if username and password:
                logging.info("Credentials loaded from config file")
                return username, password
            
            return None, None
        except Exception as e:
            logging.error(f"Failed to load credentials: {str(e)}")
            return None, None
    
    @classmethod
    def clear_credentials(cls):
        """Clear saved credentials."""
        try:
            if cls.CONFIG_FILE.exists():
                cls.CONFIG_FILE.unlink()
                logging.info("Credentials cleared")
                return True
        except Exception as e:
            logging.error(f"Failed to clear credentials: {str(e)}")
        return False
    
    @classmethod
    def save_gcloud_settings(cls, gcloud_cred_path: str, bucket_name: str):
        """
        Save Google Cloud settings to local config file.
        
        Args:
            gcloud_cred_path: Path to service account JSON file
            bucket_name: Google Cloud Storage bucket name
        """
        try:
            cls.ensure_config_dir()
            settings = {
                'gcloud_cred_path': gcloud_cred_path,
                'bucket_name': bucket_name,
                'saved_at': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            with open(cls.GCLOUD_CONFIG_FILE, 'w') as f:
                json.dump(settings, f, indent=2)
            
            logging.info(f"Google Cloud settings saved to {cls.GCLOUD_CONFIG_FILE}")
            return True
        except Exception as e:
            logging.error(f"Failed to save Google Cloud settings: {str(e)}")
            return False
    
    @classmethod
    def load_gcloud_settings(cls) -> Tuple[Optional[str], Optional[str]]:
        """
        Load Google Cloud settings from local config file.
        
        Returns:
            Tuple of (gcloud_cred_path, bucket_name) or (None, None) if not found
        """
        try:
            if not cls.GCLOUD_CONFIG_FILE.exists():
                logging.info("No saved Google Cloud settings found")
                return None, None
            
            with open(cls.GCLOUD_CONFIG_FILE, 'r') as f:
                settings = json.load(f)
            
            gcloud_cred_path = settings.get('gcloud_cred_path')
            bucket_name = settings.get('bucket_name')
            
            if gcloud_cred_path and bucket_name:
                logging.info("Google Cloud settings loaded from config file")
                return gcloud_cred_path, bucket_name
            
            return None, None
        except Exception as e:
            logging.error(f"Failed to load Google Cloud settings: {str(e)}")
            return None, None
    
    @classmethod
    def clear_gcloud_settings(cls):
        """Clear saved Google Cloud settings."""
        try:
            if cls.GCLOUD_CONFIG_FILE.exists():
                cls.GCLOUD_CONFIG_FILE.unlink()
                logging.info("Google Cloud settings cleared")
                return True
        except Exception as e:
            logging.error(f"Failed to clear Google Cloud settings: {str(e)}")
        return False


class VRStoMPSConverter:
    """Utility wrapper that runs VRS processing via Meta's MPS service.

    This class uses the aria_mps CLI (Project Aria SDK) to process VRS
    recordings via Meta's Machine Perception Services (MPS). The output
    are processed outputs (SLAM, gaze, hand-tracking data, summary files),
    not a single "MPS file".
    """
    
    def __init__(self, aria_username: str, aria_password: str):
        """
        Initialize the converter with Aria credentials.
        
        Args:
            aria_username: Aria username from environment or argument
            aria_password: Aria password from environment or argument
        """
        self.aria_username = aria_username
        self.aria_password = aria_password
        self.setup_logging()
    
    @staticmethod
    def setup_logging():
        """Configure logging for the test program."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('vrs_to_mps_test.log'),
                logging.StreamHandler()
            ]
        )
    
 
    @staticmethod
    def validate_vrs_file(vrs_path: str) -> bool:
        """
        Validate that the VRS file exists and is readable.
        
        Args:
            vrs_path: Path to the VRS file
        
        Returns:
            True if file exists and is readable, False otherwise
        """
        try:
            path = Path(vrs_path)
            if not path.exists():
                logging.error(f"VRS file not found: {vrs_path}")
                return False
            
            if not path.is_file():
                logging.error(f"Path is not a file: {vrs_path}")
                return False
            
            if not path.suffix.lower() == '.vrs':
                logging.warning(f"File does not have .vrs extension: {vrs_path}")
            
            file_size = path.stat().st_size
            logging.info(f"VRS file validated: {vrs_path} ({file_size} bytes)")
            return True
        
        except Exception as e:
            logging.error(f"Error validating VRS file: {str(e)}")
            return False

    @staticmethod
    def _resolve_aria_executable() -> Optional[str]:
        """Locate Aria CLI executable on Windows."""
        env_path = os.getenv("ARIA_CLI_PATH")
        if env_path and Path(env_path).exists():
            return env_path

        candidates = [
            "aria_mps",
            "aria_mps.exe",
            "aria_mps.bat",
            "aria-cli",
            "aria-cli.exe",
            "aria-cli.bat",
        ]

        for name in candidates:
            resolved = shutil.which(name)
            if resolved:
                return resolved

        user_base = site.getuserbase()
        version_tag = f"Python{sys.version_info.major}{sys.version_info.minor}"
        scripts_dir = Path(user_base) / version_tag / "Scripts"
        for name in candidates:
            candidate_path = scripts_dir / name
            if candidate_path.exists():
                return str(candidate_path)

        current_scripts_dir = Path(sys.executable).parent
        for name in candidates:
            candidate_path = current_scripts_dir / name
            if candidate_path.exists():
                return str(candidate_path)

        return None

    @staticmethod
    def _expected_mps_output_dir(vrs_file: str) -> Path:
        vrs_path = Path(vrs_file)
        base_name = vrs_path.name
        if vrs_path.suffix.lower() == ".vrs":
            base_name = vrs_path.name[:-4]
        return vrs_path.parent / f"mps_{base_name}_vrs"
    
    def convert_vrs_to_mps(self, vrs_file: str, output_dir: str, progress_callback=None, auth_lock=None) -> Tuple[bool, Optional[str]]:
        """
        Process a VRS file via Meta's MPS service using the aria_mps CLI.

        Args:
            vrs_file: Path to the input VRS file
            output_dir: Path to the output directory for processed MPS outputs
            progress_callback: Optional callback function for progress updates (message, percentage)
            auth_lock: Optional threading.Lock to serialize authentication (prevents race conditions)

        Returns:
            Tuple of (success: bool, output_directory: Optional[str])
        """
        def update_progress(message: str, percentage: float = -1.0):
            """Helper to update progress."""
            logging.info(message)
            if progress_callback:
                progress_callback(message, percentage)

        try:
            # Validate VRS file
            if not self.validate_vrs_file(vrs_file):
                update_progress("Failed to validate VRS file", 0.0)
                return False, None

            # Create output directory if it doesn't exist
            try:
                Path(output_dir).mkdir(parents=True, exist_ok=True)
                logging.info(f"Output directory ready: {output_dir}")
            except Exception as e:
                logging.error(f"Failed to create output directory: {str(e)}")
                update_progress(f"Failed to create output directory: {str(e)}", 0.0)
                return False, None

            # Resolve Aria CLI executable from PATH or user Scripts
            aria_executable = self._resolve_aria_executable()

            if not aria_executable:
                logging.error("Aria CLI not found in PATH, venv, or user Scripts. Ensure it is installed and available.")
                update_progress("Aria CLI not found. Check installation or PATH.", 0.0)
                return False, None

            aria_name = Path(aria_executable).stem.lower()
            use_mps_cli = aria_name == "aria_mps"

            if use_mps_cli:
                aria_command = [
                    aria_executable,
                    "single",
                    "-i", vrs_file,
                    "--username", self.aria_username,
                    "--password", self.aria_password,
                    "--no-ui",
                    "--no-save-token",
                ]
                source_output_dir = self._expected_mps_output_dir(vrs_file)
            else:
                aria_command = [
                    aria_executable,
                    "--username", self.aria_username,
                    "--password", self.aria_password,
                    "--Input", vrs_file,
                    "--Output", output_dir,
                ]
                source_output_dir = Path(output_dir)

            # Log command without password
            safe_command = aria_command.copy()
            if "--password" in safe_command:
                pwd_index = safe_command.index("--password") + 1
                if pwd_index < len(safe_command):
                    safe_command[pwd_index] = "***"
            logging.info(f"Executing Aria CLI command (VRS: {Path(vrs_file).name}): {' '.join(safe_command)}")
            logging.info(f"Expected output directory: {source_output_dir if use_mps_cli else output_dir}")

            # Use auth lock to prevent concurrent authentication (fixes race condition)
            # aria_mps authenticates early in the process, so we lock during startup
            if auth_lock is not None:
                logging.info(f"Acquiring authentication lock for {Path(vrs_file).name}...")
                auth_lock.acquire()
                try:
                    # Execute the Aria CLI command
                    process = subprocess.Popen(
                        aria_command,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                    )
                    # Wait a bit for authentication to complete before releasing lock
                    # This ensures auth finishes before next process starts authenticating
                    time.sleep(3)
                    logging.info(f"Authentication lock released for {Path(vrs_file).name}")
                finally:
                    auth_lock.release()
            else:
                # No lock provided, run normally
                process = subprocess.Popen(
                    aria_command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                )

            # Read output from aria_mps and update progress
            output_lines = []
            last_percentage = -1.0
            last_debug_message_time = time.time()
            last_percentage_line = ""

            def _parse_percentage(line: str) -> Optional[float]:
                import re
                # Match percentage patterns, including those with leading decimal like .67%
                # Also match from right to left to get the last percentage in the line
                matches = list(re.finditer(r"(\d{1,3}(?:\.\d+)?|\.\d+)\s*%", line))
                if not matches:
                    return None
                try:
                    # Use the last match in case there are multiple percentages
                    val = float(matches[-1].group(1))
                    # If value starts with just a decimal (like .67), it's already fractional
                    # If value is between 0-1, it's already the correct decimal representation
                    # If value is >1, it's a normal percentage
                    return val
                except Exception:
                    return None

            def _truncate_percentage_in_line(line: str) -> str:
                """Truncate percentage values in a line to 2 decimal places."""
                import re
                def _truncate_match(m):
                    try:
                        val = float(m.group(1))
                        trunc = math.trunc(val * 100) / 100.0
                        return f"{trunc:.2f}%"
                    except Exception:
                        return m.group(0)
                return re.sub(r"(\d{1,3}(?:\.\d+)?|\.\d+)\s*%", _truncate_match, line)
            
            def _extract_stage(line: str) -> Optional[str]:
                """Extract the processing stage from aria_mps output."""
                # Match specific patterns from aria_mps output
                import re

                # Hashing stage
                if re.search(r'\bHashing\b', line):
                    return 'Hashing'

                # Indexing/health check stage
                if re.search(r'\bIndex\b', line, re.IGNORECASE) or re.search(r'Health[\s_-]?check', line, re.IGNORECASE):
                    return 'Index'

                # Downloaded stage (MPS being downloaded)
                if re.search(r'\bDownloaded\b', line, re.IGNORECASE) or re.search(r'\bDownloading\b', line, re.IGNORECASE):
                    return 'Downloaded'

                # Encrypting stage
                if re.search(r'\bEncrypting\b', line, re.IGNORECASE) or re.search(r'\bEncryption\b', line, re.IGNORECASE):
                    return 'Encrypting'

                # Uploading stage
                if re.search(r'\bUploading\b', line, re.IGNORECASE):
                    return 'Uploading'

                return None
            
            def _clean_message(line: str) -> str:
                """Clean up aria_mps log line to show only relevant message."""
                import re
                
                # Remove timestamp prefix: "2026-02-12 15:57:22,438 [PID] [LEVEL] [module:line] - "
                # Pattern: date time,ms [number] [LEVEL] [file:line] - actual message
                cleaned = re.sub(r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d+\s+\[\d+\]\s+\[\w+\]\s+\[[^\]]+\]\s+-\s+', '', line)
                
                # If pattern didn't match, try simpler patterns
                if cleaned == line:
                    # Try removing just leading timestamp
                    cleaned = re.sub(r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d+\s+', '', line)
                
                # Remove [vrs:path] prefix if present, keep it cleaner
                cleaned = re.sub(r'\[vrs:[^\]]+\]\s*', '', cleaned)
                
                # Clean up "Uploading with chunk_size X.XX MB |" to just show the important part
                cleaned = re.sub(r'Uploading\s+with\s+chunk_size\s+[\d.]+\s+MB\s+\|\s+', 'Uploading: ', cleaned)
                
                return cleaned.strip()
            
            current_stage = None

            if process.stdout:
                for line in process.stdout:
                    sline = line.rstrip("\n")
                    output_lines.append(sline)
                    logging.info(f"  {sline}")

                    # Extract stage and percentage from this line
                    stage = _extract_stage(sline)
                    if stage:
                        current_stage = stage
                    
                    pct = _parse_percentage(sline)
                    if pct is not None:
                        last_percentage = pct
                        last_percentage_line = sline
                        # Update percentage immediately (no message to debug box, just the percentage)
                        # This allows progress bar to update every 1 second
                        pct_trunc = math.trunc(pct * 100) / 100.0
                        # Also pass the stage info to the callback
                        if current_stage:
                            update_progress(current_stage, pct_trunc)
                        else:
                            update_progress("", pct_trunc)

                    # Check if this is an error/exception line (show immediately)
                    if "error" in sline.lower() or "exception" in sline.lower():
                        update_progress(sline, -1.0)
                        continue

                    # Send debug message every 5 seconds (only the latest percentage status)
                    now = time.time()
                    if (now - last_debug_message_time) >= 5.0:
                        if last_percentage >= 0 and last_percentage_line:
                            # Clean and format the message for display
                            cleaned_line = _clean_message(last_percentage_line)
                            truncated_line = _truncate_percentage_in_line(cleaned_line)
                            
                            # If we have a stage, prepend it for clarity
                            if current_stage:
                                display_message = f"{current_stage}: {truncated_line}"
                            else:
                                display_message = truncated_line
                            
                            update_progress(display_message, -1.0)  # -1 means don't update percentage (already updated above)
                        last_debug_message_time = now

            process.wait()

            if process.returncode == 0:
                if use_mps_cli:
                    if not source_output_dir.exists():
                        logging.error(f"Expected MPS output not found: {source_output_dir}")
                        update_progress("Conversion completed, but output folder is missing.", 0.0)
                        return False, None
                    
                    # Check if we need to move files to a different location
                    # If output_dir is the same as source_output_dir, no need to move
                    need_to_move = str(Path(output_dir).resolve()) != str(source_output_dir.resolve())
                    
                    if need_to_move:
                        # Move (not copy) to avoid file locking and duplication
                        try:
                            logging.info(f"Moving conversion output from {source_output_dir} to {output_dir}...")
                            
                            # If destination exists, remove it first
                            if os.path.exists(output_dir):
                                logging.info(f"Removing existing directory: {output_dir}")
                                shutil.rmtree(output_dir)
                            
                            # Move the directory
                            shutil.move(str(source_output_dir), output_dir)
                            logging.info(f"Successfully moved conversion output to {output_dir}")
                            final_output_dir = output_dir
                        except Exception as e:
                            logging.error(f"Error moving conversion output: {str(e)}")
                            update_progress(f"Error moving files: {str(e)}", 0.0)
                            return False, None
                    else:
                        # Files are already in the right place, no move needed
                        logging.info(f"Output directory is same as source, no move needed: {source_output_dir}")
                        final_output_dir = str(source_output_dir)
                else:
                    # For non-aria_mps CLI, output is directly in output_dir
                    final_output_dir = output_dir

                # List generated MPS files
                try:
                    mps_files = os.listdir(final_output_dir)
                    logging.info("VRS to MPS conversion successful!")
                    logging.info(f"Generated {len(mps_files)} file(s):")
                    for mps_file in mps_files:
                        file_path = os.path.join(final_output_dir, mps_file)
                        if os.path.isfile(file_path):
                            file_size = os.path.getsize(file_path)
                            logging.info(f"  - {mps_file} ({file_size} bytes)")
                except Exception as e:
                    logging.warning(f"Could not list output files: {str(e)}")

                if last_percentage >= 0:
                    update_progress("", max(100.0, last_percentage))
                
                # Debug: Log the output directory and its contents before returning
                logging.info(f"Conversion complete. Final output directory: {final_output_dir}")
                if os.path.exists(final_output_dir):
                    dir_contents = os.listdir(final_output_dir)
                    logging.info(f"Output directory contains {len(dir_contents)} item(s):")
                    for item in dir_contents:
                        item_path = os.path.join(final_output_dir, item)
                        if os.path.isfile(item_path):
                            size = os.path.getsize(item_path)
                            logging.info(f"  FILE: {item} ({size} bytes)")
                        elif os.path.isdir(item_path):
                            logging.info(f"  DIR:  {item}/")
                else:
                    logging.error(f"Output directory does not exist: {final_output_dir}")
                
                return True, final_output_dir
            else:
                error_msg = f"Return code: {process.returncode}"
                logging.error(f"Aria CLI failed: {error_msg}")
                logging.error(f"Full conversion output:")
                logging.error("\n".join(output_lines))
                update_progress(f"Conversion failed: {error_msg}", 0.0)
                return False, None
        
        except FileNotFoundError:
            logging.error("Aria CLI not found. Make sure 'aria-cli' (or 'aria') is installed and in PATH.")
            logging.error("Install via: pip install projectaria")
            update_progress("Aria CLI not found. Is it installed?", 0.0)
            return False, None
        
        except Exception as e:
            logging.error(f"VRS to MPS conversion error: {str(e)}")
            update_progress(f"Conversion error: {str(e)}", 0.0)
            return False, None


class GoogleCloudUploader:
    """Google Cloud Storage uploader for converted MPS files."""
    
    def __init__(self, credentials_path: str):
        """
        Initialize the uploader with Google Cloud credentials.
        
        Args:
            credentials_path: Path to service account JSON file
        """
        self.credentials_path = credentials_path
        self.client = None
    
    def initialize_client(self) -> Tuple[bool, Optional[str]]:
        """
        Initialize Google Cloud Storage client.
        
        Returns:
            Tuple of (success: bool, error_message: Optional[str])
        """
        try:
            self.client = storage.Client.from_service_account_json(self.credentials_path)
            logging.info("Google Cloud Storage client initialized")
            return True, None
        except Exception as e:
            error_msg = f"Failed to initialize Google Cloud client: {str(e)}"
            logging.error(error_msg)
            return False, error_msg
    
    def verify_bucket(self, bucket_name: str) -> Tuple[bool, Optional[str]]:
        """
        Verify that the bucket exists and is accessible.
        
        Args:
            bucket_name: Name of the Google Cloud Storage bucket
        
        Returns:
            Tuple of (success: bool, error_message: Optional[str])
        """
        try:
            if self.client is None:
                return False, "Google Cloud client not initialized"
            self.client.get_bucket(bucket_name)
            logging.info(f"Bucket '{bucket_name}' verified")
            return True, None
        except Exception as e:
            error_msg = f"Could not access bucket '{bucket_name}': {str(e)}"
            logging.error(error_msg)
            return False, error_msg
    
    def upload_file(self, bucket_name: str, file_path: str, folder_prefix: str = "", 
                   progress_callback=None) -> Tuple[bool, Optional[str]]:
        """
        Upload a single file to Google Cloud Storage.
        
        Args:
            bucket_name: Name of the bucket
            file_path: Path to the file to upload
            folder_prefix: Optional folder prefix in the bucket
            progress_callback: Optional callback for progress updates
        
        Returns:
            Tuple of (success: bool, error_message: Optional[str])
        """
        try:
            if self.client is None:
                return False, "Google Cloud client not initialized"
            bucket = self.client.bucket(bucket_name)
            file_name = os.path.basename(file_path)
            dest_name = f"{folder_prefix.rstrip('/')}/{file_name}" if folder_prefix else file_name
            
            if progress_callback:
                progress_callback(f"Uploading {file_name}...", -1)
            
            blob = bucket.blob(dest_name)
            blob.upload_from_filename(file_path)
            
            logging.info(f"Successfully uploaded {file_name} to {dest_name}")
            return True, None
        except Exception as e:
            error_msg = f"Failed to upload '{file_path}': {str(e)}"
            logging.error(error_msg)
            return False, error_msg
    
    def upload_directory(self, bucket_name: str, directory_path: str, folder_prefix: str = "",
                        progress_callback=None) -> Tuple[bool, Optional[str], int]:
        """
        Upload all files from a directory and its subdirectories to Google Cloud Storage.
        Preserves directory structure in the bucket.
        
        Args:
            bucket_name: Name of the bucket
            directory_path: Path to the directory containing files
            folder_prefix: Optional folder prefix in the bucket
            progress_callback: Optional callback for progress updates
        
        Returns:
            Tuple of (success: bool, error_message: Optional[str], files_uploaded: int)
        """
        try:
            if self.client is None:
                return False, "Google Cloud client not initialized", 0
            
            # Debug: Check if directory exists
            logging.info(f"Checking upload directory: {directory_path}")
            if not os.path.exists(directory_path):
                error_msg = f"Directory does not exist: {directory_path}"
                logging.error(error_msg)
                return False, error_msg, 0
            
            # Recursively find all files in the directory tree
            all_files = []
            for root, dirs, files in os.walk(directory_path):
                for file_name in files:
                    file_path = os.path.join(root, file_name)
                    all_files.append((file_path, file_name, root))
            
            logging.info(f"Found {len(all_files)} file(s) (including in subdirectories) in {directory_path}")
            if all_files:
                for file_path, _, _ in all_files:
                    rel_path = os.path.relpath(file_path, directory_path)
                    logging.info(f"  - {rel_path}")
            
            if not all_files:
                return False, "No files found in directory", 0
            
            bucket = self.client.bucket(bucket_name)
            uploaded_count = 0
            dir_base_name = os.path.basename(directory_path)
            
            for idx, (file_path, file_name, root_dir) in enumerate(all_files, 1):
                # Get relative path from the base directory to preserve structure
                rel_path = os.path.relpath(file_path, directory_path)
                
                # Build destination path: prefix/foldername/relative/path
                if folder_prefix:
                    dest_name = f"{folder_prefix.rstrip('/')}/{dir_base_name}/{rel_path}"
                else:
                    dest_name = f"{dir_base_name}/{rel_path}"
                
                # Normalize path separators for GCS (use forward slashes)
                dest_name = dest_name.replace("\\", "/")
                
                if progress_callback:
                    progress_callback(f"Uploading {idx}/{len(all_files)}: {rel_path}", -1)
                
                try:
                    blob = bucket.blob(dest_name)
                    blob.upload_from_filename(file_path)
                    logging.info(f"Uploaded {rel_path} -> {dest_name}")
                    uploaded_count += 1
                except Exception as e:
                    logging.warning(f"Failed to upload {rel_path}: {str(e)}")
                    continue
            
            if uploaded_count == len(all_files):
                return True, None, uploaded_count
            else:
                return False, f"Only {uploaded_count}/{len(all_files)} files uploaded successfully", uploaded_count
        
        except Exception as e:
            error_msg = f"Error uploading directory: {str(e)}"
            logging.error(error_msg)
            return False, error_msg, 0


class CombinedConverterUploaderGUI:
    """Combined GUI for VRS to MPS conversion and Google Cloud upload."""
    
    def __init__(self, root):
        """Initialize the GUI application."""
        self.root = root
        self.root.title("Aria VRS to MPS Converter + Google Cloud Uploader")
        self.root.geometry("580x500")
        self.root.resizable(True, True)
        
        # Configure logging
        self.setup_logging()
        
        # Initialize variables
        self.selected_files = []  # List of files to convert and upload
        self.save_location = None
        self.converter = None
        self.uploader = None
        self.conversion_thread = None
        # Latest progress percentage (updated by background worker)
        self._latest_pct: Optional[float] = None
        # ID returned by `after` for the periodic refresher
        self._progress_refresher_id: Optional[str] = None
        # Track progress and status per file for parallel processing
        self._file_progress: dict = {}  # {file_name: percentage}
        self._file_status: dict = {}  # {file_name: status_message}
        self._status_display_timer_id: Optional[str] = None
        self._processing_lock = threading.Lock()
        # Limit concurrent aria_mps processes to avoid resource conflicts
        self._max_concurrent_conversions = 2  # Default value
        self._conversion_semaphore = threading.Semaphore(self._max_concurrent_conversions)

        # Global authentication lock - only one aria_mps can authenticate at a time
        # (fixes race condition when multiple instances auth with same credentials)
        self._auth_lock = threading.Lock()
        
        # Try to load saved Aria credentials
        self.saved_username, self.saved_password = CredentialsManager.load_credentials()
        
        # Try to load saved Google Cloud settings
        self.saved_gcloud_cred_path, self.saved_bucket_name = CredentialsManager.load_gcloud_settings()

        # Processing mode selection
        self.process_mode_var = tk.StringVar(value="convert_upload")
        
        # Create scrollable container
        self.create_scrollable_container()
        
        # Build the UI on the inner frame
        self.build_ui()
        
        logging.info("GUI Application initialized")
    
    def create_scrollable_container(self):
        """Create a scrollable container for the GUI content."""
        # Create main canvas with scrollbar
        self.main_frame = tk.Frame(self.root)
        self.main_frame.pack(fill="both", expand=True)
        
        # Create canvas
        self.canvas = tk.Canvas(self.main_frame, bg="white", highlightthickness=0)
        scrollbar = tk.Scrollbar(self.main_frame, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=scrollbar.set)
        
        # Pack canvas and scrollbar
        self.canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Create inner frame for content
        self.inner_frame = tk.Frame(self.canvas, bg="white")
        self.canvas_window = self.canvas.create_window((0, 0), window=self.inner_frame, anchor="nw")
        
        # Bind scrollbar updates
        self.inner_frame.bind(
            "<Configure>",
            lambda event: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )
        
        # Bind mousewheel scrolling
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)
    
    def _on_mousewheel(self, event):
        """Handle mousewheel scrolling."""
        if event.num == 5 or event.delta < 0:
            self.canvas.yview_scroll(1, "units")
        elif event.num == 4 or event.delta > 0:
            self.canvas.yview_scroll(-1, "units")
    
    @staticmethod
    def setup_logging():
        """Configure logging for the application."""
        log_file = Path.home() / '.aria_uploader' / 'aria_uploader_v2.log'
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(str(log_file)),
                logging.StreamHandler()
            ]
        )
    
    def build_ui(self):
        """Build the user interface."""
        # Title
        title_label = tk.Label(
            self.inner_frame,
            text="Aria VRS to MPS + Cloud Upload Pipeline",
            font=("Arial", 16, "bold"),
            fg="#2c3e50",
            bg="white"
        )
        title_label.pack(pady=15)
        
        # Aria Credentials Section
        self.build_aria_credentials_section()
        
        # File Selection Section
        self.build_file_section()
        
        # Concurrency Settings Section
        self.build_concurrency_section()
        
        # Google Cloud Section
        self.build_gcloud_section()
        
        # Progress Section
        self.build_progress_section()
        
        # Status Label
        self.status_label = tk.Label(
            self.inner_frame,
            text="Ready",
            fg="blue",
            font=("Arial", 10),
            bg="white"
        )
        self.status_label.pack(pady=10)
        
        # Buttons Section
        self.build_buttons_section()
    
    def build_aria_credentials_section(self):
        """Build the Aria credentials input section."""
        cred_frame = tk.LabelFrame(
            self.inner_frame,
            text="Aria Credentials",
            padx=15,
            pady=10,
            font=("Arial", 10, "bold")
        )
        cred_frame.pack(padx=15, pady=10, fill="both")
        
        # Username
        tk.Label(cred_frame, text="Username:").grid(row=0, column=0, sticky="w", pady=5)
        self.username_entry = tk.Entry(cred_frame, width=35)
        self.username_entry.grid(row=0, column=1, padx=5, pady=5)
        if self.saved_username:
            self.username_entry.insert(0, self.saved_username)
        
        # Password
        tk.Label(cred_frame, text="Password:").grid(row=1, column=0, sticky="w", pady=5)
        self.password_entry = tk.Entry(cred_frame, width=35, show="*")
        self.password_entry.grid(row=1, column=1, padx=5, pady=5)
        if self.saved_password:
            self.password_entry.insert(0, self.saved_password)
        
        # Save credentials checkbox
        self.save_creds_var = tk.BooleanVar(value=bool(self.saved_username))
        cred_checkbox = tk.Checkbutton(
            cred_frame,
            text="Save credentials locally",
            variable=self.save_creds_var
        )
        cred_checkbox.grid(row=2, column=0, sticky="w", pady=5)
        
        # Clear credentials button
        clear_btn = tk.Button(
            cred_frame,
            text="Clear Saved Credentials",
            command=self.clear_credentials,
            bg="#e74c3c",
            fg="white",
            width=20
        )
        clear_btn.grid(row=2, column=1, sticky="e", pady=5, padx=5)
    
    def build_file_section(self):
        """Build the file selection section."""
        file_frame = tk.LabelFrame(
            self.inner_frame,
            text="File Selection",
            padx=15,
            pady=10,
            font=("Arial", 10, "bold")
        )
        file_frame.pack(padx=15, pady=10, fill="both")
        
        # VRS File Selection
        tk.Button(
            file_frame,
            text="Select VRS Files",
            command=self.select_vrs_file,
            bg="#3498db",
            fg="white",
            width=35,
            height=2
        ).pack(pady=5)
        
        # Files list frame with scrollbar
        list_frame = tk.Frame(file_frame)
        list_frame.pack(pady=5, fill="both", expand=True)
        
        list_scrollbar = tk.Scrollbar(list_frame)
        list_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.files_listbox = tk.Listbox(
            list_frame,
            height=5,
            yscrollcommand=list_scrollbar.set,
            font=("Courier", 8)
        )
        list_scrollbar.config(command=self.files_listbox.yview)
        self.files_listbox.pack(side=tk.LEFT, fill="both", expand=True)
        
        # Remove button frame
        button_frame = tk.Frame(file_frame)
        button_frame.pack(pady=5, fill="x")
        
        tk.Button(
            button_frame,
            text="Remove Selected",
            command=self.remove_selected_file,
            bg="#e74c3c",
            fg="white",
            width=20
        ).pack(side=tk.LEFT, padx=2)
        
        tk.Button(
            button_frame,
            text="Clear All",
            command=self.clear_all_files,
            bg="#e67e22",
            fg="white",
            width=15
        ).pack(side=tk.LEFT, padx=2)

        # Processing mode selection
        mode_frame = tk.LabelFrame(
            file_frame,
            text="Processing Mode",
            padx=10,
            pady=5,
            font=("Arial", 9, "bold")
        )
        mode_frame.pack(pady=5, fill="x")

        tk.Radiobutton(
            mode_frame,
            text="Convert + Upload (MPS)",
            variable=self.process_mode_var,
            value="convert_upload"
        ).pack(anchor="w")

        tk.Radiobutton(
            mode_frame,
            text="Convert Only (no GCS upload)",
            variable=self.process_mode_var,
            value="convert_only"
        ).pack(anchor="w")

        tk.Radiobutton(
            mode_frame,
            text="Upload VRS Only (no conversion)",
            variable=self.process_mode_var,
            value="upload_only"
        ).pack(anchor="w")
        
        # Save Location Selection (Optional)
        save_location_frame = tk.Frame(file_frame)
        save_location_frame.pack(pady=5)
        
        tk.Button(
            save_location_frame,
            text="Select Save Location (Optional)",
            command=self.select_save_location,
            bg="#3498db",
            fg="white",
            width=30,
            height=2
        ).pack(side="left", padx=(0, 5))
        
        tk.Button(
            save_location_frame,
            text="Clear",
            command=self.clear_save_location,
            bg="#e74c3c",
            fg="white",
            width=8,
            height=2
        ).pack(side="left")
        
        self.location_label = tk.Label(
            file_frame,
            text="Not selected (will use VRS file location)",
            fg="gray",
            wraplength=400
        )
        self.location_label.pack(pady=5)
    
    def build_concurrency_section(self):
        """Build the max concurrent conversions section."""
        concurrency_frame = tk.LabelFrame(
            self.inner_frame,
            text="Concurrency Settings",
            padx=15,
            pady=10,
            font=("Arial", 10, "bold")
        )
        concurrency_frame.pack(padx=15, pady=10, fill="both")
        
        # Max concurrent conversions input
        input_frame = tk.Frame(concurrency_frame)
        input_frame.pack(fill="x", pady=5)
        
        tk.Label(input_frame, text="Max Concurrent Conversions:").pack(side="left", padx=(0, 5))
        self.max_concurrent_var = tk.StringVar(value=str(self._max_concurrent_conversions))
        self.max_concurrent_entry = tk.Entry(input_frame, width=10)
        self.max_concurrent_entry.insert(0, str(self._max_concurrent_conversions))
        self.max_concurrent_entry.pack(side="left", padx=5)
        
        # Apply button
        tk.Button(
            input_frame,
            text="Apply",
            command=self.update_max_concurrent_conversions,
            bg="#3498db",
            fg="white",
            width=8
        ).pack(side="left", padx=5)
        
        # Informational label
        info_label = tk.Label(
            concurrency_frame,
            text="⚠️  Higher values enable more parallel VRS to MPS conversions, but use more system resources (CPU & RAM).\nStart with 2 and increase if you have spare resources. Reducing this improves system stability.",
            fg="#e74c3c",
            font=("Arial", 9),
            justify="left",
            wraplength=500
        )
        info_label.pack(pady=10, fill="x")
        
        # Current status label
        self.concurrency_status_label = tk.Label(
            concurrency_frame,
            text=f"Currently: {self._max_concurrent_conversions} conversions",
            fg="blue",
            font=("Arial", 9)
        )
        self.concurrency_status_label.pack(pady=5)
    
    def update_max_concurrent_conversions(self):
        """Update the maximum number of concurrent conversions."""
        try:
            new_value = int(self.max_concurrent_entry.get())
            if new_value < 1:
                messagebox.showerror("Invalid Input", "Max concurrent conversions must be at least 1.")
                return
            if new_value > 16:
                messagebox.showwarning("Warning", "Setting high values (>16) may overwhelm your system.\nProceed with caution.")
            
            self._max_concurrent_conversions = new_value
            # Recreate the semaphore with the new value
            self._conversion_semaphore = threading.Semaphore(new_value)
            
            # Update status label
            self.concurrency_status_label.config(text=f"Currently: {new_value} conversions")
            
            messagebox.showinfo("Success", f"Max concurrent conversions set to {new_value}.\nThis will take effect for the next batch of conversions.")
            logging.info(f"Max concurrent conversions updated to {new_value}")
        except ValueError:
            messagebox.showerror("Invalid Input", "Please enter a valid integer.")
    
    def build_gcloud_section(self):
        """Build the Google Cloud section."""
        gcloud_frame = tk.LabelFrame(
            self.inner_frame,
            text="Google Cloud Storage",
            padx=15,
            pady=10,
            font=("Arial", 10, "bold")
        )
        gcloud_frame.pack(padx=15, pady=10, fill="both")
        
        # Service Account JSON
        json_frame = tk.Frame(gcloud_frame)
        json_frame.pack(fill="x", pady=5)
        tk.Label(json_frame, text="Service Account JSON:").pack(side="left", padx=(0, 5))
        self.gcloud_cred_label = tk.Label(
            json_frame,
            text="No file selected",
            fg="gray",
            wraplength=400
        )
        self.gcloud_cred_label.pack(side="left", padx=5)
        
        tk.Button(
            gcloud_frame,
            text="Browse...",
            command=self.browse_gcloud_credentials,
            bg="#3498db",
            fg="white",
            width=15
        ).pack(pady=5)
        
        self.gcloud_cred_path = None
        if self.saved_gcloud_cred_path:
            self.gcloud_cred_path = self.saved_gcloud_cred_path
            file_name = Path(self.saved_gcloud_cred_path).name
            self.gcloud_cred_label.config(text=f"Selected: {file_name}", fg="black")
        
        # Bucket name
        bucket_frame = tk.Frame(gcloud_frame)
        bucket_frame.pack(fill="x", pady=5)
        tk.Label(bucket_frame, text="Bucket Name:").pack(side="left", padx=(0, 5))
        self.bucket_entry = tk.Entry(bucket_frame, width=40)
        self.bucket_entry.pack(side="left", padx=5)
        if self.saved_bucket_name:
            self.bucket_entry.insert(0, self.saved_bucket_name)
        
        # Save Google Cloud settings checkbox
        self.save_gcloud_var = tk.BooleanVar(value=bool(self.saved_gcloud_cred_path))
        gcloud_checkbox = tk.Checkbutton(
            gcloud_frame,
            text="Save Google Cloud settings locally",
            variable=self.save_gcloud_var
        )
        gcloud_checkbox.pack(anchor="w", pady=5)
        
        # Clear Google Cloud settings button
        clear_gcloud_btn = tk.Button(
            gcloud_frame,
            text="Clear Saved Google Cloud Settings",
            command=self.clear_gcloud_settings,
            bg="#e74c3c",
            fg="white",
            width=30
        )
        clear_gcloud_btn.pack(pady=5)
        
        # Folder option
        folder_frame = tk.Frame(gcloud_frame)
        folder_frame.pack(fill="x", pady=5)
        self.create_folder_var = tk.BooleanVar(value=False)
        tk.Checkbutton(
            folder_frame,
            text="Put files into a folder (prefix)",
            variable=self.create_folder_var,
            command=self.toggle_folder
        ).pack(side="left", padx=(0, 5))
        
        self.folder_entry = tk.Entry(folder_frame, width=40, state="disabled")
        self.folder_entry.pack(side="left", padx=5)
    
    def build_progress_section(self):
        """Build the progress bar section."""
        progress_frame = tk.LabelFrame(
            self.inner_frame,
            text="Conversion & Upload Progress",
            padx=15,
            pady=10,
            font=("Arial", 10, "bold")
        )
        progress_frame.pack(padx=15, pady=10, fill="both", expand=False)
        
        # Progress bar
        self.progress_canvas = tk.Canvas(
            progress_frame,
            width=500,
            height=30,
            bg="white",
            highlightthickness=1,
            highlightbackground="#bdc3c7"
        )
        self.progress_canvas.pack(pady=10)
        
        # Progress label
        self.progress_label = tk.Label(
            progress_frame,
            text="0%",
            font=("Arial", 10, "bold")
        )
        self.progress_label.pack()
        
        # Status text
        scrollbar = tk.Scrollbar(progress_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.status_text = tk.Text(
            progress_frame,
            height=12,
            width=60,
            yscrollcommand=scrollbar.set,
            font=("Courier", 8)
        )
        scrollbar.config(command=self.status_text.yview)
        self.status_text.pack(padx=5, pady=5, fill="both", expand=False)
    
    def build_buttons_section(self):
        """Build the action buttons section."""
        button_frame = tk.Frame(self.inner_frame)
        button_frame.pack(padx=15, pady=15, fill="x")
        
        # Start button
        self.start_button = tk.Button(
            button_frame,
            text="Start",
            command=self.start_conversion,
            bg="#27ae60",
            fg="white",
            font=("Arial", 11, "bold"),
            height=2,
            width=28
        )
        self.start_button.pack(side=tk.LEFT, padx=5)
        
        # Clear button
        clear_button = tk.Button(
            button_frame,
            text="Clear",
            command=self.clear_status,
            bg="#95a5a6",
            fg="white",
            font=("Arial", 11, "bold"),
            height=2,
            width=8
        )
        clear_button.pack(side=tk.LEFT, padx=5)

        # About button
        about_button = tk.Button(
            button_frame,
            text="About",
            command=self.show_about,
            bg="#2980b9",
            fg="white",
            font=("Arial", 10),
            height=2,
            width=8
        )
        about_button.pack(side=tk.LEFT, padx=5)

    def show_about(self):
        """Show an About dialog with author credit and link."""
        try:
            about_win = tk.Toplevel(self.root)
            about_win.title("About")
            about_win.geometry("420x180")
            about_win.resizable(False, False)

            tk.Label(
                about_win,
                text="Aria Uploader v2.3",
                font=("Arial", 14, "bold")
            ).pack(pady=(10, 5))

            tk.Label(
                about_win,
                text="Author: Leo Qu",
                font=("Arial", 10)
            ).pack()

            tk.Label(
                about_win,
                text="Role: Author",
                font=("Arial", 9)
            ).pack()

            def open_github():
                webbrowser.open("https://github.com/ilyLeonOn")

            link_btn = tk.Button(
                about_win,
                text="Open GitHub: ilyLeonOn",
                command=open_github,
                bg="#2c3e50",
                fg="white",
                width=28
            )
            link_btn.pack(pady=10)

            tk.Label(
                about_win,
                text="This project uses Meta's MPS service to process VRS recordings into\nSLAM, gaze, and hand-tracking outputs.",
                font=("Arial", 8),
                justify="center",
                wraplength=380
            ).pack(pady=(5, 0))

            tk.Button(about_win, text="Close", command=about_win.destroy).pack(pady=8)
        except Exception as e:
            messagebox.showerror("About Error", f"Failed to open About dialog: {e}")
    
    def select_vrs_file(self):
        """Open file dialog to select multiple VRS files."""
        file_paths = filedialog.askopenfilenames(
            title="Select VRS Files",
            filetypes=[("VRS Files", "*.vrs"), ("All Files", "*.*")]
        )
        if file_paths:
            for file_path in file_paths:
                if file_path not in self.selected_files:
                    self.selected_files.append(file_path)
                    logging.info(f"VRS file added: {file_path}")
            self.update_files_display()
    
    def update_files_display(self):
        """Update the files listbox display."""
        self.files_listbox.delete(0, tk.END)
        for file_path in self.selected_files:
            file_name = Path(file_path).name
            self.files_listbox.insert(tk.END, f"{file_name} ({file_path})")
    
    def remove_selected_file(self):
        """Remove the selected file from the list."""
        selection = self.files_listbox.curselection()
        if selection:
            index = selection[0]
            removed_file = self.selected_files.pop(index)
            logging.info(f"VRS file removed: {removed_file}")
            self.update_files_display()
    
    def clear_all_files(self):
        """Clear all selected files."""
        if self.selected_files:
            if messagebox.askyesno("Confirm", "Clear all selected files?"):
                self.selected_files.clear()
                self.update_files_display()
                logging.info("All VRS files cleared")
    
    def select_save_location(self):
        """Open folder dialog to select save location."""
        folder_path = filedialog.askdirectory(
            title="Select Save Location for MPS Files"
        )
        if folder_path:
            self.save_location = folder_path
            self.location_label.config(
                text=f"Location: {folder_path}",
                fg="black"
            )
            logging.info(f"Save location selected: {folder_path}")
    
    def clear_save_location(self):
        """Clear the save location and revert to default (VRS file location)."""
        self.save_location = ""
        self.location_label.config(
            text="Not selected (will use VRS file location)",
            fg="gray"
        )
        logging.info("Save location cleared - will use VRS file location")
    
    def browse_gcloud_credentials(self):
        """Browse for Google Cloud service account JSON."""
        file_path = filedialog.askopenfilename(
            title="Select Google Cloud Service Account JSON",
            filetypes=[("JSON Files", "*.json"), ("All Files", "*.*")]
        )
        if file_path:
            self.gcloud_cred_path = file_path
            file_name = Path(file_path).name
            self.gcloud_cred_label.config(
                text=f"Selected: {file_name}",
                fg="black"
            )
            logging.info(f"Google Cloud credentials selected: {file_path}")
    
    def toggle_folder(self):
        """Toggle the folder entry field."""
        state = "normal" if self.create_folder_var.get() else "disabled"
        self.folder_entry.configure(state=state)
    
    def update_progress(self, message: str, percentage: float = -1.0):
        """
        Update the progress bar and status text.
        
        Args:
            message: Status message to display (empty string means no message)
            percentage: Progress percentage (0-100), -1 for no update
        """
        # Update latest percentage (for progress bar refresher)
        if percentage >= 0:
            try:
                pct = float(percentage)
            except Exception:
                pct = 0.0
            pct = max(0.0, min(100.0, pct))
            self._latest_pct = pct
        
        # Update status text only if there's a message
        if message:
            self.status_text.insert(tk.END, message + "\n")
            self.status_text.see(tk.END)
            self.root.update_idletasks()

    def _draw_progress(self, pct: float):
        """Draw the progress bar and label for the given percentage."""
        try:
            pct = float(pct)
        except Exception:
            pct = 0.0
        pct = max(0.0, min(100.0, pct))
        
        # Draw the bar
        self.progress_canvas.delete("progress")
        width = 500 * (pct / 100.0)
        self.progress_canvas.create_rectangle(
            0, 0, width, 30,
            fill="#27ae60",
            tags="progress"
        )
        
        # Update the percentage label (always show 2 decimal places)
        self.progress_label.config(text=f"{pct:.2f}%")

    def _progress_refresher(self):
        """Periodic refresher called via `after` to update the progress bar every second."""
        if self._latest_pct is not None:
            self._draw_progress(self._latest_pct)
        # schedule next run in 1000ms
        self._progress_refresher_id = self.root.after(1000, self._progress_refresher)

    def _start_progress_refresher(self):
        if self._progress_refresher_id is None:
            self._progress_refresher_id = self.root.after(1000, self._progress_refresher)

    def _stop_progress_refresher(self):
        if self._progress_refresher_id is not None:
            try:
                self.root.after_cancel(self._progress_refresher_id)
            except Exception:
                pass
            self._progress_refresher_id = None
    
    def clear_status(self):
        """Clear the status text area."""
        self.status_text.delete("1.0", tk.END)
        self.progress_canvas.delete("progress")
        self.progress_label.config(text="0%")
        self.status_label.config(text="Ready", fg="blue")
    
    def clear_credentials(self):
        """Clear saved Aria credentials."""
        if messagebox.askyesno("Confirm", "Clear all saved credentials?"):
            CredentialsManager.clear_credentials()
            self.username_entry.delete(0, tk.END)
            self.password_entry.delete(0, tk.END)
            self.save_creds_var.set(False)
            messagebox.showinfo("Success", "Credentials cleared")
    
    def clear_gcloud_settings(self):
        """Clear saved Google Cloud settings."""
        if messagebox.askyesno("Confirm", "Clear all saved Google Cloud settings?"):
            CredentialsManager.clear_gcloud_settings()
            self.gcloud_cred_path = None
            self.gcloud_cred_label.config(text="No file selected", fg="gray")
            self.bucket_entry.delete(0, tk.END)
            self.save_gcloud_var.set(False)
            messagebox.showinfo("Success", "Google Cloud settings cleared")
    
    def start_conversion(self):
        """Start the VRS to MPS conversion and upload process."""
        process_mode = self.process_mode_var.get()

        # Validate Aria inputs
        username = self.username_entry.get().strip()
        password = self.password_entry.get().strip()

        if process_mode in ("convert_upload", "convert_only"):
            if not username or not password:
                messagebox.showerror("Error", "Please enter Aria username and password")
                return
        
        if not self.selected_files:
            messagebox.showerror("Error", "Please select at least one VRS file")
            return
        
        # Save location is now optional:
        # - If not specified, MPS files will be created in the same directory as the VRS files
        # - If specified, MPS files will be copied to the specified location
        
        # Validate Google Cloud inputs (only if uploading)
        if process_mode in ("convert_upload", "upload_only"):
            if not self.gcloud_cred_path:
                messagebox.showerror("Error", "Please select a Google Cloud service account JSON file")
                return

            if not self.bucket_entry.get().strip():
                messagebox.showerror("Error", "Please enter a bucket name")
                return

            if self.create_folder_var.get() and not self.folder_entry.get().strip():
                messagebox.showerror("Error", "Folder option selected but folder name is empty")
                return
        
        # Save Aria credentials if checkbox is checked
        if process_mode in ("convert_upload", "convert_only"):
            if self.save_creds_var.get():
                CredentialsManager.save_credentials(username, password)
        
        # Save Google Cloud settings if checkbox is checked
        if process_mode in ("convert_upload", "upload_only"):
            if self.save_gcloud_var.get():
                CredentialsManager.save_gcloud_settings(
                    self.gcloud_cred_path,
                    self.bucket_entry.get().strip()
                )
        
        # Disable start button during conversion
        self.start_button.config(state=tk.DISABLED)
        if process_mode == "convert_only":
            self.status_label.config(text="Converting (no upload)...", fg="orange")
        elif process_mode == "upload_only":
            self.status_label.config(text="Uploading VRS only...", fg="orange")
        else:
            self.status_label.config(text="Converting and uploading...", fg="orange")
        
        # Clear previous status
        self.status_text.delete("1.0", tk.END)
        # Initialize latest percentage and start refresher (updates bar every second)
        self._latest_pct = 0.0
        self._start_progress_refresher()
        
        # Create converter only if needed
        if process_mode in ("convert_upload", "convert_only"):
            self.converter = VRStoMPSConverter(username, password)
        else:
            self.converter = None
        
        # Start conversion in background thread
        self.conversion_thread = threading.Thread(
            target=self._conversion_and_upload_worker,
            args=(
                self.selected_files.copy(),  # Pass a copy of the list
                self.save_location,
                self.gcloud_cred_path,
                self.bucket_entry.get().strip(),
                self.folder_entry.get().strip() if self.create_folder_var.get() else "",
                process_mode,
            )
        )
        self.conversion_thread.daemon = True
        self.conversion_thread.start()
    
    def _conversion_and_upload_worker(self, vrs_files: list, output_dir: str, 
                                     gcloud_cred: str, bucket_name: str, folder_prefix: str,
                                     process_mode: str):
        """Background worker thread for parallel conversion and upload of multiple files."""
        try:
            total_files = len(vrs_files)
            
            # Initialize progress tracking for all files
            with self._processing_lock:
                for vrs_file in vrs_files:
                    file_name = Path(vrs_file).stem
                    self._file_progress[file_name] = 0.0
                    self._file_status[file_name] = "Queued"
            
            # Initialize Google Cloud uploader once for all files (only if uploading)
            if process_mode in ("convert_upload", "upload_only"):
                self.uploader = GoogleCloudUploader(gcloud_cred)
                self.update_progress("Initializing Google Cloud client...", -1)
                client_ok, client_error = self.uploader.initialize_client()
                if not client_ok:
                    self.status_label.config(text="[X] Google Cloud error", fg="red")
                    messagebox.showerror("Google Cloud Error", client_error)
                    self.start_button.config(state=tk.NORMAL)
                    return

                self.update_progress("Verifying bucket access...", -1)
                bucket_ok, bucket_error = self.uploader.verify_bucket(bucket_name)
                if not bucket_ok:
                    self.status_label.config(text="[X] Bucket error", fg="red")
                    messagebox.showerror("Bucket Error", bucket_error)
                    self.start_button.config(state=tk.NORMAL)
                    return
            
            # Start status display timer (shows all file statuses every 5 seconds)
            self._start_status_display_timer()
            
            # Create threads for parallel processing
            file_threads = []
            logging.info(f"Creating {total_files} worker threads for parallel processing (max 2 concurrent conversions)...")
            
            for idx, vrs_file in enumerate(vrs_files, 1):
                thread = threading.Thread(
                    target=self._process_single_file,
                    args=(vrs_file, output_dir, bucket_name, folder_prefix, idx, total_files, process_mode)
                )
                thread.daemon = True
                file_threads.append(thread)
                thread.start()
                logging.info(f"Started thread {idx}/{total_files} for {Path(vrs_file).name}")
            
            # Wait for all threads to complete
            logging.info(f"Waiting for all {total_files} threads to complete...")
            for idx, thread in enumerate(file_threads, 1):
                logging.info(f"Joining thread {idx}/{total_files}...")
                thread.join()
                logging.info(f"Thread {idx}/{total_files} joined successfully")
            
            # Stop status display timer
            self._stop_status_display_timer()
            
            # Calculate final results
            total_uploaded = sum(1 for status in self._file_status.values() if "Uploaded" in status)
            total_converted = sum(1 for status in self._file_status.values() if "Uploaded" in status or "Skipped" in status)
            
            if total_uploaded > 0:
                self.update_progress("", -1)
                self.update_progress(f"All processing complete! {total_converted} file(s) processed, {total_uploaded} uploaded.", 100)
                self.status_label.config(
                    text=f"[OK] Complete! {total_converted} file(s) processed, {total_uploaded} uploaded",
                    fg="green"
                )
                messagebox.showinfo(
                    "Success",
                    f"Processing completed!\n\n"
                    f"Files processed: {total_converted}/{total_files}\n"
                    f"Files uploaded: {total_uploaded}\n"
                    f"Uploaded to: gs://{bucket_name}/{folder_prefix if folder_prefix else '(root)'}"
                )
            else:
                self.status_label.config(text="[X] All processing failed", fg="red")
                messagebox.showerror("Failed", "No files were processed successfully")
        
        except Exception as e:
            logging.error(f"Conversion/upload worker error: {str(e)}")
            self.status_label.config(text=f"[X] Error: {str(e)}", fg="red")
            messagebox.showerror("Error", f"Error: {str(e)}")
        
        finally:
            # Stop timers and re-enable start button
            try:
                self._stop_progress_refresher()
                self._stop_status_display_timer()
            except Exception:
                pass
            self.start_button.config(state=tk.NORMAL)
    
    def _process_single_file(self, vrs_file: str, output_dir: str, bucket_name: str, 
                           folder_prefix: str, file_idx: int, total_files: int,
                           process_mode: str):
        """Process a single VRS file: conversion → upload."""
        file_name = Path(vrs_file).stem
        vrs_basename = Path(vrs_file).name
        
        logging.info(f"[{file_name}] Thread started for file {file_idx}/{total_files}")
        
        try:
            # Update status
            with self._processing_lock:
                self._file_status[file_name] = "Starting..."
            
            # Upload VRS only (no conversion)
            if process_mode == "upload_only":
                with self._processing_lock:
                    self._file_status[file_name] = "Uploading VRS..."

                upload_ok, upload_error = self.uploader.upload_file(
                    bucket_name,
                    vrs_file,
                    folder_prefix=folder_prefix,
                    progress_callback=lambda msg, pct: None
                )

                if upload_ok:
                    with self._processing_lock:
                        self._file_status[file_name] = "Uploaded VRS"
                        self._file_progress[file_name] = 100.0
                    logging.info(f"Successfully uploaded VRS {file_name}")
                else:
                    with self._processing_lock:
                        self._file_status[file_name] = f"Upload failed: {upload_error}"
                        self._file_progress[file_name] = 0.0
                    logging.error(f"Upload failed for {file_name}: {upload_error}")
                return

            # Determine output directory:
            # If output_dir is None or empty, use VRS file's parent directory
            # Otherwise, create subdirectory in user-specified location
            if output_dir:
                mps_folder_name = f"mps_{file_name}_vrs"
                file_output_dir = os.path.join(output_dir, mps_folder_name)
                logging.info(f"[{file_name}] Using user-specified output dir: {file_output_dir}")
            else:
                # Use the same directory as the VRS file
                vrs_parent = str(Path(vrs_file).parent)
                mps_folder_name = f"mps_{file_name}_vrs"
                file_output_dir = os.path.join(vrs_parent, mps_folder_name)
                logging.info(f"[{file_name}] Using VRS file directory: {file_output_dir}")
            
            # Check if MPS files already exist
            if os.path.exists(file_output_dir):
                has_files = False
                for root, dirs, files in os.walk(file_output_dir):
                    if files:
                        has_files = True
                        break
                
                if has_files:
                    with self._processing_lock:
                        self._file_status[file_name] = "Skipped (exists)"
                        self._file_progress[file_name] = 100.0
                    logging.info(f"[{file_name}] Skipping conversion - MPS files already exist at {file_output_dir}")
                    converted_dir = file_output_dir
                else:
                    logging.info(f"[{file_name}] Output dir exists but empty, converting...")
                    # Convert
                    converted_dir = self._convert_file(vrs_file, file_output_dir, file_name, vrs_basename)
            else:
                logging.info(f"[{file_name}] Output dir does not exist, converting...")
                # Convert
                converted_dir = self._convert_file(vrs_file, file_output_dir, file_name, vrs_basename)
            
            if not converted_dir:
                with self._processing_lock:
                    self._file_status[file_name] = "Conversion failed"
                    self._file_progress[file_name] = 0.0
                return
            
            # Conversion only (no upload)
            if process_mode == "convert_only":
                with self._processing_lock:
                    self._file_status[file_name] = "Conversion complete"
                    self._file_progress[file_name] = 100.0
                logging.info(f"Conversion complete (no upload) for {file_name}")
                return

            # Upload immediately after conversion
            with self._processing_lock:
                self._file_status[file_name] = "Uploading..."

            upload_ok, upload_error, files_uploaded = self.uploader.upload_directory(
                bucket_name,
                converted_dir,
                folder_prefix=folder_prefix,
                progress_callback=lambda msg, pct: None  # Suppress individual upload progress
            )

            if upload_ok or files_uploaded > 0:
                with self._processing_lock:
                    self._file_status[file_name] = f"Uploaded ({files_uploaded} files)"
                    self._file_progress[file_name] = 100.0
                logging.info(f"Successfully uploaded {file_name}: {files_uploaded} files")
            else:
                with self._processing_lock:
                    self._file_status[file_name] = f"Upload failed: {upload_error}"
                    self._file_progress[file_name] = 100.0
                logging.error(f"Upload failed for {file_name}: {upload_error}")
        
        except Exception as e:
            with self._processing_lock:
                self._file_status[file_name] = f"Error: {str(e)}"
                self._file_progress[file_name] = 0.0
            logging.error(f"Error processing {file_name}: {str(e)}")
        
        finally:
            logging.info(f"[{file_name}] Thread completing for file {file_idx}/{total_files}")
    
    def _convert_file(self, vrs_file: str, file_output_dir: str, file_name: str, vrs_basename: str) -> Optional[str]:
        """Convert a single VRS file to MPS."""
        with self._processing_lock:
            self._file_status[file_name] = "Waiting to start..."
        
        logging.info(f"[{file_name}] Waiting to acquire conversion semaphore (current value: {self._conversion_semaphore._value})...")
        
        # Create a callback that updates this file's progress and stage
        def progress_callback(message: str, percentage: float):
            # Update stage if message contains a stage name
            if message:
                with self._processing_lock:
                    self._file_status[file_name] = message
            
            if percentage >= 0:
                with self._processing_lock:
                    self._file_progress[file_name] = percentage
                # Update average progress immediately (for 1-second bar refresh)
                self._update_average_progress()
        
        # Use semaphore to limit concurrent conversions (prevent resource conflicts)
        self._conversion_semaphore.acquire()
        logging.info(f"[{file_name}] Semaphore acquired (new value: {self._conversion_semaphore._value}), starting conversion...")
        try:
            with self._processing_lock:
                self._file_status[file_name] = "Starting conversion..."
            
            success, result_dir = self.converter.convert_vrs_to_mps(
                vrs_file,
                file_output_dir,
                progress_callback=progress_callback,
                auth_lock=self._auth_lock  # Pass auth lock to prevent concurrent authentication
            )
            
            if success and result_dir:
                with self._processing_lock:
                    self._file_status[file_name] = "Conversion complete"
                    self._file_progress[file_name] = 100.0
                logging.info(f"Successfully converted {file_name} to {result_dir}")
                return result_dir
            else:
                with self._processing_lock:
                    self._file_status[file_name] = "Conversion failed (returned None)"
                logging.error(f"Conversion failed for {file_name}: returned success={success}, result_dir={result_dir}")
                logging.error(f"Check log file at ~/.aria_uploader/aria_uploader_v2.log for full conversion output")
                return None
        except Exception as e:
            with self._processing_lock:
                self._file_status[file_name] = f"Conversion exception: {str(e)}"
            logging.error(f"Exception during conversion of {file_name}: {str(e)}", exc_info=True)
            return None
        finally:
            # Always release the semaphore
            logging.info(f"[{file_name}] Releasing conversion semaphore (current value: {self._conversion_semaphore._value})...")
            self._conversion_semaphore.release()
            logging.info(f"[{file_name}] Semaphore released (new value: {self._conversion_semaphore._value})")
    
    def _update_average_progress(self):
        """Calculate and update average progress across all files."""
        with self._processing_lock:
            if self._file_progress:
                avg_pct = sum(self._file_progress.values()) / len(self._file_progress)
                self._latest_pct = avg_pct
    
    def _display_all_file_statuses(self):
        """Display status of all files (called every 5 seconds)."""
        with self._processing_lock:
            status_lines = []
            for file_name in sorted(self._file_progress.keys()):
                pct = self._file_progress[file_name]
                status = self._file_status[file_name]
                status_lines.append(f"{file_name}: {pct:.2f}% - {status}")
            
            if status_lines:
                status_text = "\n".join(status_lines)
                self.update_progress(f"\n--- STATUS UPDATE ---", -1)
                self.update_progress(status_text, -1)
        
        # Schedule next status display
        if self._status_display_timer_id is not None:
            self._status_display_timer_id = self.root.after(5000, self._display_all_file_statuses)
    
    def _start_status_display_timer(self):
        """Start the 5-second status display timer."""
        if self._status_display_timer_id is None:
            self._status_display_timer_id = self.root.after(5000, self._display_all_file_statuses)
    
    def _stop_status_display_timer(self):
        """Stop the status display timer."""
        if self._status_display_timer_id is not None:
            try:
                self.root.after_cancel(self._status_display_timer_id)
            except Exception:
                pass
            self._status_display_timer_id = None


def main_gui():
    """Launch the GUI application."""
    root = tk.Tk()
    app = CombinedConverterUploaderGUI(root)
    root.mainloop()


def main_cli():
    """Legacy command-line interface."""
    parser = argparse.ArgumentParser(
        description="VRS to MPS conversion and Google Cloud upload pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using GUI (no arguments)
  python aria_uploader_v2.py
  
  # Using CLI with arguments
  python aria_uploader_v2.py --input sample.vrs --output ./mps_output \\
    --gcloud-cred service-account.json --bucket my-bucket
        """
    )
    
    parser.add_argument(
        '--input', '-i',
        help='Path to the input VRS file (CLI mode)'
    )
    
    parser.add_argument(
        '--output', '-o',
        help='Path to the output directory for MPS files'
    )
    
    parser.add_argument(
        '--username', '-u',
        help='Aria username'
    )
    
    parser.add_argument(
        '--password', '-p',
        help='Aria password'
    )
    
    parser.add_argument(
        '--gcloud-cred',
        help='Path to Google Cloud service account JSON'
    )
    
    parser.add_argument(
        '--bucket',
        help='Google Cloud Storage bucket name'
    )
    
    parser.add_argument(
        '--folder',
        help='Optional folder prefix in the bucket'
    )
    
    args = parser.parse_args()
    
    # If no input file provided, launch GUI
    if not args.input:
        main_gui()
        return
    
    # CLI mode
    aria_username = args.username or os.getenv('ARIA_USERNAME')
    aria_password = args.password or os.getenv('ARIA_PASSWORD')
    
    if not aria_username or not aria_password:
        print("Error: Aria credentials not provided.")
        print("Set via: --username and --password arguments")
        print("Or via environment variables: ARIA_USERNAME and ARIA_PASSWORD")
        sys.exit(1)
    
    output_dir = args.output or "./mps_output"
    
    # Converter
    converter = VRStoMPSConverter(aria_username, aria_password)
    success, result_dir = converter.convert_vrs_to_mps(args.input, output_dir)
    
    if not success:
        print(f"[X] Conversion failed!")
        sys.exit(1)
    
    # Google Cloud upload
    if args.gcloud_cred and args.bucket:
        print("\nStarting upload to Google Cloud Storage...")
        uploader = GoogleCloudUploader(args.gcloud_cred)
        
        client_ok, client_error = uploader.initialize_client()
        if not client_ok:
            print(f"[X] {client_error}")
            sys.exit(1)
        
        bucket_ok, bucket_error = uploader.verify_bucket(args.bucket)
        if not bucket_ok:
            print(f"[X] {bucket_error}")
            sys.exit(1)
        
        upload_ok, upload_error, files_uploaded = uploader.upload_directory(
            args.bucket,
            output_dir,
            folder_prefix=args.folder or ""
        )
        
        if upload_ok or files_uploaded > 0:
            print(f"[OK] Uploaded {files_uploaded} file(s) successfully!")
            sys.exit(0)
        else:
            print(f"[X] {upload_error}")
            sys.exit(1)
    else:
        print(f"[OK] Conversion completed successfully!")
        print(f"[OK] Output directory: {output_dir}")
        print("(Skipping upload: --gcloud-cred and --bucket not provided)")
        sys.exit(0)


if __name__ == "__main__":
    main_cli()
