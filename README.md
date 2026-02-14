# Meta Aria Uploader v2.3

A Windows GUI application for processing Meta Aria VRS (Virtual Reality Stream) recordings via Meta's MPS (Machine Perception Services) pipeline and uploading the resulting processed outputs to Google Cloud Storage.

## Features

-- **Parallel Processing**: Process multiple VRS files simultaneously (configurable concurrency)
- **Google Cloud Integration**: Automatic upload to GCS after conversion
- **Real-time Progress Tracking**: Live progress bar and detailed status updates
- **Smart Caching**: Skips re-conversion if MPS files already exist
- **Credential Management**: Save and load Meta account and GCS credentials locally
- **Flexible Output**: Choose save location or use VRS file directory

## Author

Leo Qu — Author

GitHub: https://github.com/ilyLeonOn

## System Requirements

- **Operating System**: Windows 10/11 (64-bit)
- **Python Version**: 3.12.x (REQUIRED - other versions not compatible)
- **Disk Space**: Minimum 3x the size of your VRS files combined
- **Network**: Stable internet connection for authentication and upload

## Quick Start

### 1. Install Python 3.12
Download from: https://www.python.org/downloads/

Verify installation:
```powershell
python --version
```
Should display: `Python 3.12.x`

### 2. Create Virtual Environment
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 3. Install Dependencies

**Option A - Using requirements.txt (Recommended):**
```powershell
pip install -r requirements.txt
```

**Option B - Using pyproject.toml:**
```powershell
pip install -e .
```

**Option C - Manual Installation:**
```powershell
pip install projectaria-tools==1.7.1
pip install google-cloud-storage
```

### 4. Run the Application
```powershell
python aria_uploader_v2.py
```

## First-Time Setup

1. **Meta Account Credentials**:
   - Enter your Meta account email and password
   - Click "Save Credentials" to store locally (encrypted)

2. **Google Cloud Setup**:
   - Create a GCS service account and download JSON key
   - Browse to your JSON credentials file
   - Enter your bucket name
   - Optionally set a folder prefix for organization
   - Click "Save GCS Settings"

3. **Select VRS Files**:
   - Click "Select VRS Files" and choose one or more .vrs files
   - Optionally select a save location (or leave blank to use VRS directory)

4. **Start Processing**:
   - Click "Start"
   - Monitor progress in the GUI and terminal

## Configuration

### Concurrent Processing
Default: 2 files process simultaneously

To change, use the Concurrency Settings in the GUI (v2.3) or edit `aria_uploader_v2.3.py` variable `_max_concurrent_conversions`.

**Recommendations**:
- 2-4 CPU cores: Keep at 2
- 6-8 CPU cores: Try 3
- 10+ CPU cores + SSD: Can use 4-5

## Conversion Process

Each VRS file goes through these stages (MPS service stages):
1. **Hashing** (~5-15 min): Verifies file integrity
2. **Health Check** (~1 min): Validates VRS structure
3. **Encrypting** (~5-15 min): Creates temporary .enc file
4. **Uploading** (varies): Sends encrypted data to Meta's servers
5. **Processing** (30 min-2 hrs): Meta's MPS cloud service processes the uploaded data and produces outputs

## Output Structure

```
mps_filename_vrs/  (folder of processed MPS outputs)
├── eye_gaze/
│   └── generalized_eye_gaze.csv
├── hand_tracking/
│   └── wrist_and_palm_poses.csv
├── slam/
│   ├── closed_loop_trajectory.csv
│   ├── semidense_points.csv.gz
│   └── semidense_observations.csv.gz
└── summary.json
```

All files are automatically uploaded to Google Cloud Storage.

## Troubleshooting

### "aria_mps.exe not found"
- Reinstall: `pip install projectaria-tools==1.7.1`
- Verify: `.venv\Scripts\aria_mps.exe --help`

### "Module not found" errors
- Ensure virtual environment is activated (see `(.venv)` in prompt)
- Reinstall dependencies: `pip install -r requirements.txt`

### Conversion fails with "insufficient space"
- Need 3x VRS file size available
- Check: `Get-PSDrive` to see disk usage
- Free up space and retry

### Upload fails
- Verify bucket name is correct
- Check service account has Storage Admin role
- Test credentials: `gsutil ls gs://your-bucket-name`

### GUI doesn't appear
- Reinstall Python with tcl/tk support
- Test: `python -m tkinter`

## Documentation

For detailed documentation, see **USER_GUIDE.txt** in this folder.

Topics covered:
- Complete installation instructions
- Transferring to another computer
- Common problems and solutions
- Performance tuning
- Best practices

## File Manifest

```
Aria-Uploader-v2.2/
├── aria_uploader_v2.py      # Main application
├── requirements.txt          # Simple dependency list
├── pyproject.toml           # Project configuration
├── uv.lock                  # Version lock file (for uv users)
├── USER_GUIDE.txt           # Comprehensive documentation
└── README.md                # This file
```

## Version History

**v2.3** (February 2026)
- Concurrency settings configurable in UI (no code edits needed)
- Processing mode selection: Convert+Upload, Convert Only, Upload Only
- Improved progress tracking and logging
- Better parallel processing with dynamic semaphore control

## Support

For issues or questions:
1. Check USER_GUIDE.txt for detailed troubleshooting
2. Review log file: `%USERPROFILE%\.aria_uploader\aria_uploader_v2.log`
3. Verify all prerequisites are met (Python 3.12, correct packages)

## License

Internal use only.

---

**Note**: This application requires Project Aria Tools 1.7.1, which is only compatible with Python 3.12 on Windows.
