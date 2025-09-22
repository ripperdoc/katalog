# -------------------------
# EXTENSIONS (categorized)
# -------------------------
DOCUMENT_EXTENSIONS = {
    ".txt", ".md", ".doc", ".docx", ".odt", ".pdf", ".rtf", ".tex", ".epub",
    # Layout and publishing formats
    ".ps",        # PostScript
    ".eps",       # Encapsulated PostScript
    ".indd",      # Adobe InDesign
    ".idml",      # Adobe InDesign Markup Language
    ".qxp",       # QuarkXPress
    ".xpress",    # QuarkXPress legacy format
    ".fm",        # Adobe FrameMaker
    ".p65",       # Adobe PageMaker
    ".pub",       # Microsoft Publisher
    ".xps",       # Microsoft XPS Document
}

AUDIO_EXTENSIONS = {
    ".mp3", ".wav", ".ogg", ".flac", ".aac", ".m4a", ".aiff"
}

IMAGE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp", ".svg", ".heic", ".psd"
}

MODEL_EXTENSIONS = {
    ".obj", ".fbx", ".stl", ".gltf", ".glb", ".dae", ".blend", ".3ds", ".ply"
}

VIDEO_EXTENSIONS = {
    ".mp4", ".mov", ".avi", ".mkv", ".webm", ".flv", ".wmv"
}

CODE_EXTENSIONS = {
    ".c", ".cpp", ".h", ".java", ".py", ".js", ".ts", ".html", ".css", ".json",
    ".xml", ".php", ".rb", ".go", ".rs", ".sh", ".swift", ".kt", ".yml", ".yaml", ".ini"
}

ACHIVE_EXTENSIONS = {
    ".zip", ".7z", ".tar.gz", ".rar", ".gz", ".bz2", ".xz"
}

TABLE_EXTENSIONS = {
    ".csv", ".tsv", ".xlsx", ".xls", ".ods", ".jsonl", ".parquet"
}

SIDECARS_EXTENSIONS = {
    ".DS_Store", "thumbs.db", ".Spotlight-V100", ".Trashes", ".fseventsd", ".AppleDouble", "._filename", "ehthumbs.db", "Desktop.ini", "$RECYCLE.BIN", "System Volume Information"
}

# COMMON_SYSTEM_AND_SIDECAR_FILES = {
#     "macOS": [
#         ".DS_Store",           # Finder folder metadata
#         ".Spotlight-V100",    # Spotlight index
#         ".Trashes",           # Trash directory on external drives
#         ".fseventsd",         # Filesystem event logs
#         ".AppleDouble",       # Stores resource forks on non-HFS filesystems
#         "._filename",         # AppleDouble metadata sidecar
#         "Icon\r",             # Custom folder icon file (invisible weird name)
#     ],
#     "Windows": [
#         "Thumbs.db",          # Thumbnail cache
#         "ehthumbs.db",        # Enhanced thumbnail cache
#         "Desktop.ini",        # Folder view settings
#         "$RECYCLE.BIN/",      # Recycle Bin data on drives
#         "System Volume Information/",  # Indexing and restore data
#     ],
#     "Linux": [
#         ".Trash-1000/",       # Trash directory for mounted drives
#         ".directory",         # KDE folder metadata
#         "lost+found/",        # Recovered filesystem fragments (ext filesystems)
#     ],
#     "Application_Created": [
#         ".xmp",               # Adobe / image metadata sidecar
#         "~$filename.docx",    # Microsoft Office lock file
#         ".~lock.filename.odt#", # LibreOffice lock file
#         # Adobe cache folders:
#         "Adobe Premiere Pro Auto-Save/",
#         "Media Cache Files/",
#         # IDE / dev sidecars:
#         ".vscode/",
#         ".idea/",
#         ".gradle/",
#         "node_modules/.cache/",
#         "Icon\r",             # Sometimes created by Finder for custom icons
#     ],
#     "Backup_And_Temporary": [
#         "filename.ext~",      # Generic backup file (editors/tools)
#         "filename.ext.bak",   # Backup copy
#         "filename.ext.tmp",   # Temporary file
#         ".#filename",         # Emacs temporary lockfile
#     ],
#     "Photo_Metadata_Sidecars": [
#         ".xmp",               # Lightroom / RAW metadata
#         ".pp3",               # RawTherapee edits
#         ".on1",               # ON1 metadata
#     ],
# }

# Combine all for convenience
UGC_EXTENSIONS = (
    DOCUMENT_EXTENSIONS |
    AUDIO_EXTENSIONS |
    IMAGE_EXTENSIONS |
    MODEL_EXTENSIONS |
    VIDEO_EXTENSIONS |
    CODE_EXTENSIONS |
    ACHIVE_EXTENSIONS |
    TABLE_EXTENSIONS
)

# -------------------------
# MIME TYPES (categorized)
# -------------------------
DOCUMENT_MIME_TYPES = {
    "text/plain",
    "text/markdown",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.oasis.opendocument.text",
    "application/pdf",
    "application/rtf",
    "application/x-tex",
    "application/epub+zip",

    # Layout and publishing formats
    "application/postscript",                 # .ps
    "application/eps",                        # .eps (some systems use image/x-eps)
    "application/vnd.adobe.indesign-idml",    # .idml
    "application/vnd.quark.quarkxpress",      # .qxp, .xpress
    "application/vnd.framemaker",             # .fm
    "application/vnd.ms-publisher",           # .pub
    "application/vnd.ms-xpsdocument",         # .xps
}

AUDIO_MIME_TYPES = {
    "audio/mpeg", "audio/wav", "audio/ogg", "audio/flac", "audio/aac",
    "audio/mp4", "audio/aiff"
}

IMAGE_MIME_TYPES = {
    "image/jpeg", "image/png", "image/gif", "image/bmp", "image/tiff",
    "image/webp", "image/svg+xml", "image/heic"
}

VIDEO_MIME_TYPES = {
    "video/mp4", "video/quicktime", "video/x-msvideo", "video/x-matroska",
    "video/webm", "video/x-flv", "video/x-ms-wmv"
}

CODE_MIME_TYPES = {
    "text/x-c", "text/x-c++", "text/x-java-source", "text/x-python",
    "application/javascript", "application/typescript", "text/html",
    "text/css", "application/json", "application/xml", "application/x-httpd-php",
    "text/x-ruby", "text/x-go", "text/rust", "application/x-sh",
    "text/x-swift", "text/x-kotlin"
}

OTHER_MIME_TYPES = {
    "text/csv", "text/tab-separated-values", "text/yaml",
    "application/zip", "application/x-7z-compressed", "application/gzip",
    "image/vnd.adobe.photoshop", "application/x-blender"
}

# Combine all for convenience
UGC_MIME_TYPES = (
    DOCUMENT_MIME_TYPES |
    AUDIO_MIME_TYPES |
    IMAGE_MIME_TYPES |
    VIDEO_MIME_TYPES |
    CODE_MIME_TYPES |
    OTHER_MIME_TYPES
)