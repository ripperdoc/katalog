# DESIGN

## Principles

Here are some overall principles `katalog` should follow:

- Shoulder of giants. There are already so many great tools and libraries. Try to build on what's battle tested, don't attempt to recreate the wheel.

- Pluggable. There are so many flavors of data sources and metadata processors, an easily pluggable system makes it easy for anyone to contribute and tweak it for their needs.

- Modular. A direct consequence of both above principles is that it must be easy to link together existing functionality but in different languages, formats and executables, and that it must be easy to add functionality following well defined interfaces. Modularity also reduces the risk of getting stuck in the wrong solutions as the tool develops. Finally, it is easier to develop in small teams and with AI when things can be solved in smaller contexts and single modules than across a complex system.

- Simplicity. Easier said than done, but the core building blocks of the design should be simple. Re-use wellknown concepts where possible, and try to boil things down to the essence.

- Your data. Convenience and business logic could imply that `katalog` should be a cloud-hosted SaaS service. And it may be offered like that, but a core principle must be that it's your data. Only then can you trust it to know about all your assets and files. Therefore it must be able to run it on a local machine or self-host it in a network you control.

## Core Concepts

### Asset

An asset (in Swedish, 'ett verk') is a document, a photo, a video; any piece of content created by a human (or "creative agent"). It may exist in multiple versions, variants and in multiple files, across multiple locations. It's in the nature of an asset that it cannot be automatically and uniquely separated from other assets. This is because it's matter of a definition or context when an asset can be seen as a copy of another or be seen as a separate (but perhaps similar) asset. Also, assets may contain or be contained in other assets, such as an image or a paragraph of text.

It's a central problem to solve in `katalog` how to identify assets within a mass of files. But, the naive starting point is simply that each file is a unique asset, and even at that point `katalog` can be very useful. But the longer term value comes when `katalog` create relationships between multiple files as either versions or variants of the same asset. In this way, an asset is a one-to-many relation to files, and metadata on the set of associated files tell a story about the asset - where it exists, who created it, when and how.

### Version

With version, we mean a file that represents an asset at a certain point in time. Versions may have relationships with other versions, such that versions can be ordered and branched out.

### Variant
In our definition, a variant a file that is an alternative representation of an asset. For example, the same image may exist in multiple file formats and resolutions, but ultimately represent the same content. Often, a variant can be generated from a `master` file, such as a higher resolution or full-text representation.

### File

A file, or something file-like, is one instance of digital data that represents a version or variant of an asset. A file can be located within a source using a URI format, that may place it within one (or more) path-like hierarchies. It can be uniquely identifies this way. And it comes with a set of metadata that can tell us how and when the file was created and modified, and much more.


### Directory

A directory contains other files, which allows a tree-like structure. But the definition is wider than a normal file system folder. In a cloud storage, the same exact file may exist in multiple directories. There may be files that contain other files, such as a ZIP files, and therefore represent virtual directories. Directories are not important to Katalog in themselves, and could theoretically be reduced down to just a component in a path string. But they do have important technical implications for how to scan and find files, and also convey important structural metadata to files that exist within them.

### Source

A source is a repository of files. It can be a local folder (or complete filesystem) or a remote filesystem, or it may be an API to a cloud storage or CMS. Ultimately, it lists "file-like" bytestreams with associated metadata, in a flat or folder-type structure. The metadata and settings on a source will control how files are accessed and catalogued from within it. A source can often also be seen as a mount point, in file system terminology. The whole user journey of `katalog` starts with the user adding a first source, e.g. their "Document" folder.

### Metadata

Each file record can have a large amount of metadata, some common among all files, some specific to certain types of files. Our system needs to handle the fact that there might be multiple sources for a certain piece of metadata that may not be in agreement, such as when the file was last modified, which can be given differently by file system and file header metadata. We need to track these different "opinions" on the same property. The system needs to heuristically pick one as the current "candidate" (in e.g. file listings), and when not easily resolved, a human or smart agent could make an informed decision or provide new information using some metadata setting tools.


## Architecture


### Main server

The main module is implemented as a FastAPI server. It initializes the system, manages connectors (clients), and provides an HTTP API for actions such as scanning all or specific sources. The server tracks asynchronous scan jobs using a simple job system and offers endpoints to retrieve job status and results. As a scan progresses, it collects file information and can trigger processors on each scanned file.

A CLI or a web app can connect to the FastAPI server to act as a UI.


### Client

A `Client` is responsible for accessing and listing files in a source. Clients are implemented as Python classes following a common interface, allowing plugins for different source types. Users may have multiple clients for different sources.

#### Example: LocalFSClient

The `LocalFSClient` implements the client interface for local file systems, using `os.walk` to recursively scan directories. It provides methods:

- `__init__(root_path)`: Initialize with the root directory to scan.
- `getInfo()`: Returns static info about the client (description, author, version).
- `canConnect(uri)`: Checks if the given path is a valid directory.
- `scan()`: Recursively scans the directory, yielding file info dicts (path, size, mtime, ctime, etc).

Source data model (for all clients):

- Title
- Mounted path or URI (one or more paths that connects to this location)
- Unique ID (serial, ID, etc in order to be able to recognise reconnects)
- Credentials object
- Include and exclude rules
- Local cache and settings location
- Stats (like total size, # files)
- Last scanned
- Update strategy (depends on what the underlying connector supports)

API (for all clients):
- `init` - creates the connector from configuration
- `get_info` - statically returns information about the plugin, e.g. description, author, version
- `can_connect` - checks if a given connector can connect to a URI identifier (e.g. file path)
- `scan` - accesses the source to retrieve a stream of file information objects. May have internal state, e.g. the file database, that allows resumes or quick-rescans. The scan method should operate asynchronously, and return a stream of results as they come in (an async generator?).

### Processor

A processor is a plugin which operates on a FileRecord emitted by a SourceClient. There are multiple types of processors.

The basic approach is that for each FileRecord emitted, we go through the list of all processors that have been configured. First, we check if the processor SHOULD be run on this FileRecord. The implemented processor should define this through a function `should_run` that takes a FileRecord and returns a boolean. It may return false for two general reasons: that this processor is not applied to this file (e.g. wrong format) or because it has already run and the inputs haven't changed. The latter implies that there is a function to generate a cache key, built in the required inputs from FileRecord and the current version of the processor. If the cache key has changed, it would run, otherwise not.

If the processor should be run, we should post the job to a job manager (or just async tasks) that will run it. Once it is done, it should emit the result to the main server, which can commit the result to the database.

Note that we probably need a basic system for reactive variables here (e.g. like a spreadsheet or Redux reducers). It can also be described as a Directed Acyclic Graph. If one processor updates or creates a new metadata field, other processors should maybe now run because their `should_run` would now return true. Not sure how it should be implemented efficiently under the hood.

An example could be like this:
1. A SourceClient emits a set of FileRecords including content hashes
2. If a content hash has updated, we should now run the MimeTypeProcessor
3. If the MimeType was updated, and found to be an image, we should now run e.g. the ThumbnailGenerator and several other image processors

If later, we rescan the source and just find updates to other metadata, the content hashes hasn't changed and there is no need to run the MimeTypeProcessor.


# TODO

- Handle file data access with caching
- Ensure we get mimeType and md5 for filesystem client
- Ensure processors work with their cache keys
- Figure out how to rescan sources without doing full scan (e.g. what about moved, deleted, added)
- Default ignore lists to reduce number of files