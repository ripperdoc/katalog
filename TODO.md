# TODO

# Next up

- [ ] As changesets can contain multiple providers, how do we map ScanResult.status to the changeset
      status? Also, currently the cutoff logic assumes there is always one scan per changeset.
- [ ] Are sources identified outside of the DB with their name (unique?) or their integer ID? What
      if we keep an ID the same but dramatically change the definition, e.g a different root folder.
- [ ] How to handle if two sources give the same canonical ID? they will overwrite eachother, is
      that ok?
- [ ] If a processor outputs assets (e.g. from archive), it would also output metadata that need to
      be linked to those assets. E.g. each metadata value in ProcessorResult need to be associated
      with an asset
- [ ] (Niklas) Should limited metadata types, e.g. file types, map to some enum table instead of
      saving the actual value over and over?

# Backlog

- [ ] Plugin system for **views**. A view is defined as a set of columns, computed columns (?) and
      the visualization e.g. web frontend component for them?
- [ ] Dynamic concurrency strategy for GDrive fetches - split the search space dynamically over time
      to always be able to work with concurrent fetchers.
- [ ] Store parent/child relationships from Google drive as relationship metadata. But how can we do
      that if we want the source scanner to not touch the DB?
- [ ] Speed up time to save to DB after scan, can we batch more writes?
- [ ] Use `react-jsonschema-form` to directly render a UI from Pydantic Config model for each
      plugin, instead taking in TOML
  - [ ] However, that removes the simplicity of commenting and commenting out parts of a config
- [ ] Add deletion of changesets for quick undo.
- [ ] Google Canonical URI to folders should be different than files
- [ ] How to find Google drive file's root folder? It's either a Shared Drive, "My Drive" or it's in
      Shared with me but not necessarily shown in the GDrive UI.
- [ ] Replace TortoiseORM with [SQLSpec](https://sqlspec.dev/usage/data_flow.html) for more
      efficient, decoupled database p usage
- [ ] Show detailed progress stats during scan/process, e.g. events and progress per file, but
      streamed to UI?
- [ ] Show in errors which provider, asset and/or metadata that was being processed
- [ ] If folders or shared drives are renamed the Google Drive cache will still use old names. We
      could detect name changes using our metadata system, and update the cache, but at that time we
      might still have some incorrect paths stored in metadata.
- [ ] UI to run analysis
- [ ] Icons for UI
- [ ] Use Google Drive Changes API to more efficiently (or is it?) fetch changes
- [ ] Path and filename Unicode normalization
- [ ] Read permissions for files in Google Shared Drives
- [ ] Handle remote file data access with caching
- [ ] Log excluded files just as assets, in order to still keep awareness in case we start to
      include them later?
- [ ] Search through some basic archive files e.g. zip files, and create virtual File Records
- [ ] Save more filesystem metadata, like extended attributes, if we can get to that

# Near term use cases

E.g. what do I need `katalog` to do now for White Wolf?

- [ ] Search the WW Google Drive and find duplicates and "badly named files" for manual fixing
- [ ] Search the WW Google Drive, find all PDF assets, and propose a new folder structure
- [ ] Search the WW Google Drive and propose new folder organization (how?)
- [ ] Search the WW Google and summarize stats for it
- [ ] Ask AI to review a set of files, e.g. "check if they have the right size" or "always shown
      from front"
- [ ] Quickly search and filter through files and metadata e.g. to find all PDFs and images to put
      in library
- [ ] Automatically move files to Shared Drive but keeping owners? (Seems too hard or risky to do
      with my code?)
- [ ] Find all art by Tim Bradstreet with a certain resolution

# Filesystem efficient scan

Use journalling system of file systems to only scan what has changed since last.

- [ ] **Windows (NTFS)**: Use **USN Change Journal**
  - Kernel-maintained log of file changes
  - Extremely fast, reliable
  - Journal can truncate → need fallback scan

- [ ] **macOS (APFS)**: Use **FSEvents**
  - Persistent directory-level change events
  - Efficient, survives reboots
  - Coarse-grained → may need verification

- [ ] **Linux**: Use **inotify / fanotify**
  - Real-time file change notifications
  - Low overhead
  - Not persistent → rescan after restart

# Upstream bugs

- SimpleTable: broken chevron on page footer if at the end page
- SimpleTable: Cannot copy and paste a cell into the filter field, because the cell is still
  considered selected.

# Ideas

## Source improvements

- Settings to filter incoming file records e.g. based on path or other criteria (filter before
  saving, or saving but with a flag that they are filtered?). Filtering will depend on each source
  how to best implement and what's supported.
- Settings to give default metadata to records from that source

## Processor ideas

- Archive processor - unpacks archives and contributes additional file records from it (required
  core API change?). Maybe the archive processor needs to be defined as a source instance? (might
  create lots of them)
- Read extended attributes from file systems
- Read sidecar files (may also require API changes as it connects two file records together)
- Guess metadata from path or URI (last resort?)
- TextExtractor: Extract text content (maybe not a single processor, but a processor per file type,
  which parses more than just text). Could be based on Pandoc.
- Summarizer, depends on text content, maybe also output language
  - We will need one metadata key per language
- Translator
  - Translate metadata fields to other language using AI
- Thumbnail extractor (find embedded or linked thumbnail, or create one from raw data)
- EXIF/IPTC
- OfficeDocs reader, including their specific metadata
- ID3Reader
- HTML: read metadata from common HTML markups, e.g. head element, JSON-LD, Schema.org, etc
- PDFMeta: read metadata from PDFs
- Warnings - create various warnings that can be surfaced about files, such as:
  - [ ] Empty folders
  - [ ] Empty files (may be more than just 0 bytes)
  - [ ] Filenames with space in end or beginning, including paths with same problem
  - [ ] Empty filenames
  - [ ] Disallowed characters in filenames
  - [ ] Variant, version, "Copy of" or " (1)" or similar automated copying names -> likely a variant
  - [ ] Corrupt files -> corrupt
  - [ ] Bad dates -> dates like 1970-01-01, 0000-…, future dates
  - [ ] Sidecar files
    - [ ] .DS_Store,
    - [ ] `~$marbetsavtal WWP och LD.docx` vs real `Samarbetsavtal WWP och LD.docx`, e.g temp Office
          files
    - [ ] Extended attribute sidecars from MacOS
          `._World of Darkness_ The Documentary - 85' VA_SD540p.mp4` vs
          `World of Darkness_ The Documentary - 85' VA_SD540p.mp4`
  - [ ] Name readability -> short names, many underscores, no spaces. Can be applied to most string
        based metadata.

## Analyzer ideas

- Topic extraction, e.g. Named Entity Recognition, see below for a topic or entity table
- Content similarity - group files that have similar content, either text or bit similarity
  (identical copies would be found via hash comparison)
  - If we have a group of similar files, we could also try to denote which one is top quality. Or,
    seen another way, all the other files are variants of that one.
- Metadata similarity - group files where selected metadata is similar, typically path, timing,
  size, title and author information
- AI similarity - let an LLM group files that are deemed similar, based on selected metadata
- Stats: produce a stats report, e.g. stats over some metadata, file type, file sizes, file counts,
  etc.
- Analyze potential compression, e.g. for a set of files with size, what could we expect to compress
  them to

## UI ideas

- Cell visualizations:
  - Show icon with popup if a cell has multiple values
  - Shorten hash to last 6, tooltip shows full
  - Paths show the beginning and end, but puts ellipsis in middle if column size is too short
  - Datetimes show up as human readable "since" but full date in tooltip (can be switched?)
  - Bytes are shown as human readable
  - MIME is shown as file icon and display name, full mime in tooltip
  - Source links to the source
  - ID links to full file display
  - Absolute URI could open on local drive? We might want to store as file:// but actually display
    it as a path depending on file system

## Core ideas

- A topic database to allow general topic modelling. May be covered by expanding file relationships?
-

### Generalize to any dataset (not just files)

This may be slightly a sidetrack for katalog.

When I want to put together a dataset, there are several major manual steps, even in the age of AI:

1. Copying the data from any random place. It can be a PDF, a web page and much more
2. Extracting it into a structured (often table) format that matches the format I am consolidating
   the data into
3. I may need to also reformat fields into different types
4. After adding the new records, I need to deduplicate
5. If there are conflicts, I need to make a decision about them
6. And whenever the source updates, I need to redo this all again, except now it’s even harder as
   most of the data will already be copied, therefore need to be deduplicated

Several of the steps are fuzzy, that cannot easily be converted in a regular manner. LLMs are now
pretty good at doing that, but we are hampered by the fact that the AI cannot cross all domains.
It’s not native in the spreadsheet, in the browser, and I need to copy and paste between various AI
tools.

Even so, the AI will do a poorer job if it’s not prompted correctly, not informed about the data
types and context, if it’s not able to produce output in the right format. It will also do worse on
large batch data, but it will be prohibitively slow and costly if applied on a per record basis.

Also, we cannot rely on fully automatic solutions, because there will always be exceptions and we
may want to make human edits. And by the way, we need many people to do that.

From a solution standpoint, the tricky part is that probably the solution has to adapt to whatever
database tool a team is using. We can’t make a better Airtable or Google Sheets, we need to let
users use that tool, but remove the work of getting data into it.

A general architecture could be:

- For each general source, define adapters that can extract data (via API, via scraping, OCR, etc).
  These may require credentials or plugins and may break if the data source is not willing to share
  (e.g. scraping). There are already many best practices here, we want to re-use as much as that as
  possible.
- Assuming we can now read a source, we now need to create a recipe for input structures and output
  structures, e.g. “that random user’s spreadsheet with these 32 columns”. This includes a
  structured schema and rule set, and and several AI prompts, including prompts for converting
  individual records. The prompt can both read from, and write to, the rule set, as it receives
  additional information or the source changes. A knowledgeable user can also edit this rule set.
- At this point, in theory, we can automatically retrieve all the data we care about, in the format
  we care about, and it brings us to the next phase of data cleaning and manipulation. Here we have
  to face the fact that most users will have their favourite tool already, and it may not be
  possible or desirable to provide another UI that lets users do this. It would simplify the
  solution a lot if we can indeed “outsource” the rest of the features to that tool.

A UX approach to it is also - instead of having to tell upfront to the AI what you want, you should
do it in steps together: add the source, answer relevant questions from the AI, ingest, answer more
relevant questions.

Internals:

- One approach would be to fetch a record from each source, and then form record pairs, where the
  same “real” entity is described by multiple records. There could then be a transformation rule
  applied to each record to form a joint record. That includes selecting fields, transforming
  fields. Manual edits could be seen as additional records. This all depends on the ability to
  either uniquely tie the records together, usually by saying “sourceA ID and sourceB ID are
  actually referring to the same thing”.

Other minor tools required:

- Date conversions
- Trimming
- Fetching data from other sources, e.g. I have a list of ISBN, I want to use the ISBN to fetch the
  image from Amazon and the description from Google Books

Look into AutoMerge, as they have thought a lot about this problem:
https://www.inkandswitch.com/cambria/
