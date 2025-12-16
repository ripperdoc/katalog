# TODO

# Next up

- [ ] Check asset identifiers, should we use prefix or an opaque internal ID
- [ ] Fix slow saving of asset metadata to db
- [ ] Stream progress events to HTTP caller or TQDM by reusing logger
- [ ] Store parent/child relationships from Google drive into relationships table
- [ ] If a processor outputs assets (e.g. from archive), it would also output metadata that need to
      be linked to those assets. E.g. each metadata value in ProcessorResult need to be associated
      with an asset

# Backlog

- [ ] UI to run scans and show progress
- [ ] UI to run analysis
- [ ] Read permissions for files in Google Shared Drives
- [ ] Handle remote file data access with caching
- [ ] Default ignore lists to reduce number of files. Start with an include list. Still log all
      excluded files, for easy discovery.
- [ ] Search through some basic archive files e.g. zip files, and create virtual File Records
- [ ] Save more file metadata, like extended attributes, if we can get to that

# Near term use cases

E.g. what do I need `katalog` to do now for White Wolf?

- [ ] Search the WW Google Drive and find duplicates and "badly named files" for manual fixing
- [ ] Search the WW Google Drive and propose new folder organization (how?)
- [ ] Search the WW Google and summarize stats for it
- [ ] Quickly search and filter through files and metadata e.g. to find all PDFs and images to put
      in library
- [ ] Automatically move files to Shared Drive but keeping owners? (Seems too hard or risky to do
      with my code?)
