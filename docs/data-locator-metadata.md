Katalog is meant to store metadata about digital assets - usually files but also possibly more loose
concepts such as a post, a product, etc.

That poses the question what to do with binary data - for a file, that is the main file content, but
it could also be things like thumbnails, previews, etc. In theory, not much is stopping us from
simply supporting a BLOG metadata type and put it all in the DB. And I don't want to rule that out,
but in general, that will consume too much data. With up to 1M assets, it's infeasible to keep
binary data inside a sqlite database.

Instead, I want a way to store binary and url resolvers as JSON metadata. The binary resolver
contains the information to, at runtime, read binary data through the source plugin (e.g. the actor
that created the metadata). URL resolvers returns a URL at runtime that can be accessed from
localhost, e.g. either a public, authenticated URL or a file that is cached locally and served by
the server.

Plugins can create any number of binary or URL resolvers - but the normal case is just one binary
resolver for file data and one URL resolver.

The actor that created the metadata resolver is responsible to handle it, so there should be a
helper method that takes a metadata row, finds the plugin and uses the resolver to return a binary
reader handle or URL handle.

The data reader should support reading a limited byte range in order to get file headers, but the
common case would be to read the full file to perform some processing.

Within this, we need a caching layer. It should be possible to never have to redownload a specific
binary blob again. The source plugins should normally just use a utility caching layer that stores
binary blobs in the workspace file system, using md5 or sha256 content hash, and keeps a maximum
cache size and throwing out the oldest first. We can use an existing utility in Python ecosystem for
this.

URLs that are generated/resolved usually have a time on them, but it's up to the source plugin to
cache this for some time or not.
