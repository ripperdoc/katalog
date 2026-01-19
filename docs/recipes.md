# Recipes

This is an internal design note trying to describe how different specific actions and use cases can
be implemented using current abstractions.

## Find duplicate files

1. Query assets with group by hash.
2. Select all duplicates of one specific file as collection
3. Manually mark (edit metadata) for deletion
   1. or run an analyzer on the collection that picks the best to keep?
4. Apply changes to the source

Comments:

- Can we re-use snapshot as a proposed change set? It's been saved to DB, but if it can be shown in
  a good "diff" view user can decide whether to apply the snapshot to a specific source
- We need a way to describe commands to a source from a snapshot, and a common API for SourcePlugins

## Organize a set of files to have better folder structure

1. Make a query and save as collection
2. Use a Processor OR Analyzer to check the path on each asset
3. Create a snapshot with the proposed edits
4. (at a later time) apply the snapshot to source(s) to acutally move the files

Comments:

- Processor is much simpler but can only make decisions in isolation
- An analyzer could look at files together, e.g. LLM, to move them to a better structure

## Convert a set of images to something optimized

1. Make a query and save as collection
2. Run a processor on each
   1. It reads the file data accessor OR path and runs e.g. imagemagick
      1. That is saved to filesystem as a side-effect (e.g. regardless what source the asset is
         from)
      2. Or, that is saved, associated with a snapshot, and can be applied to the source as a
         separate step?

## Make manual edits to some asset's metadata

1. Go to an asset details page (or possibly from the table)
2. Make an edit using built-in UI
   1. This visibly and under the hood starts an ongoing snapshot. The snapshot is tagged to a
      special provider representing the current user, or a built-in plugin?
   2. Every edit is saved to the DB as it happens, using the ongoing snapshot
   3. User can move on to another asset and edit it too
3. User needs click a button in the top bar to complete the snapshot (but technically nothing really
   bad happens if it's not closed, or we can just formally block new snapshots if the last is open).

## Delete garbage files

1. Get a collection of garbage files found with query
   1. Or, processors running on all files sets a flag, which is made into collection
2. Processor runs on collection, flags each as "to delete"
3. Save snapshot back to source, e.g. removing these files

Comments:

- Most things can be handled as metadata, but "delete file" cannot currently be represented as
  metadata.
- To find garbage files via query we need fairly advanced querying, e.g. regex on file name. The
  processor route will always be more flexible.

## Display stats for a collection (e.g. top file types, number of files, etc)

1. Start with a query or collection
2. Run an analyzer to collect stats on it
3. Save the output as JSON into the snapshot
4. Render it using the frontend code included in the analyzer

## Display file sizes as tree map

Either this is another "view", and we can install the view. Or it's an analyzer, that may do very
minimal work, and save that stat to a snapshot, and then render it using a custom component from the
analyzer.

## Export a collection as CSV

1. Start with a query or collection
2. Run an analyzer
3. Save the CSV as a side-effect (local file) or in the snapshot payload

## Show some image files as a gallery

1. Start with a query or collection
2. Select a view (offered by a plugin?)
3. Browse the same information as before, but as a gallery

## Find group of files that have similar names

1. Run an analyzer
2. Output the first collection of similarily named files? Or all collections?

Comment

- Seems tricky to represent well with current architecture, is it some kind of clustering?

## Make a query ignore "trash files"

Comment: It's typical to find trash files like `._`, `.DS_Store`, etc. How can we ignore such files
easily in most queries?
