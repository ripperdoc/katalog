# Rules

- Don't remove comments unless no longer relevant
- Make comments if some change is not obvious
- This is a `uv` based project, always use `uv` to install and run things. Use
  `UV_CACHE_DIR=.uv-cache` to avoid permissions issues.
- When logging and creating strings, prefer f-strings vs other ways
- Don't write type ignore comments unless confirming with user first
- Don't create or run on CLI temporary test code. If a test seems necessary, ask the user for
  permission to create a pytest unit test instead.

For details on the architecture, read DESIGN.md.
