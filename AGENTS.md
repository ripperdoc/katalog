# Rules

- Always respond in text before changing code if the user seem to not be sure or asking something.
- Always ask the user if a requirement is unclear.
- Strive for minimal, iterative changes instead of sweeping
- Don't remove comments unless no longer relevant
- Make comments if some change is not obvious
- This is a `uv` based project, always use `uv` to install and run things. Use
  `UV_CACHE_DIR=.uv-cache` to avoid permissions issues.
- When logging and creating strings, prefer f-strings vs other ways
- Don't write type ignore comments unless confirming with user first
- Don't create or run on CLI temporary test code. If a test seems necessary, ask the user for
  permission to create a pytest unit test instead.
- Avoid using `getattr` and `setattr`, it is bug prone and messes up typing. If there is no other
  way, hide it behind an instance method where possible.
- Always code bloat and spreading complexity by following these rules of thumb:
  - Better to modify an existing API / method / property than add a similar but different one. Ask
    if the API change would be significant.
  - If a change becomes complex because of work arounds to still support existing APIs, stop and
    check if there is a better over
  - Ask before adding extra backward compatibility layers - compatibility may not be needed

For details on the architecture, read DESIGN.md.
