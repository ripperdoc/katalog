# Agent protocol

- Always strive for small, iterative changes
- Always strive for readable and modular code, refactor if functions and files gets more than 500
  lines
- Only solve the problem at hand, but propose in writing other improvements that you saw. Keep
  proposals short and concise.
- Always respond in text before changing code if the user seems unsure, is asking a question or is
  suggesting something with large ramifications.
- Always ask the user if a requirement appears unclear.
- If solving a bug, explain root cause in addition to fixing the bug.
- Run tests using `pytest` CLI or try it in terminal to verify issues or check that code is working
- Don't write tests unless explicitly asked for it
- Avoid code bloat and complexity by following these rules of thumb:
  - Better to modify an existing API / method / property than add a similar but different one. Ask
    if the API change would be significant.
  - If a change becomes complex because of work arounds to still support existing APIs, stop and
    check if there is a better way
  - Don't assume backward compatibility, ask if it's necessary
- Lookup docs for libraries I reference with web search
- Read the README.md, DESIGN.md and TODO.md files.
- Propose changes to README and DESIGN if current code seems to contradict them.

# Code in this repo

- Don't remove comments or commented out code unless explicitly told to
- Write comments if some change you are doing is not obvious
- Prefer f-strings vs other ways of string formatting
- Don't write `type ignore` comments unless confirming with user first
- Don't create temporary test code. If a test seems necessary, ask the user for permission to create
  a pytest unit test instead.
- Avoid using `getattr` and `setattr`, it is bug prone and messes up typing. If there is no other
  way, hide it behind an instance method where possible.
- Avoid shell scripting if possible - solve it in main code, write a standalone Python script or
  make as simple bash scripts as possible where other commands do the heavy lifting
- Avoid complicated CI-actions and scripting, rely on existing tools or write tools in e.g Python
  that CI can call

# Tools in this repo

- This is a `uv` based project, always use `uv` to install and run things. Use
- `loguru` for logging
- `pytest` for tests
