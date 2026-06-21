<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

- Keep changes focused and reviewable.
- Use Conventional Commit PR titles: `type(scope): summary`. Accepted types:
  `feat`, `fix`, `docs`, `test`, `ci`, `refactor`, `perf`, `chore`, `revert`,
  `style`, and `build`.
- PR descriptions must include `Summary` and `Validation`.
- Sign every commit with DCO: `git commit -s`.
- Do not hand-edit the root `CODEOWNERS` — it is generated. To change review
  routing, edit `.github/codeowners/areas.yaml` and regenerate; CI gates 100%
  coverage and `CODEOWNERS`↔`areas.yaml` drift. See
  `.github/codeowners/README.md` (use `who_owns.py` to check who reviews a path).
