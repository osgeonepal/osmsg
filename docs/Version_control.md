# Version Control

## Conventional Commits

A [specification](https://www.conventionalcommits.org/en/v1.0.0) for adding human and machine
readable meaning to commit messages.

**Format**: <type>[optional scope]: <description>

Example `feat: allow provided config object to extend other configs`
Example `fix: fixed the bug in issue #123`

**Advantage**: Automated SemVer version management (major.minor.patch), and automated changelogs.

## Commitizen CLI

[Commitizen](https://commitizen-tools.github.io/commitizen) is a Python tool to help with creating
**conventional commits** and automating version control.

### Install

Commitizen is a dev dependency of the project, so `just setup` (or `uv sync`) installs it. Run it with
`uv run cz`.

### Committing code

- Commit with `uv run cz commit` and follow the prompts.
- You can select the type of commit, plus additional metadata.

### Bumping a Version

When you decide it is time to create a new version:

1. Create a new branch:

   ```bash
   git checkout -b bump/new_release
   ```

2. Bump the version and push:

   ```bash
   uv run cz bump
   git push
   ```

This will:

- Update the SemVer version number in locations specific in `pyproject.toml`, throughout the codebase.
  - If a `feat` commit is included, the version is bumped by a minor increment (0.x.0); if only `fix`
    is included a patch will be used (0.0.x).
- Automatically update CHANGELOG.md with all changes since the last version.
- Create a tag matching the version number.

> Note: in a repo where you have direct push access, you would update on main and push.
> This repo follows Git-Flow, so the bump goes through a PR.

## Creating Releases

1. Update the version throughout the code ([Bumping a Version](#bumping-a-version)).
2. Click `Draft a new release`.
3. Click `Choose a tag`, then input the current version number and press enter (this will
   automatically create a matching tag for your release).
4. Set the `Release title` to v`x.x.x`, replacing with your version number.
5. Add a description if possible, then release.

This should trigger the PyPI publishing workflow, and your version will be available on [PyPI](https://pypi.org/project/osmsg/).
