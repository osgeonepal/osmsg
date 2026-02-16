# Installation guide

## Installation (with pip)

Follow [windows_installation](./docs/Install_windows.md) for Windows Users.

- Install [osmium](https://github.com/osmcode/pyosmium) lib on your machine:

```bash
pip install osmium
```

- Install osmsg

```bash
pip install osmsg
```

## [DOCKER] Install with Docker locally

- Clone repo & Build Local container :

```bash
docker build -t osmsg:latest .
```

- Run Container terminal to run osmsg commands:

```bash
docker run -it osmsg
```

Attach your volume for stats generation if necessary

```bash
docker run -it -v /home/user/data:/app/data osmsg
```

### Development Setup

#### How to Install

1. Install system dependencies.

```bash
sudo apt-get update
sudo apt-get install -y osmium-tool
```

1. Fork the repo <https://github.com/osgeonepal/OSMSG>

2. Then clone your fork:

```bash
git clone https://github.com/<your-username>/OSMSG.git
```

3. Install UV

We use UV because it is freaking fast and simplifies dependency management.
UV streamlines the installation and synchronization of dependencies,
making development smoother and more efficient.

Install UV by running:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

4. Sync Project Dependencies

Once UV is installed, install the project dependencies directly
into your virtual environment (.venv) with:

```bash
uv sync
```

This command reads project's configuration (i.e. pyproject.toml) and ensures
that all required libraries are installed with the correct versions.
It will also create a virtual environment if one does not already exist.

5. Install Pre-commit Hooks

We use pre-commit hooks to ensure code quality and consistency across the project.
Our pre-commit configuration includes:

- UV Lock: Ensures locking of dependency versions.
- Ruff Hooks (linter and formatter): Ruff is used for linting and formatting.
  It helps catch issues and enforces a consistent code style.
- Commitizen: Helps enforce conventional commit messages for better project history.

On Linux:

To set up these hooks, activate the virtual environment from the project's root directory

- On Linux and macOS run:

```bash
source .venv/bin/activate
```

On Windows you can run:

```PS
.venv\Scripts\activate
```

and then run:

```bash
pre-commit install
```

This will automatically run the following on every commit:

- uv-lock: Validates your UV lock file.
- ruff: Checks code style and formatting.
- commitizen: Validates commit messages against the conventional commits specification.

When you make changes you can run

```bash
uv run pre-commit run --all-files
```

or run it on a specifik file:

```bash
uv run pre-commit run --files [file path]
```

To check you pass all the precommit checks.

6. Getting Started

Once you have UV installed, dependencies synced, and pre-commit hooks set,
you’re ready for development. A typical workflow might look like:

- Work on a feature or bug fix. Just tell other people what you will be
  working on in issues
- Run your tests – our project uses Pytest for testing.
- Commit your changes – pre-commit hooks ensure that your code meets our
  quality standards and that your commit messages follow the Conventional
  Commits guidelines.
- Submit your PR - Create a branch with suitable name as per
  as your changes and raise PR

Bring up frontend

```bash
uv run --group ui streamlit run your_app_py
```
