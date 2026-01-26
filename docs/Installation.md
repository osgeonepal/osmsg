## Installation

Follow [windows_installation](./docs/Install_windows.md) for Windows Users .

- Install [osmium](https://github.com/osmcode/pyosmium) lib on your machine

```
pip install osmium
```

- Install osmsg

```
pip install osmsg
```

### Development Setup

**If you use Debian based Linux distros, make sure the following system libraries and tools are installed**

- build-essential - Provices gcc, g++ and make for compiling C/C++ code (<https://packages.ubuntu.com/jammy/build-essential>)
- clang - C/C++ compiler. Required by packages like 'pyproj' for building extensions on some systems.
- cmake - Needed for osmium (<https://github.com/osmcode/pyosmium>)
- libproj-dev - Needed for Geopandas (<https://github.com/geopandas/geopandas?tab=readme-ov-file>)
- proj-bin - Needed for Geopandas (<https://github.com/geopandas/geopandas?tab=readme-ov-file>)
- libgeos-dev - Needed for for Shapely and GeoPandas (Boost C++ libraries).
- libboost-dev - Needed for for osmium (Boost C++ libraries).
- libboost-system-dev - Needed for for osmium (Boost C++ libraries).
- libboost-filesystem-dev - Needed for for osmium (Boost C++ libraries).
- python3-dev - Provides Python headers for building Python extensions.

In the terminal, you can run

```bash
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    cmake \
    libproj-dev \
    proj-bin \
    libgeos-dev \
    libboost-dev \
    libboost-system-dev \
    libboost-filesystem-dev \
    python3-dev \
    clang \
```

**How to Install**

1. Fork the repo <https://github.com/kshitijrajsharma/OSMSG.git>

2. Then clone your fork:

```
git clone https://github.com/[Your github profile name]/[The repo name you choose].git
```

3. Install UV

We use UV because it is freaking fast and simplifies dependency management. UV streamlines the installation and synchronization of dependencies, making development smoother and more efficient.

Install UV by running:

```
curl -LsSf https://astral.sh/uv/install.sh | sh
```

4. Sync Project Dependencies

Once UV is installed, install the project dependencies directly into your virtual environment (.venv) with:

```
uv sync
```

This command reads project's configuration (i.e. pyproject.toml) and ensures that all required libraries are installed with the correct versions.
It will also create a virtual environment if one does not already exist.

5. Install Pre-commit Hooks

We use pre-commit hooks to ensure code quality and consistency across the project. Our pre-commit configuration includes:

- UV Lock: Ensures locking of dependency versions.
- Ruff Hooks (linter and formatter): Ruff is used for linting and formatting. It helps catch issues and enforces a consistent code style.
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

```
pre-commit install
```

This will automatically run the following on every commit:

- uv-lock: Validates your UV lock file.
- ruff: Checks code style and formatting.
- commitizen: Validates commit messages against the conventional commits specification.

When you make changes you can run

```
uv run pre-commit run --all-files
```

To check you pass all the precommit checks.

6. Getting Started

Once you have UV installed, dependencies synced, and pre-commit hooks set, you’re ready for development. A typical workflow might look like:

- Work on a feature or bug fix. Just tell other people what you will be working on in issues
- Run your tests – our project uses Pytest for testing.
- Commit your changes – pre-commit hooks ensure that your code meets our quality standards and that your commit messages follow the Conventional Commits guidelines.
  -Submit your PR - Create a branch with suitable name as per as your changes and raise PR

Bring up frontend

```
uv run --group ui streamlit run your_app_py
```

### [DOCKER] Install with Docker locally

- Clone repo & Build Local container :

  ```
  docker build -t osmsg:latest .
  ```

- Run Container terminal to run osmsg commands:

  ```
  docker run -it osmsg
  ```

  Attach your volume for stats generation if necessary

  ```
  docker run -it -v /home/user/data:/app/data osmsg
  ```
