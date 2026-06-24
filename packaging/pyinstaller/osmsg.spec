# PyInstaller spec for a one-file osmsg.exe. The native deps (osmium, duckdb, pyarrow, shapely)
# ship compiled extensions + data files, so collect each fully rather than rely on import analysis.
from PyInstaller.utils.hooks import collect_all

datas, binaries, hiddenimports = [], [], []
for pkg in ("osmium", "duckdb", "pyarrow", "shapely", "typer", "typer_config", "rich", "pydantic"):
    d, b, h = collect_all(pkg)
    datas += d
    binaries += b
    hiddenimports += h

a = Analysis(
    ["entry.py"],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=["tkinter", "matplotlib", "pytest"],
    noarchive=False,
)
pyz = PYZ(a.pure)
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="osmsg",
    console=True,
    onefile=True,
    upx=False,
)
