#!/usr/bin/env python3
from __future__ import annotations

import re
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MARKER = ROOT / ".native-v4-migrated"

if MARKER.exists():
    raise SystemExit(0)

TEXT_EXTENSIONS = {
    ".go", ".mod", ".sum", ".md", ".yml", ".yaml", ".json", ".toml", ".txt", ".bat", ".sh"
}


def iter_text_files():
    ignored = {".git", "vendor", "node_modules"}
    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue
        if any(part in ignored for part in path.parts):
            continue
        if path.suffix in TEXT_EXTENSIONS or path.name in {"Makefile", "go.mod", "go.sum"}:
            yield path


def rewrite_internal_paths(text: str) -> str:
    # Major-version suffixes are module-local. Rewrite only this repository's
    # module paths; do not touch third-party /v3 dependencies such as resty.dev/v3.
    return re.sub(
        r"github\.com/aqcool/socket\.io((?:/[A-Za-z0-9_.-]+)*)/v3",
        lambda m: "github.com/aqcool/socket.io" + m.group(1) + "/v4",
        text,
    )


# 1. Promote the application-facing v4 package to the repository root.
v4_dir = ROOT / "v4"
if not v4_dir.is_dir():
    raise SystemExit("v4 directory not found")

for source in sorted(v4_dir.glob("*.go")):
    target = ROOT / source.name
    if target.exists():
        raise SystemExit(f"refusing to overwrite existing root file: {target}")
    shutil.move(str(source), str(target))

runtime_doc = ROOT / "docs" / "V4_RUNTIME.md"
runtime_doc.parent.mkdir(parents=True, exist_ok=True)
if (v4_dir / "README.md").exists():
    shutil.move(str(v4_dir / "README.md"), str(runtime_doc))

# The root go.mod is the canonical v4 module from now on; the old facade module
# is intentionally removed.
shutil.rmtree(v4_dir)

# Compile-check preview is superseded by the real root v4 package.
preview = ROOT / "design" / "v4preview"
if preview.exists():
    shutil.rmtree(preview)

# 2. Rewrite all repository-owned /v3 module/import paths to /v4.
for path in list(iter_text_files()):
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        continue
    updated = rewrite_internal_paths(text)
    if updated != text:
        path.write_text(updated, encoding="utf-8")

# 3. Root module becomes the canonical github.com/aqcool/socket.io/v4 module.
root_mod = ROOT / "go.mod"
mod_text = root_mod.read_text(encoding="utf-8")
mod_text = re.sub(r"^module\s+.+$", "module github.com/aqcool/socket.io/v4", mod_text, count=1, flags=re.M)

# The root API uses the native Socket.IO core module. Keep local replaces for
# monorepo development; release tooling later drops/updates these as needed.
if "github.com/aqcool/socket.io/servers/socket/v4" not in mod_text:
    mod_text += "\nrequire github.com/aqcool/socket.io/servers/socket/v4 v4.0.0\n"

replaces = {
    "github.com/aqcool/socket.io/parsers/engine/v4": "./parsers/engine",
    "github.com/aqcool/socket.io/parsers/socket/v4": "./parsers/socket",
    "github.com/aqcool/socket.io/servers/engine/v4": "./servers/engine",
    "github.com/aqcool/socket.io/servers/socket/v4": "./servers/socket",
}
if "replace (" not in mod_text:
    mod_text += "\nreplace (\n"
    for module, local in replaces.items():
        mod_text += f"\t{module} => {local}\n"
    mod_text += ")\n"
else:
    # Root module did not previously have local replaces, but handle reruns safely.
    for module, local in replaces.items():
        if f"{module} =>" not in mod_text:
            mod_text = mod_text.replace("replace (", f"replace (\n\t{module} => {local}", 1)
root_mod.write_text(mod_text, encoding="utf-8")

# 4. Internal monorepo requires must use a v4 semantic version after path bump.
for mod in ROOT.rglob("go.mod"):
    if mod == root_mod:
        continue
    text = mod.read_text(encoding="utf-8")
    text = rewrite_internal_paths(text)
    text = re.sub(
        r"(?m)^(\s*github\.com/aqcool/socket\.io[^\s]*?/v4)\s+v3\.[^\s]+",
        r"\1 v4.0.0",
        text,
    )
    # Root-module dependencies now point at the canonical root v4 module.
    text = text.replace("github.com/aqcool/socket.io/v4 v3.0.4", "github.com/aqcool/socket.io/v4 v4.0.0")
    mod.write_text(text, encoding="utf-8")

# 5. The promoted public package is no longer a legacy bridge. Rename the
# implementation alias and terminology while retaining the proven protocol core.
for name in ["server.go", "namespace.go", "socket.go", "operator.go"]:
    path = ROOT / name
    if not path.exists():
        continue
    text = path.read_text(encoding="utf-8")
    text = text.replace('legacy "github.com/aqcool/socket.io/servers/socket/v4"', 'core "github.com/aqcool/socket.io/servers/socket/v4"')
    text = re.sub(r"\blegacy\.", "core.", text)
    text = re.sub(r"\blegacy([A-Z])", r"core\1", text)
    text = text.replace("legacy bridge", "native core boundary")
    text = text.replace("bridge backend", "native core backend")
    text = text.replace("proven v3 protocol core", "native v4 protocol core")
    text = text.replace("reusing the proven v3 protocol core", "using the native v4 protocol core")
    path.write_text(text, encoding="utf-8")

# 6. Version metadata: this branch is the first native v4 development line.
for path in [ROOT / "pkg" / "version" / "version.go", ROOT / "servers" / "socket" / "version.go"]:
    if path.exists():
        text = path.read_text(encoding="utf-8")
        text = re.sub(r'v?3\.0\.4', 'v4.0.0-alpha.1', text)
        path.write_text(text, encoding="utf-8")

# 7. Makefile: there is now one active major-version line. Remove the temporary
# facade module from the module matrix and v3/v4 split introduced during alpha.
makefile = ROOT / "Makefile"
if makefile.exists():
    text = makefile.read_text(encoding="utf-8")
    text = text.replace("V3_MODULES :=", "MODULES :=")
    text = re.sub(r"(?m)^EXPERIMENTAL_MODULES\s*:=.*\n", "", text)
    text = re.sub(r"(?m)^MODULES\s*:=\s*\$\(V3_MODULES\).*\n", "", text)
    text = re.sub(r"(?m)^\s*v4\s*\\?\s*$", "", text)
    text = text.replace("$(V3_MODULES)", "$(MODULES)")
    makefile.write_text(text, encoding="utf-8")

# 8. CI: v4 is the root package now, so remove the obsolete v4 submodule lint
# entry and point the dedicated race job at the root module.
workflow = ROOT / ".github" / "workflows" / "go.yml"
if workflow.exists():
    text = workflow.read_text(encoding="utf-8")
    text = re.sub(r'(?m)^\s*- "v4"\s*\n', "", text)
    text = text.replace('make test MODULE="v4"', 'make test MODULE="."')
    text = text.replace('working-directory: v4', 'working-directory: .')
    workflow.write_text(text, encoding="utf-8")

# 9. Keep design docs, but remove wording that describes the root API as a
# temporary bridge and update target module path where necessary.
for path in [ROOT / "docs" / "V4_API_DESIGN.md", ROOT / "docs" / "V4_IMPLEMENTATION_PLAN.md", ROOT / "docs" / "V4_MIGRATION.md", runtime_doc]:
    if path.exists():
        text = path.read_text(encoding="utf-8")
        text = rewrite_internal_paths(text)
        text = text.replace("bridge phase", "native implementation")
        text = text.replace("removal of the v3 protocol-core dependency", "v3 protocol-core dependency removed")
        path.write_text(text, encoding="utf-8")

MARKER.write_text(
    "Native v4 module/path migration completed. Do not re-run the one-shot migration.\n",
    encoding="utf-8",
)
