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
        # Workflow files are deliberately not rewritten by the one-shot bot
        # because GitHub requires a separate workflows permission for bot pushes.
        if ".github" in path.parts and "workflows" in path.parts:
            continue
        if path.suffix in TEXT_EXTENSIONS or path.name in {"Makefile", "go.mod", "go.sum"}:
            yield path


def rewrite_internal_paths(text: str) -> str:
    return re.sub(
        r"github\.com/aqcool/socket\.io((?:/[A-Za-z0-9_.-]+)*)/v3",
        lambda m: "github.com/aqcool/socket.io" + m.group(1) + "/v4",
        text,
    )


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

shutil.rmtree(v4_dir)

preview = ROOT / "design" / "v4preview"
if preview.exists():
    shutil.rmtree(preview)

for path in list(iter_text_files()):
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        continue
    updated = rewrite_internal_paths(text)
    if updated != text:
        path.write_text(updated, encoding="utf-8")

root_mod = ROOT / "go.mod"
mod_text = root_mod.read_text(encoding="utf-8")
mod_text = re.sub(r"^module\s+.+$", "module github.com/aqcool/socket.io/v4", mod_text, count=1, flags=re.M)

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
    for module, local in replaces.items():
        if f"{module} =>" not in mod_text:
            mod_text = mod_text.replace("replace (", f"replace (\n\t{module} => {local}", 1)
root_mod.write_text(mod_text, encoding="utf-8")

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
    text = text.replace("github.com/aqcool/socket.io/v4 v3.0.4", "github.com/aqcool/socket.io/v4 v4.0.0")
    mod.write_text(text, encoding="utf-8")

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

for path in [ROOT / "pkg" / "version" / "version.go", ROOT / "servers" / "socket" / "version.go"]:
    if path.exists():
        text = path.read_text(encoding="utf-8")
        text = re.sub(r'v?3\.0\.4', 'v4.0.0-alpha.1', text)
        path.write_text(text, encoding="utf-8")

makefile = ROOT / "Makefile"
if makefile.exists():
    text = makefile.read_text(encoding="utf-8")
    text = text.replace("V3_MODULES :=", "MODULES :=")
    text = re.sub(r"(?m)^EXPERIMENTAL_MODULES\s*:=.*\n", "", text)
    text = re.sub(r"(?m)^MODULES\s*:=\s*\$\(V3_MODULES\).*\n", "", text)
    text = re.sub(r"(?m)^\s*v4\s*\\?\s*$", "", text)
    text = text.replace("$(V3_MODULES)", "$(MODULES)")
    makefile.write_text(text, encoding="utf-8")

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
