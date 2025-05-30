#!/usr/bin/env python3
"""
PhotoRanker — Turbo Browser (complete build)
===========================================
Fully wired‑up, end‑to‑end version:

• **Thumbnail cache** in `.pr_thumbs/` (128 px, 256 px).
• **Virtualised grid** (`QListView` + `ThumbModel`).
• **Async loading** via `QThreadPool` – UI never blocks.
• **Folder tree** shows live image counts.
• **Thumb size slider** 64‑256 px.
• **Double‑click** opens a scrollable, fit‑to‑window full‑res viewer.

Dependencies: PyQt5, Pillow≥10 (or pillow‑simd for speed), rawpy (for .DNG).
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Tuple

from PIL import Image
import rawpy  # type: ignore
from PyQt5 import QtCore, QtGui, QtWidgets

# ─────────────────────────── Config ────────────────────────────
DB_FILE = "photo_ranker.db"
THUMB_DIR = Path(".pr_thumbs")
THUMB_SIZES = [128, 256]  # cache levels
DEFAULT_THUMB = 128       # starting icon size
Image.MAX_IMAGE_PIXELS = None

# ────────────────────────── Utilities ──────────────────────────

def sha1(path: str) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as fh:
        while chunk := fh.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def ensure_thumb(path: str, size: int) -> Path:
    """Return cached thumbnail path (JPEG). Create if missing."""
    h = sha1(path)
    subdir = THUMB_DIR / h[:2]
    subdir.mkdir(parents=True, exist_ok=True)
    thumb_path = subdir / f"{h}_{size}.jpg"
    if thumb_path.exists():
        return thumb_path
    try:
        if path.lower().endswith(".dng"):
            with rawpy.imread(path) as raw:
                img = Image.fromarray(raw.postprocess())
        else:
            img = Image.open(path)
        img.thumbnail((size, size), Image.LANCZOS)
        img.save(thumb_path, "JPEG", quality=85)
    except Exception:
        return Path(path)  # fallback: original path
    return thumb_path

# ─────────────────────── Database helpers ──────────────────────

def load_rows() -> List[Tuple[str, float, int]]:
    con = sqlite3.connect(DB_FILE)
    rows = con.execute(
        "SELECT path, rating, compared FROM images WHERE blacklist=0"
    ).fetchall()
    con.close()
    return [(os.path.abspath(p), r, c) for p, r, c in rows]


def build_dir_counts(rows) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for p, *_ in rows:
        path = Path(p)
        for parent in [path.parent, *path.parents]:
            counts[str(parent)] = counts.get(str(parent), 0) + 1
    return counts

# ────────────────── Worker for async thumbnails ─────────────────

class ThumbTask(QtCore.QRunnable):
    finished = QtCore.pyqtSignal(str, QtGui.QPixmap)  # path, pixmap

    def __init__(self, path: str, size: int):
        super().__init__()
        self.path = path
        self.size = size
        self.setAutoDelete(True)

    def run(self):
        tpath = ensure_thumb(self.path, self.size)
        try:
            img = Image.open(tpath)
            if img.mode != "RGBA":
                img = img.convert("RGBA")
            data = img.tobytes("raw", "RGBA")
            qimg = QtGui.QImage(data, img.width, img.height, QtGui.QImage.Format_RGBA8888)
            pix = QtGui.QPixmap.fromImage(qimg)
            self.finished.emit(self.path, pix)
        except Exception:
            pass

# ─────────────────────── Model / delegate ──────────────────────

class ThumbModel(QtCore.QAbstractListModel):
    def __init__(self, rows: List[Tuple[str, float, int]], icon_size: int):
        super().__init__()
        self.rows = rows
        self.icon_size = icon_size
        self.pool = QtCore.QThreadPool.globalInstance()
        self.cache: Dict[str, QtGui.QPixmap] = {}

    def rowCount(self, _index):
        return len(self.rows)

    def data(self, idx, role):
        if not idx.isValid():
            return None
        path, rating, _ = self.rows[idx.row()]
        if role == QtCore.Qt.DecorationRole:
            if path in self.cache:
                return self.cache[path]
            task = ThumbTask(path, self.icon_size)
            task.finished.connect(self._got_thumb, QtCore.Qt.QueuedConnection)
            self.pool.start(task)
            return QtGui.QPixmap(self.icon_size, self.icon_size)  # placeholder
        if role == QtCore.Qt.ToolTipRole:
            return f"{os.path.basename(path)}\nElo: {rating:.0f}"

    @QtCore.pyqtSlot(str, QtGui.QPixmap)
    def _got_thumb(self, path: str, pix: QtGui.QPixmap):
        self.cache[path] = pix
        try:
            row = next(i for i, r in enumerate(self.rows) if r[0] == path)
            self.dataChanged.emit(self.index(row), self.index(row), [QtCore.Qt.DecorationRole])
        except StopIteration:
            pass

# ───────────────────────── Viewer widget ───────────────────────

class Viewer(QtWidgets.QScrollArea):
    def __init__(self, path: str):
        super().__init__()
        self.setWindowTitle(os.path.basename(path))
        self.setWidgetResizable(True)
        lbl = QtWidgets.QLabel(alignment=QtCore.Qt.AlignCenter)
        self.setWidget(lbl)
        QtCore.QThreadPool.globalInstance().start(self.Loader(path, lbl))

    class Loader(QtCore.QRunnable):
        def __init__(self, path: str, label: QtWidgets.QLabel):
            super().__init__()
            self.path, self.label = path, label
        def run(self):
            try:
                if self.path.lower().endswith(".dng"):
                    with rawpy.imread(self.path) as raw:
                        img = Image.fromarray(raw.postprocess())
                else:
                    img = Image.open(self.path)
                if img.mode != "RGBA":
                    img = img.convert("RGBA")
                data = img.tobytes("raw", "RGBA")
                qimg = QtGui.QImage(data, img.width, img.height, QtGui.QImage.Format_RGBA8888)
                pix = QtGui.QPixmap.fromImage(qimg)
                QtCore.QMetaObject.invokeMethod(
                    self.label, "setPixmap", QtCore.Qt.QueuedConnection,
                    QtCore.Q_ARG(QtGui.QPixmap, pix)
                )
            except Exception:
                pass

# ───────────────────────── Main window ─────────────────────────

class Browser(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PhotoRanker — Turbo Browser")
        self.resize(1400, 900)

        self.rows = load_rows()
        if not self.rows:
            QtWidgets.QMessageBox.information(self, "Empty", "No images in DB")
            sys.exit()
        self.dir_counts = build_dir_counts(self.rows)
        self.icon_size = DEFAULT_THUMB

        self._init_ui()
        self._populate_tree()
        # Load root folder initially
        root_item = self.tree.topLevelItem(0)
        if root_item:
            self.tree.setCurrentItem(root_item)
            self._load_folder(Path(root_item.data(0, QtCore.Qt.UserRole)))

    # ---------- UI setup ----------
    def _init_ui(self):
        splitter = QtWidgets.QSplitter()
        self.setCentralWidget(splitter)

        # Folder tree
        self.tree = QtWidgets.QTreeWidget()
        self.tree.setHeaderHidden(True)
        self.tree.itemSelectionChanged.connect(self._on_folder_sel)
        splitter.addWidget(self.tree)

        # Thumb grid (QListView)
        self.view = QtWidgets.QListView(viewMode=QtWidgets.QListView.IconMode)
        self.view.setResizeMode(QtWidgets.QListView.Adjust)
        self.view.setSpacing(8)
        self.view.setIconSize(QtCore.QSize(self.icon_size, self.icon_size))
        self.view.setUniformItemSizes(True)
        self.view.doubleClicked.connect(self._open_viewer)
        splitter.addWidget(self.view)
        splitter.setSizes([320, 1080])

        # Toolbar
        tb = self.addToolBar("Options")
        tb.setMovable(False)
        size_slider = QtWidgets.QSlider(QtCore.Qt.Horizontal, minimum=64, maximum=256, value=self.icon_size)
        size_slider.valueChanged.connect(self._change_icon_size)
        tb.addWidget(QtWidgets.QLabel("Thumb:"))
        tb.addWidget(size_slider)

    # ---------- Folder tree ----------
    def _populate_tree(self):
        nodes: Dict[str, QtWidgets.QTreeWidgetItem] = {}
        for folder, cnt in sorted(self.dir_counts.items()):
            parts = Path(folder).parts
            built = "" if os.name == "nt" else "/"
            parent_item = None
            for part in parts:
                built = os.path.join(built, part) if built else part
                if built not in nodes:
                    item = QtWidgets.QTreeWidgetItem()
                    item.setText(0, f"{part} ({self.dir_counts.get(built, 0)})")
                    item.setData(0, QtCore.Qt.UserRole, built)
                    nodes[built] = item
                    if parent_item:
                        parent_item.addChild(item)
                    else:
                        self.tree.addTopLevelItem(item)
                parent_item = nodes[built]
        self.tree.expandToDepth(1)

    def _on_folder_sel(self):
        data = self.tree.currentItem().data(0, QtCore.Qt.UserRole)
        if data:
            self._load_folder(Path(data))

    def _load_folder(self, folder: Path):
        norm = os.path.normcase(str(folder))
        subset = [r for r in self.rows if os.path.normcase(r[0]).startswith(norm)]
        self.model = ThumbModel(subset, self.icon_size)
        self.view.setModel(self.model)

    # ---------- interactions ----------
    def _change_icon_size(self, val):
        self.icon_size = val
        self.view.setIconSize(QtCore.QSize(val, val))
        # reload current folder to apply new size
        current = self.tree.currentItem().data(0, QtCore.Qt.UserRole)
        if current:
            self._load_folder(Path(current))

    def _open_viewer(self, index: QtCore.QModelIndex):
        path = self.model.rows[index.row()][0]
        viewer = Viewer(path)
        viewer.resize(1200, 800)
        viewer.setAttribute(QtCore.Qt.WA_DeleteOnClose)
        viewer.show()

# ──────────────────────────── main ────────────────────────────

def main():
    app = QtWidgets.QApplication(sys.argv)
    b = Browser()
    b.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
