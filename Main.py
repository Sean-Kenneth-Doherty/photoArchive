#!/usr/bin/env python3
"""
PhotoRanker — Minimal v5.1
==========================
Small PyQt5 photo‑ranking tool with **SQLite‑only persistence**.
This patch (v5.1) adds **automatic schema migration** so older
`photo_ranker.db` files—from versions that lacked the `rank` column—work
without errors.

Patch highlights
----------------
* `_init_schema()` now checks `PRAGMA table_info(images)` and `ALTER TABLE …
  ADD COLUMN rank INTEGER` if missing.

Keyboard
--------
← better → worse Z blacklist‑left X blacklist‑right Esc quit
"""

from __future__ import annotations

import hashlib
import os
import random
import sqlite3
import sys
from typing import List, Optional, Tuple

from PIL import Image
import rawpy  # type: ignore
from PyQt5 import QtCore, QtGui, QtWidgets

DB_FILE = "photo_ranker.db"
K_FACTOR = 32  # Elo constant

# ----------------------------------------------------------------------------
# Helpers --------------------------------------------------------------------

def file_hash(path: str, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while blk := fh.read(chunk):
            h.update(blk)
    return h.hexdigest()


def open_image(path: str) -> Image.Image:
    if path.lower().endswith(".dng"):
        with rawpy.imread(path) as raw:
            return Image.fromarray(raw.postprocess())
    return Image.open(path)


def pil_to_qimage(img: Image.Image) -> QtGui.QImage:
    if img.mode != "RGBA":
        img = img.convert("RGBA")
    w, h = img.size
    return QtGui.QImage(img.tobytes("raw", "RGBA"), w, h, QtGui.QImage.Format_RGBA8888)


class ResponsiveLabel(QtWidgets.QLabel):
    def __init__(self):
        super().__init__()
        self._pix: Optional[QtGui.QPixmap] = None
        self.setAlignment(QtCore.Qt.AlignCenter)
        self.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Expanding)

    def setPixmap(self, pix: QtGui.QPixmap):  # type: ignore[override]
        self._pix = pix
        self._update()

    def resizeEvent(self, _ev):
        self._update()

    def _update(self):
        if self._pix:
            super().setPixmap(
                self._pix.scaled(self.size(), QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation)
            )

# ----------------------------------------------------------------------------
# SQLite layer ---------------------------------------------------------------

class DB:
    def __init__(self, path: str):
        self.con = sqlite3.connect(path)
        self._init_schema()

    def _init_schema(self):
        """Create table if missing and add new columns when upgrading."""
        # initial table if never created
        self.con.execute(
            """CREATE TABLE IF NOT EXISTS images (
                   hash TEXT PRIMARY KEY,
                   path TEXT,
                   rating REAL DEFAULT 1200,
                   compared INTEGER DEFAULT 0,
                   blacklist INTEGER DEFAULT 0
               )"""
        )
        # migrate: add rank column if absent
        cols = [row[1] for row in self.con.execute("PRAGMA table_info(images)")]
        if "rank" not in cols:
            self.con.execute("ALTER TABLE images ADD COLUMN rank INTEGER")
        self.con.commit()

    # maintenance
    def touch(self, h: str, path: str):
        self.con.execute("INSERT OR IGNORE INTO images(hash, path) VALUES(?, ?)", (h, path))
        self.con.execute("UPDATE images SET path=? WHERE hash=?", (path, h))
        self.con.commit()

    # queries
    def unranked(self) -> List[str]:
        return [r[0] for r in self.con.execute("SELECT hash FROM images WHERE blacklist=0 AND rank IS NULL")]

    def ranked_order(self) -> List[str]:
        return [r[0] for r in self.con.execute("SELECT hash FROM images WHERE rank IS NOT NULL AND blacklist=0 ORDER BY rank ASC")]

    def path(self, h: str) -> str:
        row = self.con.execute("SELECT path FROM images WHERE hash=?", (h,)).fetchone()
        return row[0] if row else ""

    def rating(self, h: str) -> float:
        return self.con.execute("SELECT rating FROM images WHERE hash=?", (h,)).fetchone()[0]

    def rank(self, h: str) -> Optional[int]:
        row = self.con.execute("SELECT rank FROM images WHERE hash=?", (h,)).fetchone()
        return row[0] if row and row[0] is not None else None

    def update_elo(self, winner: str, loser: str):
        rw, rl = self.rating(winner), self.rating(loser)
        expected_w = 1 / (1 + 10 ** ((rl - rw) / 400))
        rw_new = rw + K_FACTOR * (1 - expected_w)
        rl_new = rl + K_FACTOR * (expected_w - 1)
        self.con.executemany(
            "UPDATE images SET rating=?, compared = compared+1 WHERE hash=?",
            [(rw_new, winner), (rl_new, loser)],
        )
        self.con.commit()

    def update_ranks(self, ordered: List[str]):
        self.con.executemany("UPDATE images SET rank=? WHERE hash=?", [(i + 1, h) for i, h in enumerate(ordered)])
        self.con.commit()

    def blacklist(self, h: str):
        self.con.execute("UPDATE images SET blacklist=1, rank=NULL WHERE hash=?", (h,))
        self.con.commit()

# ----------------------------------------------------------------------------
# Sorter ---------------------------------------------------------------------

class PairwiseSorter:
    def __init__(self, unranked: List[str], db: DB, seeded: List[str]):
        self.db = db
        self.pool = unranked[:]
        random.shuffle(self.pool)
        self.sorted = seeded[:]  # already‑ranked list
        self._cand: Optional[str] = None
        self.low = self.high = 0

    def next_pair(self) -> Tuple[Optional[str], Optional[str]]:
        if self._cand is None:
            while self.pool:
                self._cand = self.pool.pop()
                if self._cand not in self.sorted:
                    break
            else:
                return None, None
            if not self.sorted:
                self.sorted.append(self._cand)
                self._cand = None
                self.db.update_ranks(self.sorted)
                return self.next_pair()
            self.low, self.high = 0, len(self.sorted)
        mid = (self.low + self.high) // 2
        return self._cand, self.sorted[mid]

    def vote(self, cand_better: bool):
        if self._cand is None:
            return
        ref_idx = (self.low + self.high) // 2
        ref = self.sorted[ref_idx]
        winner, loser = (self._cand, ref) if cand_better else (ref, self._cand)
        self.db.update_elo(winner, loser)
        if cand_better:
            self.high = ref_idx
        else:
            self.low = ref_idx + 1
        if self.low >= self.high:
            self.sorted.insert(self.low, self._cand)
            self._cand = None
            self.db.update_ranks(self.sorted)

    def progress(self) -> Tuple[int, int]:
        done = len(self.sorted)
        return done, done + len(self.pool) + (1 if self._cand else 0)

    def add_hashes(self, hs: List[str]):
        for h in hs:
            if h not in self.pool and h not in self.sorted and h != self._cand:
                self.pool.append(h)
        random.shuffle(self.pool)

# ----------------------------------------------------------------------------
# Folder ingest --------------------------------------------------------------

def ingest_folder(folder: str, db: DB) -> List[str]:
    new_hashes: List[str] = []
    for root, _d, files in os.walk(folder):
        for f in files:
            if not f.lower().endswith((".jpg", ".jpeg", ".png", ".dng")):
                continue
            p = os.path.join(root, f)
            h = file_hash(p)
            db.touch(h, p)
            new_hashes.append(h)
    return new_hashes

# ----------------------------------------------------------------------------
# Main -----------------------------------------------------------------------

def main():
    app = QtWidgets.QApplication(sys.argv)
    db = DB(DB_FILE)

    ranked = db.ranked_order()
    unranked = db.unranked()

    if not (ranked or unranked):
        folder = QtWidgets.QFileDialog.getExistingDirectory(None, "Select image folder")
        if not folder:
            sys.exit()
        unranked = ingest_folder(folder, db)

    if not (ranked or unranked):
        QtWidgets.QMessageBox.warning(None, "No images", "No non‑blacklisted images found.")
        sys.exit()

    sorter = PairwiseSorter(unranked, db, ranked)

    # UI setup
    win = QtWidgets.QMainWindow()
    win.setWindowTitle("PhotoRanker v5.1")
    central = QtWidgets.QWidget()
    win.setCentralWidget(central)
    vbox = QtWidgets.QVBoxLayout(central)
    info = QtWidgets.QLabel(alignment=QtCore.Qt.AlignCenter)
    left, right = ResponsiveLabel(), ResponsiveLabel()
    hbox = QtWidgets.QHBoxLayout()
    hbox.addWidget(left)
    hbox.addWidget(right)
    vbox.addWidget(info)
    vbox.addLayout(hbox)

    # menu
    menubar = win.menuBar()
    file_menu = menubar.addMenu("&File")
    act_add = QtWidgets.QAction("Add Folder…", win)
    act_add.setShortcut(QtGui.QKeySequence("Ctrl+O"))

    def add_folder():
        folder = QtWidgets.QFileDialog.getExistingDirectory(win, "Add image folder")
        if folder:
            new_hashes = ingest_folder(folder, db)
            sorter.add_hashes([h for h in new_hashes if h not in ranked and h not in unranked])
            unranked.extend(new_hashes)
            refresh()

    act_add.triggered.connect(add_folder)
    file_menu.addAction(act_add)

    file_menu.addSeparator()
    act_quit = QtWidgets.QAction("Quit", win)
    act_quit.setShortcut(QtGui.QKeySequence("Ctrl+Q"))
    act_quit.triggered.connect(app.quit)
    file_menu.addAction(act_quit)

    win.showMaximized()

    # helpers
    total_photos = len(ranked) + len(unranked)

    def _rank_str(h: str) -> str:
        r = db.rank(h)
        return f"#{r}" if r else "?"

    def refresh():
        pair = sorter.next_pair()
        if pair == (None, None):
            info.setText("✓ All done — leaderboard saved.")
            left.clear()
            right.clear()
            return
        h_a, h_b = pair
        left.setPixmap(QtGui.QPixmap.fromImage(pil_to_qimage(open_image(db.path(h_a)))))
        right.setPixmap(QtGui.QPixmap.fromImage(pil_to_qimage(open_image(db.path(h_b)))))
        done, todo = sorter.progress()
        info.setText(
            f"{done}/{todo} ({done/todo*100:.1f}%)  |  {_rank_str(h_a)} vs {_rank_str(h_b)} / {total_photos}  |  {int(db.rating(h_a))} vs {int(db.rating(h_b))} Elo"
        )

    # key events
    def on_key(evt):
        key = evt.key()
        if key == QtCore.Qt.Key_Left:
            sorter.vote(True)
            refresh()
        elif key == QtCore.Qt.Key_Right:
            sorter.vote(False)
            refresh()
        elif key == QtCore.Qt.Key_Z:  # blacklist left
            cand, _ref = sorter.next_pair()
            if cand:
                db.blacklist(cand)
            refresh()
        elif key == QtCore.Qt.Key_X:  # blacklist right
            _cand, ref = sorter.next_pair()
            if ref:
                db.blacklist(ref)
            refresh()
        elif key == QtCore.Qt.Key_Escape:
            app.quit()

    win.keyPressEvent = on_key  # type: ignore

    refresh()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
