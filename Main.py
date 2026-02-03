import os
import random
import json
import tkinter as tk
from tkinter import filedialog, ttk, messagebox
from PIL import Image, ImageTk
from datetime import datetime
from collections import defaultdict
import rawpy
from queue import Queue
from threading import Thread
import time

# Try to import controller support
try:
    import inputs
    CONTROLLER_AVAILABLE = True
except ImportError:
    CONTROLLER_AVAILABLE = False

# === CONFIGURATION ===
RATINGS_FILE = "elo_ratings.json"
BLACKLIST_FILE = "blacklist.json"
TOP_RANK_COUNT = 50
Image.MAX_IMAGE_PIXELS = None
GRID_SIZE = 9  # 3x3 grid for grid mode
CLUSTER_TIME_THRESHOLD = 10  # seconds between shots to group as cluster

# === TIER DEFINITIONS ===
TIER_UNREVIEWED = "unreviewed"
TIER_DISCARD = "discard"
TIER_KEEP = "keep"
TIER_PORTFOLIO = "portfolio"

# === UTILITY FUNCTIONS ===
def open_image(img_path):
    """Open an image file, handling RAW formats."""
    if img_path.lower().endswith('.dng'):
        return Image.fromarray(rawpy.imread(img_path).postprocess())
    return Image.open(img_path)

def get_images_from_folder(folder_path):
    """Recursively get all supported image files from a folder."""
    extensions = ('jpg', 'png', 'jpeg', 'dng', 'cr2', 'nef', 'arw')
    return [os.path.join(subdir, file)
            for subdir, _, files in os.walk(folder_path)
            for file in files if file.lower().endswith(extensions)]

def resize_image(img, width, height):
    """Resize image maintaining aspect ratio."""
    img_ratio = img.width / img.height
    target_ratio = width / height
    if img_ratio > target_ratio:
        new_width = width
        new_height = int(width / img_ratio)
    else:
        new_height = height
        new_width = int(height * img_ratio)
    return img.resize((new_width, new_height), Image.LANCZOS)

def get_image_timestamp(img_path):
    """Get image timestamp from EXIF or file modification time."""
    try:
        img = Image.open(img_path)
        exif = img._getexif()
        if exif and 36867 in exif:  # DateTimeOriginal
            return datetime.strptime(exif[36867], "%Y:%m:%d %H:%M:%S")
    except:
        pass
    return datetime.fromtimestamp(os.path.getmtime(img_path))

def update_elo_rank(winner_elo, loser_elo, K=32):
    """Update ELO ratings after a comparison."""
    expected_winner = 1 / (1 + 10 ** ((loser_elo - winner_elo) / 400))
    winner_elo += K * (1 - expected_winner)
    loser_elo += K * (expected_winner - 1)
    return winner_elo, loser_elo


class PhotoRanker:
    """Main application class managing all ranking modes."""

    def __init__(self):
        self.root = None
        self.folder_path = None
        self.images = []
        self.elo_ratings = {}
        self.blacklist = []
        self.clusters = {}  # cluster_id -> list of image paths
        self.preload_queue = Queue(maxsize=10)
        self.running = True

    def load_ratings(self):
        """Load existing ratings from file."""
        if os.path.exists(RATINGS_FILE):
            with open(RATINGS_FILE, 'r') as f:
                loaded = json.load(f)
                for key, value in loaded.items():
                    if isinstance(value, (int, float)):
                        loaded[key] = {
                            'path': '', 'rating': value, 'compared': 0,
                            'confidence': 0.0, 'tier': TIER_UNREVIEWED, 'cluster': None
                        }
                    elif 'tier' not in value:
                        value['tier'] = TIER_UNREVIEWED
                    if 'cluster' not in value:
                        value['cluster'] = None
                return loaded
        return {}

    def save_ratings(self):
        """Save ratings to file."""
        with open(RATINGS_FILE, 'w') as f:
            json.dump(self.elo_ratings, f, indent=2)

    def load_blacklist(self):
        """Load blacklist from file."""
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, 'r') as f:
                return json.load(f)
        return []

    def save_blacklist(self):
        """Save blacklist to file."""
        with open(BLACKLIST_FILE, 'w') as f:
            json.dump(self.blacklist, f)

    def initialize_images(self):
        """Initialize or update ratings for all images in folder."""
        for image in self.images:
            filename = os.path.basename(image)
            existing = self.elo_ratings.get(filename, {})
            self.elo_ratings[filename] = {
                'path': image,
                'rating': existing.get('rating', 1200),
                'compared': existing.get('compared', 0),
                'confidence': existing.get('confidence', 0.0),
                'tier': existing.get('tier', TIER_UNREVIEWED),
                'cluster': existing.get('cluster', None)
            }

    def build_clusters(self):
        """Group images into clusters based on timestamp proximity."""
        # Get timestamps for all images
        img_times = []
        for img in self.images:
            filename = os.path.basename(img)
            if filename in self.blacklist:
                continue
            ts = get_image_timestamp(img)
            img_times.append((img, ts))

        # Sort by timestamp
        img_times.sort(key=lambda x: x[1])

        # Group into clusters
        self.clusters = {}
        cluster_id = 0
        current_cluster = []
        last_time = None

        for img, ts in img_times:
            if last_time and (ts - last_time).total_seconds() > CLUSTER_TIME_THRESHOLD:
                if current_cluster:
                    self.clusters[cluster_id] = current_cluster
                    cluster_id += 1
                current_cluster = []
            current_cluster.append(img)
            last_time = ts

        if current_cluster:
            self.clusters[cluster_id] = current_cluster

        # Update ratings with cluster info
        for cid, imgs in self.clusters.items():
            for img in imgs:
                filename = os.path.basename(img)
                if filename in self.elo_ratings:
                    self.elo_ratings[filename]['cluster'] = cid

        return len(self.clusters)

    def get_stats(self):
        """Get current statistics."""
        total = len([i for i in self.images if os.path.basename(i) not in self.blacklist])
        tiers = {TIER_UNREVIEWED: 0, TIER_DISCARD: 0, TIER_KEEP: 0, TIER_PORTFOLIO: 0}
        compared = 0

        for img in self.images:
            filename = os.path.basename(img)
            if filename in self.blacklist:
                continue
            data = self.elo_ratings.get(filename, {})
            tier = data.get('tier', TIER_UNREVIEWED)
            tiers[tier] = tiers.get(tier, 0) + 1
            if data.get('compared', 0) > 0:
                compared += 1

        return {
            'total': total,
            'blacklisted': len(self.blacklist),
            'unreviewed': tiers[TIER_UNREVIEWED],
            'discarded': tiers[TIER_DISCARD],
            'kept': tiers[TIER_KEEP],
            'portfolio': tiers[TIER_PORTFOLIO],
            'compared': compared,
            'clusters': len(self.clusters)
        }

    def get_filtered_images(self, tiers=None, exclude_blacklist=True):
        """Get images filtered by tier."""
        result = []
        for img in self.images:
            filename = os.path.basename(img)
            if exclude_blacklist and filename in self.blacklist:
                continue
            if tiers:
                tier = self.elo_ratings.get(filename, {}).get('tier', TIER_UNREVIEWED)
                if tier not in tiers:
                    continue
            result.append(img)
        return result

    def run(self):
        """Main entry point."""
        # Select folder
        temp_root = tk.Tk()
        temp_root.withdraw()
        self.folder_path = filedialog.askdirectory(title='Select folder containing images')
        temp_root.destroy()

        if not self.folder_path:
            return

        # Load data
        self.images = get_images_from_folder(self.folder_path)
        self.elo_ratings = self.load_ratings()
        self.blacklist = self.load_blacklist()
        self.initialize_images()
        self.build_clusters()
        self.save_ratings()

        # Show mode selector
        self.show_mode_selector()

    def show_mode_selector(self):
        """Show the main mode selection menu."""
        self.root = tk.Tk()
        self.root.title('PhotoRanker - Mode Selector')
        self.root.geometry('900x700')
        self.root.configure(bg='#1a1a2e')

        # Header
        header = tk.Label(self.root, text="PhotoRanker",
                         font=('Helvetica', 32, 'bold'), bg='#1a1a2e', fg='#eee')
        header.pack(pady=20)

        # Stats frame
        stats_frame = tk.Frame(self.root, bg='#16213e', padx=20, pady=15)
        stats_frame.pack(fill=tk.X, padx=40, pady=10)

        stats = self.get_stats()
        stats_text = (
            f"Total: {stats['total']} photos  |  "
            f"Clusters: {stats['clusters']}  |  "
            f"Blacklisted: {stats['blacklisted']}\n"
            f"Unreviewed: {stats['unreviewed']}  |  "
            f"Keep: {stats['kept']}  |  "
            f"Portfolio: {stats['portfolio']}  |  "
            f"Discard: {stats['discarded']}"
        )
        tk.Label(stats_frame, text=stats_text, font=('Helvetica', 12),
                bg='#16213e', fg='#aaa', justify=tk.CENTER).pack()

        # Mode buttons frame
        modes_frame = tk.Frame(self.root, bg='#1a1a2e')
        modes_frame.pack(fill=tk.BOTH, expand=True, padx=40, pady=20)

        # Mode button style
        def create_mode_button(parent, title, subtitle, color, command):
            frame = tk.Frame(parent, bg=color, padx=20, pady=15)
            frame.pack(fill=tk.X, pady=8)
            frame.bind('<Button-1>', lambda e: command())

            title_lbl = tk.Label(frame, text=title, font=('Helvetica', 16, 'bold'),
                                bg=color, fg='white')
            title_lbl.pack(anchor='w')
            title_lbl.bind('<Button-1>', lambda e: command())

            sub_lbl = tk.Label(frame, text=subtitle, font=('Helvetica', 11),
                              bg=color, fg='#ddd')
            sub_lbl.pack(anchor='w')
            sub_lbl.bind('<Button-1>', lambda e: command())

            return frame

        # Triage Mode
        create_mode_button(modes_frame,
            "1. Triage Mode",
            f"Quick single-photo review: Keep / Discard / Portfolio  ({stats['unreviewed']} unreviewed)",
            '#0f4c75', self.start_triage_mode)

        # Grid Mode
        create_mode_button(modes_frame,
            "2. Grid Mode",
            f"Pick the best from {GRID_SIZE} photos at once - 9x faster than pairwise",
            '#1b6ca8', self.start_grid_mode)

        # Swiss Mode
        create_mode_button(modes_frame,
            "3. Swiss Tournament Mode",
            "Smart ELO pairing - only compare similar-rated photos",
            '#3282b8', self.start_swiss_mode)

        # Cluster Mode
        create_mode_button(modes_frame,
            "4. Cluster Mode",
            f"Review burst shots - pick best from each of {stats['clusters']} time-grouped clusters",
            '#4a9fd4', self.start_cluster_mode)

        # View Rankings
        create_mode_button(modes_frame,
            "View Rankings",
            "See current photo rankings and export top photos",
            '#5c6bc0', self.view_rankings)

        # Bottom buttons
        bottom_frame = tk.Frame(self.root, bg='#1a1a2e')
        bottom_frame.pack(side=tk.BOTTOM, pady=20)

        tk.Button(bottom_frame, text="Rebuild Clusters", command=self.rebuild_clusters,
                 bg='#333', fg='white', padx=15, pady=8).pack(side=tk.LEFT, padx=5)
        tk.Button(bottom_frame, text="Change Folder", command=self.change_folder,
                 bg='#333', fg='white', padx=15, pady=8).pack(side=tk.LEFT, padx=5)
        tk.Button(bottom_frame, text="Quit", command=self.quit_app,
                 bg='#c0392b', fg='white', padx=15, pady=8).pack(side=tk.LEFT, padx=5)

        # Key bindings
        self.root.bind('1', lambda e: self.start_triage_mode())
        self.root.bind('2', lambda e: self.start_grid_mode())
        self.root.bind('3', lambda e: self.start_swiss_mode())
        self.root.bind('4', lambda e: self.start_cluster_mode())
        self.root.bind('<Escape>', lambda e: self.quit_app())

        self.root.mainloop()

    def rebuild_clusters(self):
        """Rebuild clusters and refresh display."""
        count = self.build_clusters()
        self.save_ratings()
        messagebox.showinfo("Clusters Rebuilt", f"Found {count} clusters based on photo timestamps.")
        self.root.destroy()
        self.show_mode_selector()

    def change_folder(self):
        """Change the image folder."""
        new_folder = filedialog.askdirectory(title='Select folder containing images')
        if new_folder:
            self.folder_path = new_folder
            self.images = get_images_from_folder(self.folder_path)
            self.initialize_images()
            self.build_clusters()
            self.save_ratings()
            self.root.destroy()
            self.show_mode_selector()

    def quit_app(self):
        """Save and quit."""
        self.save_ratings()
        self.save_blacklist()
        self.running = False
        if self.root:
            self.root.quit()
            self.root.destroy()

    # === TRIAGE MODE ===
    def start_triage_mode(self):
        """Start single-photo triage mode."""
        self.root.destroy()

        # Get unreviewed images first, then others
        unreviewed = self.get_filtered_images(tiers=[TIER_UNREVIEWED])
        if not unreviewed:
            unreviewed = self.get_filtered_images()

        if not unreviewed:
            messagebox.showinfo("No Images", "No images available for triage.")
            self.show_mode_selector()
            return

        self.triage_images = unreviewed.copy()
        self.triage_index = 0

        # Create triage window
        self.triage_window = tk.Tk()
        self.triage_window.title('Triage Mode - Quick Review')
        self.triage_window.state('zoomed')
        self.triage_window.configure(bg='#1a1a2e')

        # Main frame
        main_frame = tk.Frame(self.triage_window, bg='#1a1a2e')
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Image display
        self.triage_label = tk.Label(main_frame, bg='#1a1a2e')
        self.triage_label.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)

        # Info bar
        info_frame = tk.Frame(main_frame, bg='#16213e', pady=10)
        info_frame.pack(fill=tk.X)

        self.triage_info = tk.Label(info_frame, text="", font=('Helvetica', 14),
                                    bg='#16213e', fg='white')
        self.triage_info.pack()

        self.triage_progress = tk.Label(info_frame, text="", font=('Helvetica', 11),
                                        bg='#16213e', fg='#aaa')
        self.triage_progress.pack()

        # Button frame
        btn_frame = tk.Frame(main_frame, bg='#1a1a2e', pady=15)
        btn_frame.pack(fill=tk.X)

        buttons = [
            ("Discard (←)", '#c0392b', lambda: self.triage_action(TIER_DISCARD)),
            ("Keep (↓)", '#27ae60', lambda: self.triage_action(TIER_KEEP)),
            ("Portfolio! (→)", '#f39c12', lambda: self.triage_action(TIER_PORTFOLIO)),
            ("Skip (Space)", '#555', lambda: self.triage_action(None)),
            ("Blacklist (X)", '#8e44ad', self.triage_blacklist),
            ("Back", '#333', self.triage_back),
        ]

        for text, color, cmd in buttons:
            tk.Button(btn_frame, text=text, command=cmd, bg=color, fg='white',
                     font=('Helvetica', 12), padx=20, pady=10).pack(side=tk.LEFT, padx=5, expand=True)

        tk.Button(btn_frame, text="Exit (Esc)", command=self.exit_triage,
                 bg='#333', fg='white', font=('Helvetica', 12),
                 padx=20, pady=10).pack(side=tk.RIGHT, padx=20)

        # Key bindings
        self.triage_window.bind('<Left>', lambda e: self.triage_action(TIER_DISCARD))
        self.triage_window.bind('<Down>', lambda e: self.triage_action(TIER_KEEP))
        self.triage_window.bind('<Right>', lambda e: self.triage_action(TIER_PORTFOLIO))
        self.triage_window.bind('<Up>', lambda e: self.triage_action(TIER_PORTFOLIO))
        self.triage_window.bind('<space>', lambda e: self.triage_action(None))
        self.triage_window.bind('x', lambda e: self.triage_blacklist())
        self.triage_window.bind('<BackSpace>', lambda e: self.triage_back())
        self.triage_window.bind('<Escape>', lambda e: self.exit_triage())

        # Controller support
        if CONTROLLER_AVAILABLE:
            self.start_controller_thread(self.triage_window, {
                'BTN_WEST': lambda: self.triage_action(TIER_DISCARD),    # X
                'BTN_SOUTH': lambda: self.triage_action(TIER_KEEP),      # A
                'BTN_EAST': lambda: self.triage_action(TIER_PORTFOLIO),  # B
                'BTN_NORTH': lambda: self.triage_blacklist(),            # Y
            })

        self.show_triage_image()
        self.triage_window.mainloop()

    def show_triage_image(self):
        """Display current triage image."""
        if self.triage_index >= len(self.triage_images):
            messagebox.showinfo("Complete", "Triage complete!")
            self.exit_triage()
            return

        img_path = self.triage_images[self.triage_index]
        filename = os.path.basename(img_path)
        data = self.elo_ratings.get(filename, {})

        try:
            # Get window size for image scaling
            self.triage_window.update()
            w = self.triage_window.winfo_width() - 40
            h = self.triage_window.winfo_height() - 200

            img = open_image(img_path)
            img = resize_image(img, w, max(h, 400))
            photo = ImageTk.PhotoImage(img)

            self.triage_label.config(image=photo)
            self.triage_label.image = photo
        except Exception as e:
            self.triage_label.config(text=f"Error loading: {e}", image='')

        # Update info
        tier = data.get('tier', TIER_UNREVIEWED)
        rating = data.get('rating', 1200)
        cluster = data.get('cluster', 'N/A')

        self.triage_info.config(text=f"{filename}  |  Rating: {rating:.0f}  |  "
                                     f"Tier: {tier.upper()}  |  Cluster: {cluster}")
        self.triage_progress.config(
            text=f"Photo {self.triage_index + 1} of {len(self.triage_images)}")

    def triage_action(self, tier):
        """Handle triage decision."""
        if self.triage_index < len(self.triage_images):
            img_path = self.triage_images[self.triage_index]
            filename = os.path.basename(img_path)

            if tier:
                self.elo_ratings[filename]['tier'] = tier
                # Boost/penalize rating based on tier
                if tier == TIER_PORTFOLIO:
                    self.elo_ratings[filename]['rating'] += 50
                elif tier == TIER_DISCARD:
                    self.elo_ratings[filename]['rating'] -= 50

            self.save_ratings()

        self.triage_index += 1
        self.show_triage_image()

    def triage_blacklist(self):
        """Blacklist current image."""
        if self.triage_index < len(self.triage_images):
            img_path = self.triage_images[self.triage_index]
            filename = os.path.basename(img_path)

            if filename not in self.blacklist:
                self.blacklist.append(filename)
                self.save_blacklist()

        self.triage_index += 1
        self.show_triage_image()

    def triage_back(self):
        """Go back to previous image."""
        if self.triage_index > 0:
            self.triage_index -= 1
            self.show_triage_image()

    def exit_triage(self):
        """Exit triage mode."""
        self.save_ratings()
        self.triage_window.destroy()
        self.show_mode_selector()

    # === GRID MODE ===
    def start_grid_mode(self):
        """Start grid comparison mode."""
        self.root.destroy()

        # Get images (prefer kept/unreviewed)
        candidates = self.get_filtered_images(tiers=[TIER_KEEP, TIER_UNREVIEWED, TIER_PORTFOLIO])
        if len(candidates) < GRID_SIZE:
            candidates = self.get_filtered_images()

        if len(candidates) < GRID_SIZE:
            messagebox.showinfo("Not Enough", f"Need at least {GRID_SIZE} images for grid mode.")
            self.show_mode_selector()
            return

        self.grid_candidates = candidates

        # Create grid window
        self.grid_window = tk.Tk()
        self.grid_window.title('Grid Mode - Pick the Best')
        self.grid_window.state('zoomed')
        self.grid_window.configure(bg='#1a1a2e')

        # Header
        header_frame = tk.Frame(self.grid_window, bg='#16213e', pady=10)
        header_frame.pack(fill=tk.X)

        tk.Label(header_frame, text="Click the BEST photo (or press 1-9)",
                font=('Helvetica', 16, 'bold'), bg='#16213e', fg='white').pack()

        self.grid_stats = tk.Label(header_frame, text="", font=('Helvetica', 11),
                                   bg='#16213e', fg='#aaa')
        self.grid_stats.pack()

        # Grid frame
        self.grid_frame = tk.Frame(self.grid_window, bg='#1a1a2e')
        self.grid_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.grid_labels = []
        self.grid_current = []

        # Create 3x3 grid
        cols = int(GRID_SIZE ** 0.5)
        for i in range(GRID_SIZE):
            row, col = i // cols, i % cols

            cell_frame = tk.Frame(self.grid_frame, bg='#2a2a4a', padx=3, pady=3)
            cell_frame.grid(row=row, column=col, sticky='nsew', padx=3, pady=3)

            # Number label
            num_label = tk.Label(cell_frame, text=str(i+1), font=('Helvetica', 14, 'bold'),
                                bg='#2a2a4a', fg='#f39c12')
            num_label.pack(anchor='nw')

            # Image label
            img_label = tk.Label(cell_frame, bg='#2a2a4a', cursor='hand2')
            img_label.pack(fill=tk.BOTH, expand=True)
            img_label.bind('<Button-1>', lambda e, idx=i: self.grid_select(idx))

            # Rating label
            rating_label = tk.Label(cell_frame, text="", font=('Helvetica', 9),
                                   bg='#2a2a4a', fg='#888')
            rating_label.pack(anchor='s')

            self.grid_labels.append((img_label, rating_label))

            self.grid_frame.grid_columnconfigure(col, weight=1)
            self.grid_frame.grid_rowconfigure(row, weight=1)

        # Bottom buttons
        btn_frame = tk.Frame(self.grid_window, bg='#1a1a2e', pady=10)
        btn_frame.pack(fill=tk.X)

        tk.Button(btn_frame, text="Skip All (Space)", command=self.grid_skip,
                 bg='#555', fg='white', padx=20, pady=8).pack(side=tk.LEFT, padx=20)
        tk.Button(btn_frame, text="Exit (Esc)", command=self.exit_grid,
                 bg='#c0392b', fg='white', padx=20, pady=8).pack(side=tk.RIGHT, padx=20)

        # Key bindings
        for i in range(1, GRID_SIZE + 1):
            self.grid_window.bind(str(i), lambda e, idx=i-1: self.grid_select(idx))
        self.grid_window.bind('<space>', lambda e: self.grid_skip())
        self.grid_window.bind('<Escape>', lambda e: self.exit_grid())

        self.grid_comparisons = 0
        self.show_grid()
        self.grid_window.mainloop()

    def show_grid(self):
        """Display a new grid of images."""
        # Select images - prefer least compared
        sorted_imgs = sorted(self.grid_candidates,
                            key=lambda x: self.elo_ratings[os.path.basename(x)].get('compared', 0))

        # Take from least compared pool with some randomization
        pool = sorted_imgs[:min(50, len(sorted_imgs))]
        self.grid_current = random.sample(pool, min(GRID_SIZE, len(pool)))

        # Get window dimensions
        self.grid_window.update()
        cols = int(GRID_SIZE ** 0.5)
        cell_w = (self.grid_window.winfo_width() - 40) // cols - 20
        cell_h = (self.grid_window.winfo_height() - 150) // cols - 50

        for i, img_path in enumerate(self.grid_current):
            try:
                img = open_image(img_path)
                img = resize_image(img, cell_w, cell_h)
                photo = ImageTk.PhotoImage(img)

                self.grid_labels[i][0].config(image=photo)
                self.grid_labels[i][0].image = photo

                filename = os.path.basename(img_path)
                rating = self.elo_ratings[filename].get('rating', 1200)
                self.grid_labels[i][1].config(text=f"{rating:.0f}")
            except Exception as e:
                self.grid_labels[i][0].config(text=f"Error", image='')
                self.grid_labels[i][1].config(text="")

        self.grid_stats.config(text=f"Comparisons this session: {self.grid_comparisons}")

    def grid_select(self, winner_idx):
        """Handle grid selection."""
        if winner_idx >= len(self.grid_current):
            return

        winner_path = self.grid_current[winner_idx]
        winner_file = os.path.basename(winner_path)

        # Winner beats all others in this grid
        for i, img_path in enumerate(self.grid_current):
            if i == winner_idx:
                continue

            loser_file = os.path.basename(img_path)
            winner_elo = self.elo_ratings[winner_file]['rating']
            loser_elo = self.elo_ratings[loser_file]['rating']

            # Use smaller K since it's implicit comparison
            K = 24 if abs(winner_elo - loser_elo) < 100 else 12
            new_winner, new_loser = update_elo_rank(winner_elo, loser_elo, K)

            self.elo_ratings[winner_file]['rating'] = new_winner
            self.elo_ratings[loser_file]['rating'] = new_loser
            self.elo_ratings[winner_file]['compared'] += 1
            self.elo_ratings[loser_file]['compared'] += 1

        self.grid_comparisons += 1
        self.save_ratings()
        self.show_grid()

    def grid_skip(self):
        """Skip current grid."""
        self.show_grid()

    def exit_grid(self):
        """Exit grid mode."""
        self.save_ratings()
        self.grid_window.destroy()
        self.show_mode_selector()

    # === SWISS TOURNAMENT MODE ===
    def start_swiss_mode(self):
        """Start Swiss tournament pairing mode."""
        self.root.destroy()

        # Get images (prefer kept/portfolio for fine-tuning)
        candidates = self.get_filtered_images(tiers=[TIER_KEEP, TIER_PORTFOLIO])
        if len(candidates) < 2:
            candidates = self.get_filtered_images()

        if len(candidates) < 2:
            messagebox.showinfo("Not Enough", "Need at least 2 images for Swiss mode.")
            self.show_mode_selector()
            return

        self.swiss_candidates = candidates

        # Create window
        self.swiss_window = tk.Tk()
        self.swiss_window.title('Swiss Tournament Mode')
        self.swiss_window.state('zoomed')
        self.swiss_window.configure(bg='#1a1a2e')

        # Main frame
        main_frame = tk.Frame(self.swiss_window, bg='#1a1a2e')
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Header
        header = tk.Frame(main_frame, bg='#16213e', pady=10)
        header.pack(fill=tk.X)

        tk.Label(header, text="Swiss Tournament - Similar Rating Matchups",
                font=('Helvetica', 16, 'bold'), bg='#16213e', fg='white').pack()

        self.swiss_stats = tk.Label(header, text="", font=('Helvetica', 11),
                                    bg='#16213e', fg='#aaa')
        self.swiss_stats.pack()

        # Image display frame
        img_frame = tk.Frame(main_frame, bg='#1a1a2e')
        img_frame.pack(fill=tk.BOTH, expand=True, pady=10)

        # Left image
        left_frame = tk.Frame(img_frame, bg='#1a1a2e')
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10)

        self.swiss_left_label = tk.Label(left_frame, bg='#2a2a4a', cursor='hand2')
        self.swiss_left_label.pack(fill=tk.BOTH, expand=True)
        self.swiss_left_label.bind('<Button-1>', lambda e: self.swiss_select(True))

        self.swiss_left_info = tk.Label(left_frame, text="", font=('Helvetica', 12),
                                        bg='#1a1a2e', fg='white')
        self.swiss_left_info.pack(pady=5)

        # Right image
        right_frame = tk.Frame(img_frame, bg='#1a1a2e')
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=10)

        self.swiss_right_label = tk.Label(right_frame, bg='#2a2a4a', cursor='hand2')
        self.swiss_right_label.pack(fill=tk.BOTH, expand=True)
        self.swiss_right_label.bind('<Button-1>', lambda e: self.swiss_select(False))

        self.swiss_right_info = tk.Label(right_frame, text="", font=('Helvetica', 12),
                                         bg='#1a1a2e', fg='white')
        self.swiss_right_info.pack(pady=5)

        # Buttons
        btn_frame = tk.Frame(main_frame, bg='#1a1a2e', pady=15)
        btn_frame.pack(fill=tk.X)

        tk.Button(btn_frame, text="← Left Wins (←)", command=lambda: self.swiss_select(True),
                 bg='#27ae60', fg='white', font=('Helvetica', 14),
                 padx=30, pady=12).pack(side=tk.LEFT, padx=20, expand=True)

        tk.Button(btn_frame, text="Skip (Space)", command=self.swiss_skip,
                 bg='#555', fg='white', font=('Helvetica', 12),
                 padx=20, pady=10).pack(side=tk.LEFT, padx=10)

        tk.Button(btn_frame, text="Right Wins (→) →", command=lambda: self.swiss_select(False),
                 bg='#27ae60', fg='white', font=('Helvetica', 14),
                 padx=30, pady=12).pack(side=tk.RIGHT, padx=20, expand=True)

        # Blacklist buttons
        bl_frame = tk.Frame(main_frame, bg='#1a1a2e', pady=5)
        bl_frame.pack(fill=tk.X)

        tk.Button(bl_frame, text="Blacklist Left (Z)", command=lambda: self.swiss_blacklist(True),
                 bg='#8e44ad', fg='white', padx=15, pady=5).pack(side=tk.LEFT, padx=20)
        tk.Button(bl_frame, text="Exit (Esc)", command=self.exit_swiss,
                 bg='#c0392b', fg='white', padx=15, pady=5).pack(side=tk.LEFT, expand=True)
        tk.Button(bl_frame, text="Blacklist Right (X)", command=lambda: self.swiss_blacklist(False),
                 bg='#8e44ad', fg='white', padx=15, pady=5).pack(side=tk.RIGHT, padx=20)

        # Key bindings
        self.swiss_window.bind('<Left>', lambda e: self.swiss_select(True))
        self.swiss_window.bind('<Right>', lambda e: self.swiss_select(False))
        self.swiss_window.bind('<space>', lambda e: self.swiss_skip())
        self.swiss_window.bind('z', lambda e: self.swiss_blacklist(True))
        self.swiss_window.bind('x', lambda e: self.swiss_blacklist(False))
        self.swiss_window.bind('<Escape>', lambda e: self.exit_swiss())

        # Controller support
        if CONTROLLER_AVAILABLE:
            self.start_controller_thread(self.swiss_window, {
                'BTN_WEST': lambda: self.swiss_select(True),      # X = left
                'BTN_EAST': lambda: self.swiss_select(False),     # B = right
                'BTN_SOUTH': lambda: self.swiss_skip(),           # A = skip
                'BTN_NORTH': lambda: self.swiss_blacklist(False), # Y = blacklist right
            })

        self.swiss_comparisons = 0
        self.swiss_left = None
        self.swiss_right = None

        # Start preloading
        self.preload_queue = Queue(maxsize=5)
        Thread(target=self.swiss_preload, daemon=True).start()

        self.show_swiss_pair()
        self.swiss_window.mainloop()

    def get_swiss_pair(self):
        """Get a pair of images with similar ratings (Swiss pairing)."""
        # Sort by rating
        sorted_imgs = sorted(self.swiss_candidates,
                            key=lambda x: self.elo_ratings[os.path.basename(x)].get('rating', 1200))

        # Filter out blacklisted
        sorted_imgs = [i for i in sorted_imgs if os.path.basename(i) not in self.blacklist]

        if len(sorted_imgs) < 2:
            return None, None

        # Pick a random image, then find a similar-rated opponent
        # Prefer less-compared images
        by_compared = sorted(sorted_imgs,
                            key=lambda x: self.elo_ratings[os.path.basename(x)].get('compared', 0))

        img1 = random.choice(by_compared[:max(10, len(by_compared)//4)])

        # Find similar rated (within 100 points)
        img1_rating = self.elo_ratings[os.path.basename(img1)]['rating']
        similar = [i for i in sorted_imgs
                   if i != img1 and
                   abs(self.elo_ratings[os.path.basename(i)]['rating'] - img1_rating) < 150]

        if not similar:
            # Fall back to any other image
            similar = [i for i in sorted_imgs if i != img1]

        if not similar:
            return None, None

        img2 = random.choice(similar[:10])

        # Randomize left/right
        if random.random() < 0.5:
            return img1, img2
        return img2, img1

    def swiss_preload(self):
        """Preload images for Swiss mode."""
        while self.running:
            try:
                if self.preload_queue.qsize() < 5:
                    img1, img2 = self.get_swiss_pair()
                    if img1 and img2:
                        # Preload images
                        left = open_image(img1)
                        right = open_image(img2)
                        self.preload_queue.put((img1, img2, left, right))
                time.sleep(0.1)
            except:
                time.sleep(0.5)

    def show_swiss_pair(self):
        """Show next Swiss pair."""
        # Try preloaded first
        if not self.preload_queue.empty():
            self.swiss_left, self.swiss_right, left_img, right_img = self.preload_queue.get()
        else:
            self.swiss_left, self.swiss_right = self.get_swiss_pair()
            if not self.swiss_left:
                messagebox.showinfo("Complete", "No more pairs to compare!")
                self.exit_swiss()
                return
            left_img = open_image(self.swiss_left)
            right_img = open_image(self.swiss_right)

        # Get display size
        self.swiss_window.update()
        w = (self.swiss_window.winfo_width() - 60) // 2
        h = self.swiss_window.winfo_height() - 250

        try:
            left_resized = resize_image(left_img, w, h)
            right_resized = resize_image(right_img, w, h)

            left_photo = ImageTk.PhotoImage(left_resized)
            right_photo = ImageTk.PhotoImage(right_resized)

            self.swiss_left_label.config(image=left_photo)
            self.swiss_left_label.image = left_photo
            self.swiss_right_label.config(image=right_photo)
            self.swiss_right_label.image = right_photo
        except Exception as e:
            print(f"Error loading images: {e}")

        # Update info
        left_data = self.elo_ratings[os.path.basename(self.swiss_left)]
        right_data = self.elo_ratings[os.path.basename(self.swiss_right)]

        self.swiss_left_info.config(
            text=f"{os.path.basename(self.swiss_left)}\nRating: {left_data['rating']:.0f}")
        self.swiss_right_info.config(
            text=f"{os.path.basename(self.swiss_right)}\nRating: {right_data['rating']:.0f}")

        rating_diff = abs(left_data['rating'] - right_data['rating'])
        self.swiss_stats.config(
            text=f"Comparisons: {self.swiss_comparisons}  |  Rating difference: {rating_diff:.0f}")

    def swiss_select(self, left_wins):
        """Handle Swiss selection."""
        if not self.swiss_left or not self.swiss_right:
            return

        winner = self.swiss_left if left_wins else self.swiss_right
        loser = self.swiss_right if left_wins else self.swiss_left

        winner_file = os.path.basename(winner)
        loser_file = os.path.basename(loser)

        winner_elo = self.elo_ratings[winner_file]['rating']
        loser_elo = self.elo_ratings[loser_file]['rating']

        # Dynamic K-factor
        K = 32 if abs(winner_elo - loser_elo) < 100 else 16

        new_winner, new_loser = update_elo_rank(winner_elo, loser_elo, K)

        self.elo_ratings[winner_file]['rating'] = new_winner
        self.elo_ratings[loser_file]['rating'] = new_loser
        self.elo_ratings[winner_file]['compared'] += 1
        self.elo_ratings[loser_file]['compared'] += 1

        self.swiss_comparisons += 1
        self.save_ratings()
        self.show_swiss_pair()

    def swiss_skip(self):
        """Skip current pair."""
        self.show_swiss_pair()

    def swiss_blacklist(self, is_left):
        """Blacklist an image in Swiss mode."""
        img = self.swiss_left if is_left else self.swiss_right
        if img:
            filename = os.path.basename(img)
            if filename not in self.blacklist:
                self.blacklist.append(filename)
                self.save_blacklist()
        self.show_swiss_pair()

    def exit_swiss(self):
        """Exit Swiss mode."""
        self.running = False
        self.save_ratings()
        self.swiss_window.destroy()
        self.running = True
        self.show_mode_selector()

    # === CLUSTER MODE ===
    def start_cluster_mode(self):
        """Start cluster review mode."""
        self.root.destroy()

        if not self.clusters:
            self.build_clusters()

        if not self.clusters:
            messagebox.showinfo("No Clusters", "No clusters found. Try adjusting the time threshold.")
            self.show_mode_selector()
            return

        # Filter to clusters with multiple images
        self.cluster_ids = [cid for cid, imgs in self.clusters.items()
                           if len(imgs) > 1 and
                           any(os.path.basename(i) not in self.blacklist for i in imgs)]

        if not self.cluster_ids:
            messagebox.showinfo("No Clusters", "No multi-image clusters to review.")
            self.show_mode_selector()
            return

        self.cluster_index = 0

        # Create window
        self.cluster_window = tk.Tk()
        self.cluster_window.title('Cluster Mode - Pick Best from Burst')
        self.cluster_window.state('zoomed')
        self.cluster_window.configure(bg='#1a1a2e')

        # Header
        header = tk.Frame(self.cluster_window, bg='#16213e', pady=10)
        header.pack(fill=tk.X)

        tk.Label(header, text="Pick the BEST photo from this cluster (burst shots)",
                font=('Helvetica', 16, 'bold'), bg='#16213e', fg='white').pack()

        self.cluster_info = tk.Label(header, text="", font=('Helvetica', 11),
                                     bg='#16213e', fg='#aaa')
        self.cluster_info.pack()

        # Scrollable frame for cluster images
        container = tk.Frame(self.cluster_window, bg='#1a1a2e')
        container.pack(fill=tk.BOTH, expand=True)

        canvas = tk.Canvas(container, bg='#1a1a2e', highlightthickness=0)
        scrollbar_y = ttk.Scrollbar(container, orient='vertical', command=canvas.yview)
        scrollbar_x = ttk.Scrollbar(container, orient='horizontal', command=canvas.xview)

        self.cluster_frame = tk.Frame(canvas, bg='#1a1a2e')

        canvas.configure(yscrollcommand=scrollbar_y.set, xscrollcommand=scrollbar_x.set)

        scrollbar_y.pack(side=tk.RIGHT, fill=tk.Y)
        scrollbar_x.pack(side=tk.BOTTOM, fill=tk.X)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        canvas.create_window((0, 0), window=self.cluster_frame, anchor='nw')
        self.cluster_frame.bind('<Configure>',
                               lambda e: canvas.configure(scrollregion=canvas.bbox('all')))

        self.cluster_canvas = canvas

        # Buttons
        btn_frame = tk.Frame(self.cluster_window, bg='#1a1a2e', pady=10)
        btn_frame.pack(fill=tk.X)

        tk.Button(btn_frame, text="← Previous Cluster", command=self.prev_cluster,
                 bg='#555', fg='white', padx=15, pady=8).pack(side=tk.LEFT, padx=20)
        tk.Button(btn_frame, text="Skip Cluster (Space)", command=self.skip_cluster,
                 bg='#555', fg='white', padx=15, pady=8).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="Next Cluster →", command=self.next_cluster,
                 bg='#555', fg='white', padx=15, pady=8).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="Exit (Esc)", command=self.exit_cluster,
                 bg='#c0392b', fg='white', padx=15, pady=8).pack(side=tk.RIGHT, padx=20)

        # Key bindings
        self.cluster_window.bind('<Left>', lambda e: self.prev_cluster())
        self.cluster_window.bind('<Right>', lambda e: self.next_cluster())
        self.cluster_window.bind('<space>', lambda e: self.skip_cluster())
        self.cluster_window.bind('<Escape>', lambda e: self.exit_cluster())

        self.show_cluster()
        self.cluster_window.mainloop()

    def show_cluster(self):
        """Display current cluster."""
        # Clear previous
        for widget in self.cluster_frame.winfo_children():
            widget.destroy()

        if self.cluster_index >= len(self.cluster_ids):
            messagebox.showinfo("Complete", "All clusters reviewed!")
            self.exit_cluster()
            return

        cluster_id = self.cluster_ids[self.cluster_index]
        images = [i for i in self.clusters[cluster_id]
                 if os.path.basename(i) not in self.blacklist]

        self.cluster_info.config(
            text=f"Cluster {self.cluster_index + 1} of {len(self.cluster_ids)}  |  "
                 f"{len(images)} photos in this burst")

        # Calculate grid dimensions
        self.cluster_window.update()
        cols = min(4, len(images))
        thumb_w = (self.cluster_window.winfo_width() - 100) // cols - 20
        thumb_h = 300

        self.cluster_images = images

        for i, img_path in enumerate(images):
            row, col = i // cols, i % cols

            cell = tk.Frame(self.cluster_frame, bg='#2a2a4a', padx=5, pady=5)
            cell.grid(row=row, column=col, padx=5, pady=5, sticky='nsew')

            try:
                img = open_image(img_path)
                img = resize_image(img, thumb_w, thumb_h)
                photo = ImageTk.PhotoImage(img)

                lbl = tk.Label(cell, image=photo, bg='#2a2a4a', cursor='hand2')
                lbl.image = photo
                lbl.pack()
                lbl.bind('<Button-1>', lambda e, idx=i: self.cluster_select_best(idx))

                # Number and rating
                filename = os.path.basename(img_path)
                rating = self.elo_ratings[filename].get('rating', 1200)

                info = tk.Label(cell, text=f"{i+1}. {rating:.0f}",
                               font=('Helvetica', 10), bg='#2a2a4a', fg='#aaa')
                info.pack()

                # Key binding
                if i < 9:
                    self.cluster_window.bind(str(i+1),
                                            lambda e, idx=i: self.cluster_select_best(idx))
            except Exception as e:
                tk.Label(cell, text=f"Error: {e}", bg='#2a2a4a', fg='red').pack()

    def cluster_select_best(self, winner_idx):
        """Select best from cluster."""
        if winner_idx >= len(self.cluster_images):
            return

        winner = self.cluster_images[winner_idx]
        winner_file = os.path.basename(winner)

        # Winner beats all others in cluster
        for i, img_path in enumerate(self.cluster_images):
            if i == winner_idx:
                continue

            loser_file = os.path.basename(img_path)
            winner_elo = self.elo_ratings[winner_file]['rating']
            loser_elo = self.elo_ratings[loser_file]['rating']

            new_winner, new_loser = update_elo_rank(winner_elo, loser_elo, K=24)

            self.elo_ratings[winner_file]['rating'] = new_winner
            self.elo_ratings[loser_file]['rating'] = new_loser
            self.elo_ratings[winner_file]['compared'] += 1
            self.elo_ratings[loser_file]['compared'] += 1

        # Mark winner as "keep" tier if unreviewed
        if self.elo_ratings[winner_file].get('tier') == TIER_UNREVIEWED:
            self.elo_ratings[winner_file]['tier'] = TIER_KEEP

        self.save_ratings()
        self.next_cluster()

    def prev_cluster(self):
        """Go to previous cluster."""
        if self.cluster_index > 0:
            self.cluster_index -= 1
            self.show_cluster()

    def next_cluster(self):
        """Go to next cluster."""
        self.cluster_index += 1
        self.show_cluster()

    def skip_cluster(self):
        """Skip current cluster."""
        self.next_cluster()

    def exit_cluster(self):
        """Exit cluster mode."""
        self.save_ratings()
        self.cluster_window.destroy()
        self.show_mode_selector()

    # === VIEW RANKINGS ===
    def view_rankings(self):
        """View current rankings."""
        self.root.destroy()

        rank_window = tk.Tk()
        rank_window.title('Photo Rankings')
        rank_window.state('zoomed')
        rank_window.configure(bg='#1a1a2e')

        # Header
        header = tk.Frame(rank_window, bg='#16213e', pady=10)
        header.pack(fill=tk.X)

        tk.Label(header, text="Photo Rankings", font=('Helvetica', 20, 'bold'),
                bg='#16213e', fg='white').pack()

        stats = self.get_stats()
        tk.Label(header, text=f"Portfolio: {stats['portfolio']} | Keep: {stats['kept']} | "
                             f"Discard: {stats['discarded']} | Unreviewed: {stats['unreviewed']}",
                font=('Helvetica', 11), bg='#16213e', fg='#aaa').pack()

        # Filter buttons
        filter_frame = tk.Frame(rank_window, bg='#1a1a2e', pady=10)
        filter_frame.pack(fill=tk.X)

        self.rank_filter = tk.StringVar(value='all')

        filters = [('All', 'all'), ('Portfolio', TIER_PORTFOLIO),
                   ('Keep', TIER_KEEP), ('Discard', TIER_DISCARD)]

        for text, value in filters:
            tk.Radiobutton(filter_frame, text=text, variable=self.rank_filter, value=value,
                          bg='#1a1a2e', fg='white', selectcolor='#333',
                          command=lambda: self.update_rankings_view(tree)).pack(side=tk.LEFT, padx=10)

        # Treeview
        style = ttk.Style()
        style.theme_use('clam')
        style.configure("Treeview", background="#2a2a4a", foreground="white",
                       fieldbackground="#2a2a4a", rowheight=25)
        style.map('Treeview', background=[('selected', '#3282b8')])

        tree_frame = tk.Frame(rank_window, bg='#1a1a2e')
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)

        tree = ttk.Treeview(tree_frame,
                           columns=('Rank', 'Filename', 'Rating', 'Tier', 'Compared', 'Cluster'),
                           show='headings')

        tree.heading('Rank', text='#')
        tree.heading('Filename', text='Filename')
        tree.heading('Rating', text='Rating')
        tree.heading('Tier', text='Tier')
        tree.heading('Compared', text='Compared')
        tree.heading('Cluster', text='Cluster')

        tree.column('Rank', width=50)
        tree.column('Filename', width=300)
        tree.column('Rating', width=80)
        tree.column('Tier', width=100)
        tree.column('Compared', width=80)
        tree.column('Cluster', width=80)

        scrollbar = ttk.Scrollbar(tree_frame, orient='vertical', command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)

        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        tree.pack(fill=tk.BOTH, expand=True)

        self.rankings_tree = tree
        self.update_rankings_view(tree)

        # Bottom buttons
        btn_frame = tk.Frame(rank_window, bg='#1a1a2e', pady=10)
        btn_frame.pack(fill=tk.X)

        tk.Button(btn_frame, text="View Top Images",
                 command=lambda: self.view_top_images(rank_window),
                 bg='#f39c12', fg='white', padx=20, pady=8).pack(side=tk.LEFT, padx=20)

        tk.Button(btn_frame, text="Export Top 50 Paths",
                 command=self.export_top_paths,
                 bg='#27ae60', fg='white', padx=20, pady=8).pack(side=tk.LEFT, padx=5)

        tk.Button(btn_frame, text="Back to Menu",
                 command=lambda: self.exit_rankings(rank_window),
                 bg='#333', fg='white', padx=20, pady=8).pack(side=tk.RIGHT, padx=20)

        rank_window.bind('<Escape>', lambda e: self.exit_rankings(rank_window))
        rank_window.mainloop()

    def update_rankings_view(self, tree):
        """Update rankings treeview with filter."""
        for item in tree.get_children():
            tree.delete(item)

        filter_val = self.rank_filter.get()

        sorted_ratings = sorted(self.elo_ratings.items(),
                               key=lambda x: x[1].get('rating', 0), reverse=True)

        rank = 1
        for filename, data in sorted_ratings:
            if filename in self.blacklist:
                continue

            tier = data.get('tier', TIER_UNREVIEWED)

            if filter_val != 'all' and tier != filter_val:
                continue

            tree.insert('', 'end', values=(
                rank,
                filename,
                f"{data.get('rating', 1200):.0f}",
                tier.upper(),
                data.get('compared', 0),
                data.get('cluster', '-')
            ))
            rank += 1

    def view_top_images(self, parent):
        """View top ranked images with thumbnails."""
        top_window = tk.Toplevel(parent)
        top_window.title('Top Ranked Photos')
        top_window.state('zoomed')
        top_window.configure(bg='#1a1a2e')

        # Scrollable canvas
        canvas = tk.Canvas(top_window, bg='#1a1a2e', highlightthickness=0)
        scrollbar = ttk.Scrollbar(top_window, orient='vertical', command=canvas.yview)

        frame = tk.Frame(canvas, bg='#1a1a2e')

        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        canvas.create_window((0, 0), window=frame, anchor='nw')

        # Get top images
        sorted_ratings = sorted(self.elo_ratings.items(),
                               key=lambda x: x[1].get('rating', 0), reverse=True)

        top_window.update()
        cols = 4
        thumb_w = (top_window.winfo_width() - 100) // cols - 20
        thumb_h = 250

        count = 0
        for filename, data in sorted_ratings[:TOP_RANK_COUNT]:
            if filename in self.blacklist:
                continue

            img_path = data.get('path', '')
            if not os.path.exists(img_path):
                continue

            row, col = count // cols, count % cols

            cell = tk.Frame(frame, bg='#2a2a4a', padx=5, pady=5)
            cell.grid(row=row, column=col, padx=5, pady=5)

            try:
                img = open_image(img_path)
                img = resize_image(img, thumb_w, thumb_h)
                photo = ImageTk.PhotoImage(img)

                lbl = tk.Label(cell, image=photo, bg='#2a2a4a')
                lbl.image = photo
                lbl.pack()

                tk.Label(cell, text=f"#{count+1} - {data['rating']:.0f}\n{filename[:30]}",
                        font=('Helvetica', 9), bg='#2a2a4a', fg='white').pack()
            except:
                tk.Label(cell, text=f"Error loading\n{filename}",
                        bg='#2a2a4a', fg='red').pack()

            count += 1

        frame.update_idletasks()
        canvas.configure(scrollregion=canvas.bbox('all'))

        # Mouse wheel scrolling
        canvas.bind_all('<MouseWheel>', lambda e: canvas.yview_scroll(-1*(e.delta//120), 'units'))

    def export_top_paths(self):
        """Export paths of top rated photos."""
        sorted_ratings = sorted(self.elo_ratings.items(),
                               key=lambda x: x[1].get('rating', 0), reverse=True)

        paths = []
        for filename, data in sorted_ratings[:50]:
            if filename not in self.blacklist:
                paths.append(data.get('path', filename))

        export_file = filedialog.asksaveasfilename(
            defaultextension='.txt',
            filetypes=[('Text files', '*.txt')],
            title='Export Top Photo Paths'
        )

        if export_file:
            with open(export_file, 'w') as f:
                f.write('\n'.join(paths))
            messagebox.showinfo("Exported", f"Exported {len(paths)} photo paths to {export_file}")

    def exit_rankings(self, window):
        """Exit rankings view."""
        window.destroy()
        self.show_mode_selector()

    # === CONTROLLER SUPPORT ===
    def start_controller_thread(self, window, button_map):
        """Start controller input thread."""
        def controller_loop():
            while self.running:
                try:
                    events = inputs.get_gamepad()
                    for event in events:
                        if event.state == 1 and event.code in button_map:
                            window.after(0, button_map[event.code])
                except inputs.UnpluggedError:
                    time.sleep(1)
                except:
                    time.sleep(0.5)

        Thread(target=controller_loop, daemon=True).start()


# === MAIN ENTRY POINT ===
if __name__ == '__main__':
    app = PhotoRanker()
    app.run()
