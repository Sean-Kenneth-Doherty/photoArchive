import os
import random
import json
from PyQt5 import QtWidgets, QtGui, QtCore
from PIL import Image, ImageEnhance
import rawpy
from queue import Queue
from threading import Thread

RATINGS_FILE = "elo_ratings.json"
BLACKLIST_FILE = "blacklist.json"
TOP_RANK_COUNT = 10
Image.MAX_IMAGE_PIXELS = None  # To handle large images

def open_image(img_path):
    return Image.open(img_path) if not img_path.lower().endswith('.dng') else Image.fromarray(rawpy.imread(img_path).postprocess())

def get_images_from_folder(folder_path):
    return [os.path.join(subdir, file) for subdir, _, files in os.walk(folder_path) for file in files if file.lower().endswith(('jpg', 'png', 'jpeg', 'dng'))]

def update_elo_rank(winner_elo, loser_elo, K):
    expected_winner = 1 / (1 + 10 ** ((loser_elo - winner_elo) / 400))
    winner_elo += K * (1 - expected_winner)
    loser_elo += K * (expected_winner - 1)
    return winner_elo, loser_elo

def get_unrated_count():
    return sum(1 for img in images if elo_ratings[os.path.basename(img)]['rating'] == 1200)

def select_winner(left_win):
    global image1, image2
    winner, loser = (image1, image2) if left_win else (image2, image1)
    filename_winner, filename_loser = os.path.basename(winner), os.path.basename(loser)

    elo_ratings[filename_winner]['compared'] += 1
    elo_ratings[filename_loser]['compared'] += 1
    elo_ratings[filename_winner]['confidence'] = elo_ratings[filename_winner]['compared'] / float(len(images))
    elo_ratings[filename_loser]['confidence'] = elo_ratings[filename_loser]['compared'] / float(len(images))

    winner_elo, loser_elo = elo_ratings[filename_winner]['rating'], elo_ratings[filename_loser]['rating']
    K = 32 if abs(winner_elo - loser_elo) < 100 else 16
    winner_elo, loser_elo = update_elo_rank(winner_elo, loser_elo, K)

    elo_ratings[filename_winner]['rating'] = winner_elo
    elo_ratings[filename_loser]['rating'] = loser_elo
    
    update_progress()
    show_next_images()

def get_least_compared_images():
    return sorted([img for img in images if os.path.basename(img) not in blacklist], 
                  key=lambda img: elo_ratings[os.path.basename(img)]['compared'])

def show_next_images():
    global image1, image2
    if not preloaded_images.empty():
        img1_path, img2_path, left_img, right_img = preloaded_images.get()
        image1, image2 = img1_path, img2_path
        left_label.setPixmap(QtGui.QPixmap.fromImage(left_img))
        right_label.setPixmap(QtGui.QPixmap.fromImage(right_img))
        update_image_info()
        return

    least_compared_images = get_least_compared_images()[:20]  # Grab the 20 least compared images
    if len(least_compared_images) < 2:
        print("Not enough images to compare. Please add more images or remove some from the blacklist.")
        return

    image1 = random.choice(least_compared_images)
    image2 = random.choice([img for img in least_compared_images if img != image1])

    if random.choice([True, False]): image1, image2 = image2, image1

    update_images(image1, image2)

def resize_image(img, width, height):
    img_ratio = img.width / img.height
    target_ratio = width / height
    new_width = width if img_ratio > target_ratio else int(height * img_ratio)
    new_height = int(width / img_ratio) if img_ratio > target_ratio else height
    return img.resize((new_width, new_height), Image.LANCZOS)

def update_images(img1, img2):
    left_img, right_img = open_and_resize_image(img1), open_and_resize_image(img2)
    left_label.setPixmap(QtGui.QPixmap.fromImage(left_img))
    right_label.setPixmap(QtGui.QPixmap.fromImage(right_img))
    update_image_info()

def update_image_info():
    left_info.setText(f"{os.path.basename(image1)}\nRating: {elo_ratings[os.path.basename(image1)]['rating']:.2f}\nCompared: {elo_ratings[os.path.basename(image1)]['compared']}")
    right_info.setText(f"{os.path.basename(image2)}\nRating: {elo_ratings[os.path.basename(image2)]['rating']:.2f}\nCompared: {elo_ratings[os.path.basename(image2)]['compared']}")

def on_key(event):
    if event.key() == QtCore.Qt.Key_Left: select_winner(True)
    elif event.key() == QtCore.Qt.Key_Right: select_winner(False)
    elif event.key() == QtCore.Qt.Key_Z: blacklist_and_replace_image(True)
    elif event.key() == QtCore.Qt.Key_X: blacklist_and_replace_image(False)
    elif event.key() == QtCore.Qt.Key_Escape: quit_program()

def blacklist_and_replace_image(is_left):
    global image1, image2
    image_to_blacklist = image1 if is_left else image2
    filename = os.path.basename(image_to_blacklist)
    if filename not in blacklist:
        blacklist.append(filename)
        print(f"Blacklisted: {filename}")
        save_blacklist()
    
    # Get a new image to replace the blacklisted one
    least_compared_images = get_least_compared_images()
    if not least_compared_images:
        print("No more images available to compare.")
        return
    
    new_image = random.choice([img for img in least_compared_images if img != image1 and img != image2])
    
    if is_left:
        image1 = new_image
        left_img = open_and_resize_image(image1)
        left_label.setPixmap(QtGui.QPixmap.fromImage(left_img))
    else:
        image2 = new_image
        right_img = open_and_resize_image(image2)
        right_label.setPixmap(QtGui.QPixmap.fromImage(right_img))
    
    update_image_info()

def save_ratings():
    with open(RATINGS_FILE, 'w') as file:
        json.dump(elo_ratings, file)

def save_blacklist():
    with open(BLACKLIST_FILE, 'w') as file:
        json.dump(blacklist, file)

def load_ratings():
    if os.path.exists(RATINGS_FILE):
        with open(RATINGS_FILE, 'r') as file:
            loaded_ratings = json.load(file)
            for key, value in loaded_ratings.items():
                if isinstance(value, (int, float)):
                    loaded_ratings[key] = {'path': '', 'rating': value, 'compared': 0, 'confidence': 0.0}
            return loaded_ratings
    return {}

def load_blacklist():
    if os.path.exists(BLACKLIST_FILE):
        with open(BLACKLIST_FILE, 'r') as file:
            return json.load(file)
    return []

def view_rankings():
    ranking_window = QtWidgets.QWidget()
    ranking_window.setWindowTitle('Image Rankings')
    ranking_window.setGeometry(100, 100, 800, 600)
    ranking_window.setStyleSheet("background-color: #2c2c2c; color: white;")

    layout = QtWidgets.QVBoxLayout(ranking_window)
    tree = QtWidgets.QTreeWidget()
    tree.setHeaderLabels(['Filename', 'Rating', 'Compared', 'Blacklisted'])
    tree.setStyleSheet("QTreeWidget { background-color: #2c2c2c; color: white; } QTreeWidget::item:selected { background-color: #22559b; }")

    for image, details in sorted(elo_ratings.items(), key=lambda x: x[1]['rating'], reverse=True):
        item = QtWidgets.QTreeWidgetItem([image, f"{details['rating']:.2f}", str(details['compared']), 'Yes' if image in blacklist else 'No'])
        tree.addTopLevelItem(item)

    layout.addWidget(tree)
    ranking_window.setLayout(layout)
    ranking_window.show()

def view_top_ranked():
    top_rank_window = QtWidgets.QWidget()
    top_rank_window.setWindowTitle('Top Ranked Images')
    top_rank_window.setStyleSheet("background-color: #2c2c2c; color: white;")
    top_rank_window.showMaximized()

    layout = QtWidgets.QVBoxLayout(top_rank_window)
    scroll_area = QtWidgets.QScrollArea()
    scroll_area.setWidgetResizable(True)
    scroll_area.setStyleSheet("background-color: #2c2c2c;")

    scroll_content = QtWidgets.QWidget()
    scroll_layout = QtWidgets.QVBoxLayout(scroll_content)

    for filename, details in sorted(elo_ratings.items(), key=lambda x: x[1]['rating'], reverse=True)[:TOP_RANK_COUNT]:
        img_path = details["path"]
        rating = round(details["rating"], 2)
        if os.path.exists(img_path) and filename not in blacklist:
            img = open_and_resize_image(img_path, width=400, height=300)
            photo = QtGui.QPixmap.fromImage(img)
            image_label = QtWidgets.QLabel()
            image_label.setPixmap(photo)
            scroll_layout.addWidget(image_label)
            scroll_layout.addWidget(QtWidgets.QLabel(f'{filename}\nRating: {rating}'))
        elif filename in blacklist:
            scroll_layout.addWidget(QtWidgets.QLabel(f'{filename} is blacklisted. Rating: {rating}'))
        else:
            scroll_layout.addWidget(QtWidgets.QLabel(f'{filename} not found. Rating: {rating}'))

    scroll_content.setLayout(scroll_layout)
    scroll_area.setWidget(scroll_content)
    layout.addWidget(scroll_area)
    top_rank_window.setLayout(layout)
    top_rank_window.show()

def quit_program():
    save_ratings()
    save_blacklist()
    app.quit()

def open_and_resize_image(img_path, width=None, height=None):
    img = open_image(img_path)
    if width and height:
        return resize_image(img, width, height)
    return resize_image(img, image_width, image_height)

def get_next_images_for_preload():
    least_compared = get_least_compared_images()[:20]
    if len(least_compared) < 2:
        return None, None
    img1, img2 = random.sample(least_compared, 2)
    return img1, img2

def preload_images():
    while True:
        img1, img2 = get_next_images_for_preload()
        if img1 is None or img2 is None:
            continue
        left_img = open_and_resize_image(img1)
        right_img = open_and_resize_image(img2)
        preloaded_images.put((img1, img2, left_img, right_img))

def update_progress():
    unrated_count = get_unrated_count()
    total_count = len(images)
    progress = (total_count - unrated_count) / total_count * 100
    progress_bar.setValue(progress)

def create_styled_button(text, command):
    button = QtWidgets.QPushButton(text)
    button.setStyleSheet("background-color: #4CAF50; color: white; padding: 10px 20px; font-size: 12pt;")
    button.clicked.connect(command)
    return button

# Main program
app = QtWidgets.QApplication([])

folder_path = QtWidgets.QFileDialog.getExistingDirectory(None, 'Select a folder containing images')
images = get_images_from_folder(folder_path)
elo_ratings = load_ratings()
blacklist = load_blacklist()

for image in images:
    filename = os.path.basename(image)
    existing_entry = elo_ratings.get(filename, {'rating': 1200, 'compared': 0, 'confidence': 0.0})
    elo_ratings[filename] = {
        'path': image,
        'rating': existing_entry.get('rating', 1200),
        'compared': existing_entry.get('compared', 0),
        'confidence': existing_entry.get('confidence', 0.0)
    }

image_width, image_height = 800, 600
preloaded_images = Queue(maxsize=5)
Thread(target=preload_images, daemon=True).start()

root = QtWidgets.QWidget()
root.setWindowTitle('Image Ranking')
root.showMaximized()
root.setStyleSheet("background-color: #2C2C2C; color: white;")

main_layout = QtWidgets.QVBoxLayout(root)

image_layout = QtWidgets.QHBoxLayout()
main_layout.addLayout(image_layout)

left_label = QtWidgets.QLabel()
left_label.setStyleSheet("background-color: #2C2C2C;")
image_layout.addWidget(left_label)

right_label = QtWidgets.QLabel()
right_label.setStyleSheet("background-color: #2C2C2C;")
image_layout.addWidget(right_label)

info_layout = QtWidgets.QHBoxLayout()
main_layout.addLayout(info_layout)

left_info = QtWidgets.QLabel()
left_info.setStyleSheet("color: white; font-size: 12pt;")
info_layout.addWidget(left_info)

right_info = QtWidgets.QLabel()
right_info.setStyleSheet("color: white; font-size: 12pt;")
info_layout.addWidget(right_info)

button_layout = QtWidgets.QHBoxLayout()
main_layout.addLayout(button_layout)

left_button = create_styled_button("← Left (←)", lambda: select_winner(True))
button_layout.addWidget(left_button)

right_button = create_styled_button("Right (→) →", lambda: select_winner(False))
button_layout.addWidget(right_button)

view_ranking_button = create_styled_button("View Rankings", view_rankings)
button_layout.addWidget(view_ranking_button)

view_top_button = create_styled_button("View Top Ranked", view_top_ranked)
button_layout.addWidget(view_top_button)

quit_button = create_styled_button("Quit", quit_program)
button_layout.addWidget(quit_button)

progress_bar = QtWidgets.QProgressBar()
progress_bar.setStyleSheet("background-color: #2C2C2C; color: white;")
main_layout.addWidget(progress_bar)

root.setLayout(main_layout)

root.keyPressEvent = on_key

show_next_images()
update_progress()
root.show()
app.exec_()
