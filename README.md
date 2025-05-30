# PhotoRanker

## Overview

PhotoRanker is a desktop application for Windows that allows users to rank images using the Elo rating system. The application provides a modern and stylish GUI using PyQt5 and optimizes image loading and processing using multithreading and caching.

## Features

- Rank images using the Elo rating system
- Modern and stylish GUI with PyQt5
- Optimized image loading and processing with multithreading and caching
- View rankings and top-ranked images
- Blacklist images from being ranked

## Setup and Installation

### Prerequisites

- Python 3.6 or higher
- pip (Python package installer)

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/SeanDohertyPhotos/PhotoRanker.git
   cd PhotoRanker
   ```

2. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

## Running the Program

1. Navigate to the project directory:

   ```bash
   cd PhotoRanker
   ```

2. Run the main script:

   ```bash
   python Main.py
   ```

3. Select a folder containing images when prompted.

## Dependencies

The program requires the following dependencies:

- PyQt5
- Pillow
- rawpy

These dependencies are listed in the `requirements.txt` file and can be installed using the `pip install -r requirements.txt` command.

## Usage

- Use the left and right arrow keys to select the winner between two images.
- Press 'Z' or 'X' to blacklist the left or right image, respectively.
- Press 'Escape' to quit the program.
- Use the buttons to view rankings and top-ranked images.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.
