import os
import csv
from colorthief import ColorThief

# Sometimes after color_extraction_script_2 runs there are a few frames left in each folder,
# this script deals with those
#
#
# This script will check each movie's frame directory for any leftover frames and write them to the 
# corresponding CSV file.

def extract_dominant_colors(image_path, num_colors=10):
    try:
        color_thief = ColorThief(image_path)
        palette = color_thief.get_palette(color_count=num_colors)
        return palette  # Return the palette directly as RGB tuples
    except Exception as e:
        print(f"Error extracting colors from {image_path}: {e}")
        return []

def process_remaining_frames(movie_index, frame_dir, csv_file, num_colors=10):
    frame_files = [f for f in os.listdir(frame_dir) if f.startswith('output_') and f.endswith('.png')]
    frame_files.sort()

    if not frame_files:
        print(f"No remaining frames found in {frame_dir}.")
        return

    print(f"Processing remaining frames in {frame_dir}...")

    for frame_file in frame_files:
        local_frame_path = os.path.join(frame_dir, frame_file)
        try:
            colors = extract_dominant_colors(local_frame_path, num_colors)
            if colors:
                with open(csv_file, "a") as csvfile:
                    writer = csv.writer(csvfile)
                    frame_filename = os.path.basename(local_frame_path)
                    row = [frame_filename]
                    for r, g, b in colors:
                        row += [r, g, b]
                    writer.writerow(row)
                    print(f"Written to CSV: {row}")
                os.remove(local_frame_path)  # Remove the file after processing
                print(f"Processed and deleted {local_frame_path}")
        except Exception as e:
            print(f"Failed to process {local_frame_path}: {e}")

if __name__ == "__main__":
    base_dir = '/Users/rsudhir/Documents/GitHub/Data-Science-Project---Colors-Of-Romance/Hindi-Analysis/Hindi-Movie-CSVs'  

    for movie_index in range(1, 26):  # Assuming you have 25 movies
        frame_dir = os.path.join(base_dir, f"movie_{movie_index}_frames")
        csv_file = os.path.join(base_dir, f"{movie_index}.csv")

        if os.path.exists(frame_dir) and os.path.exists(csv_file):
            process_remaining_frames(movie_index, frame_dir, csv_file)
        else:
            print(f"Either {frame_dir} or {csv_file} does not exist. Skipping movie {movie_index}.")
