# The Colors of Romance - Project README

## Overview

This project explores the influence of language and culture on the color palettes used in romance films across three languages: English, Hindi, and Korean. The analysis includes K-Means clustering, Gaussian Mixture Modeling (GMM), and an examination of brightness, saturation, contrast, and color temperature. Additionally, the project delves into potential linguistic and cultural links to the colors found in these films.

## Directory Structure

### 1. Combined-Analysis
This directory contains the combined analysis for all three languages (English, Hindi, Korean). It includes histograms comparing the distributions of colors across languages.

- **Combined_GMM_Histogram_Analysis.ipynb**: GMM analysis for all languages in a single notebook, including histograms and comparisons.
- **Combined_K-means_Histogram_Analysis.ipynb**: K-Means analysis for all languages with histograms showing the distribution of colors.
- **Combined_K-means_Time_Series_Histogram_Analysis.ipynb**: Time-series analysis showing how color tones vary throughout the length of the films for all languages.

### 2. English-Analysis
This directory focuses on the analysis of English romance films.

- **English_GMM_Analysis.ipynb**: Contains the GMM analysis for English films.
- **English_K_means_&_Color_Analysis_.ipynb**: Includes the K-Means clustering and analysis of contrast, saturation, brightness, and color temperature.
- **English_K_means_Segmented_Analysis.ipynb**: Analyzes the K-Means clusters for each segment of the films, providing insights into how color usage varies throughout the films.

### 3. Hindi-Analysis
This directory is dedicated to the analysis of Hindi romance films.

- **Hindi_GMM_Analysis.ipynb**: Contains the GMM analysis for Hindi films.
- **Hindi_K_means_&_Color_Analysis_.ipynb**: Includes the K-Means clustering and analysis of contrast, saturation, brightness, and color temperature.
- **Hindi_K_means_Segmented_Analysis.ipynb**: Analyzes the K-Means clusters for each segment of the films, providing insights into how color usage varies throughout the films.

### 4. Korean-Analysis
This directory is dedicated to the analysis of Korean romance films.

- **Korean_GMM_Analysis.ipynb**: Contains the GMM analysis for Korean films.
- **Korean_K_means_&_Color_Analysis_.ipynb**: Includes the K-Means clustering and analysis of contrast, saturation, brightness, and color temperature.
- **Korean_K_means_Segmented_Analysis.ipynb**: Analyzes the K-Means clusters for each segment of the films, providing insights into how color usage varies throughout the films.

### 5. color_extraction_script
This directory contains scripts used for extracting color data from the films.

- **color_extraction_script.py**: Extracts frame data for one movie, using scene detection to limit the number of frames analyzed.
- **color_extraction_script_2.py**: Automates the extraction process for a group of movies, processing each film sequentially.
- **remaining_frames_extraction.py**: Processes any frames that may have been skipped in `color_extraction_script_2.py`, ensuring that all relevant frames are analyzed.
- **Testing Scene Detect - Howls**: A testing directory used to validate the scene detection algorithm in the color extraction scripts.

## Key Points of Analysis

1. **K-Means Clustering**: Used to identify predominant colors within different segments of each movie and across languages. Final clustering condensed approximately 2,600 colors into a set of 30 representative colors for each language.

2. **Brightness, Saturation, and Contrast**: These attributes were analyzed to determine biases in color characteristics, revealing cultural influences in visual aesthetics.

3. **Color Temperature**: Explored to understand the overall tendency towards warmer or cooler tones in films, with cultural and linguistic factors potentially influencing these preferences.

4. **Gaussian Mixture Modeling (GMM)**: Investigated as an alternative to K-Means, but ultimately not used due to less distinct clustering results.

5. **Linguistic and Cultural Links**: Explored the possible connections between language, culture, and color usage in films, with insights drawn from external studies and literature.
