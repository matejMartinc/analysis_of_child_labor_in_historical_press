import json
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict
import pandas as pd
import numpy as np


def analyze_corpus(json_file_path, lang, distrib, decade):
    labels_per_year = defaultdict(lambda: defaultdict(int))
    labels_per_source = defaultdict(lambda: defaultdict(int))
    all_labels = set()
    with open(json_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)

            # --- ID Parsing ---
            parts = data['id'].split('-_')
            date_part = parts[0]
            year = date_part.split('-')[0]
            if decade:
                year = year[:-1] + '0s'
            source_and_id = parts[1].split('_')
            source = source_and_id[0]


            # --- Annotation Processing ---
            annotations = data.get('annotations')
            # Check if annotations is a string and load it as JSON
            if isinstance(annotations, str):
                annotations = json.loads(annotations)

            if annotations:
                for annotation in annotations:
                    # Split labels by comma and strip whitespace
                    labels = [label.strip().replace('Metaphors', 'Metaphore').replace('Metaphore', 'Metaphor') for label in annotation['Label'].split(';')]
                    for label in labels:
                        labels_per_year[year][label] += 1
                        labels_per_source[source][label] += 1
                        all_labels.add(label)

                        # --- Color Map Generation ---
    sorted_labels = sorted(list(all_labels))
    num_labels = len(sorted_labels)

    if num_labels > 0:
        # Generate a list of unique colors using a perceptually uniform colormap
        # 'turbo' is excellent for creating many visually distinct colors
        colors = plt.cm.turbo(np.linspace(0, 1, num_labels))
        # Create a dictionary to map each label to a specific color
        color_map = dict(zip(sorted_labels, colors))
    else:
        color_map = {}
    if not distrib:
        # --- Data Conversion for Plotting ---
        # Convert nested dictionaries to DataFrames
        df_year = pd.DataFrame.from_dict(labels_per_year, orient='index').fillna(0)
        df_year = df_year.sort_index()
        df_source = pd.DataFrame.from_dict(labels_per_source, orient='index').fillna(0)

        # --- Plotting ---
        # Plot for labels per year
        if not df_year.empty:
            plot_colors = [color_map[label] for label in df_year.columns]
            df_year.plot(kind='bar', stacked=True, figsize=(15, 8), color=plot_colors)
            plt.title('Number of Different Labels per Year')
            plt.xlabel('Year')
            plt.ylabel('Number of Labels')
            plt.xticks(rotation=45)
            plt.legend(title='Labels', bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.tight_layout()
            if not decade:
                plt.savefig('year_count_' + lang + '.png')
            else:
                plt.savefig('decade_count_' + lang + '.png')

        # Plot for labels per source
        if not df_source.empty:
            plot_colors = [color_map[label] for label in df_source.columns]
            df_source.plot(kind='bar', stacked=True, figsize=(15, 8), color=plot_colors)
            plt.title('Number of Different Labels per Source')
            plt.xlabel('Source')
            plt.ylabel('Number of Labels')
            plt.xticks(rotation=45)
            plt.legend(title='Labels', bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.tight_layout()
            plt.savefig('source_count_' + lang + '.png')
    else:

        df_year = pd.DataFrame.from_dict(labels_per_year, orient='index').fillna(0)
        df_year = df_year.sort_index()
        # Normalize the data to get the distribution (percentage)
        df_year_dist = df_year.div(df_year.sum(axis=1), axis=0) * 100

        df_source = pd.DataFrame.from_dict(labels_per_source, orient='index').fillna(0)
        # Normalize the data to get the distribution (percentage)
        df_source_dist = df_source.div(df_source.sum(axis=1), axis=0) * 100

        # --- Plotting ---
        # Plot for label distribution per year
        if not df_year_dist.empty:
            plot_colors = [color_map[label] for label in df_year_dist.columns]
            df_year_dist.plot(kind='bar', stacked=True, figsize=(15, 8), color=plot_colors)
            plt.title('Distribution of Annotation Labels per Year')
            plt.xlabel('Year')
            plt.ylabel('Distribution of Labels (%)')
            plt.xticks(rotation=45, ha="right")
            # Move the legend outside of the plot
            plt.legend(title='Labels', bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.tight_layout()
            if not decade:
                plt.savefig('year_distrib_' + lang + '.png')
            else:
                plt.savefig('decade_distrib_' + lang + '.png')

        # Plot for label distribution per source
        if not df_source_dist.empty:
            plot_colors = [color_map[label] for label in df_source_dist.columns]
            df_source_dist.plot(kind='bar', stacked=True, figsize=(15, 8), color=plot_colors)
            plt.title('Distribution of Annotation Labels per Source')
            plt.xlabel('Source')
            plt.ylabel('Distribution of Labels (%)')
            plt.xticks(rotation=45, ha="right")
            # Move the legend outside of the plot
            plt.legend(title='Labels', bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.tight_layout()
            plt.savefig('source_distrib_' + lang + '.png')


analyze_corpus("articles_de_corpus_annotated.jsonl", lang='de', distrib=False, decade=False)
