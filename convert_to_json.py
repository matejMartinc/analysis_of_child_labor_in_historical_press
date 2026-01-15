import json


def convert_jsonl_to_formatted_json(input_file_path, output_file_path):
    all_records = []

    try:
        # Open the input JSONL file and read it line by line
        with open(input_file_path, 'r', encoding='utf-8') as f_in:
            for line in f_in:
                # Skip any empty lines in the file
                if not line.strip():
                    continue

                # Load the JSON object from the current line
                record = json.loads(line)

                # The 'annotations' value is a string, so we need to parse it separately
                if 'annotations' in record and isinstance(record['annotations'], str):
                    annotations_string = record['annotations']
                    # Parse the string into a Python list of dictionaries
                    annotations_list = json.loads(annotations_string)
                    # Replace the original string with the new list
                    record['annotations'] = annotations_list

                all_records.append(record)

        # Open the output file to write the final, formatted JSON
        with open(output_file_path, 'w', encoding='utf-8') as f_out:
            # Dump the list of all records into a single JSON array
            # The 'indent=4' argument makes the output human-readable
            json.dump(all_records, f_out, indent=4, ensure_ascii=False)

        print(f"Successfully converted '{input_file_path}' to '{output_file_path}'.")

    except FileNotFoundError:
        print(f"Error: The file '{input_file_path}' was not found.")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON. Please check file format. Details: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

convert_jsonl_to_formatted_json('results/articles_en_corpus_annotated.jsonl', 'results/articles_en_corpus_annotated_gemini_2.5_pro.json')
convert_jsonl_to_formatted_json('results/articles_ch_corpus_annotated.jsonl', 'results/articles_ch_corpus_annotated_gemini_2.5_pro.json')
convert_jsonl_to_formatted_json('results/articles_de_corpus_annotated.jsonl', 'results/articles_de_corpus_annotated_gemini_2.5_pro.json')
convert_jsonl_to_formatted_json('results/articles_fr_corpus_annotated.jsonl', 'results/articles_fr_corpus_annotated_gemini_2.5_pro.json')