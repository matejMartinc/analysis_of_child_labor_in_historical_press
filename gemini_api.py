import os

import nest_asyncio
import pandas as pd

nest_asyncio.apply()

from google import genai
from rapidfuzz import fuzz

GOOGLE_API_KEY = ""

import asyncio
import json
from tqdm import tqdm
from aiolimiter import AsyncLimiter


api_rate_limiter = AsyncLimiter(max_rate=10, time_period=1)

MAX_RETRIES = 3


def create_spanned_annotations_json(article_text, annotations_str):
    annotations = []
    # Split the annotation string into lines and remove any leading/trailing whitespace
    lines = [line.strip() for line in annotations_str.strip().split('\n')]

    # A variable to ensure we don't find the same text snippet twice.
    # We will always search from the end of the last found span.
    search_from_index = 0

    # Iterate through the lines two at a time (one for Label, one for Text)
    for i in range(0, len(lines), 2):
        # Ensure we have a valid pair of Label and Text lines
        if i + 1 < len(lines) and lines[i].startswith("Label:") and lines[i + 1].startswith("Text:"):

            # Extract the label and the text content
            label = lines[i].replace("Label:", "").strip()
            text = lines[i + 1].replace("Text:", "").strip()

            # Remove potential surrounding quotes from the text to search for
            if text.startswith('"') and text.endswith('"'):
                text = text[1:-1]

            # Find the start position of the text in the article
            # Begin the search from the end of the last match
            start_pos = article_text.find(text, search_from_index)

            if start_pos != -1:
                end_pos = start_pos + len(text)

                annotations.append({
                    "Label": label,
                    "Text": text,
                    "Span": [start_pos, end_pos]
                })

                # Update the search position for the next iteration
                search_from_index = end_pos
            else:
                # --- CORRECTED FUZZY MATCHING LOGIC ---
                # If no exact match, search for the best fuzzy match.

                # --- Configuration ---
                MIN_SIMILARITY_THRESHOLD = 80  # Adjusted threshold for potentially difficult matches

                # Search the rest of the article from the last known point
                search_area = article_text[search_from_index:]

                if not search_area: continue  # Skip if there's nothing left to search

                try:
                    # This function returns a ScoreAlignment object with indices of the best match
                    alignment = fuzz.partial_ratio_alignment(
                        text,
                        search_area,
                        score_cutoff=MIN_SIMILARITY_THRESHOLD
                    )

                    if alignment:
                        # **THE FIX IS HERE:** Reconstruct the matched text using the returned indices
                        match_start_relative = alignment.dest_start
                        match_end_relative = alignment.dest_end
                        best_match_text = search_area[match_start_relative:match_end_relative]

                        # Convert relative indices to absolute positions in the full article
                        best_start_pos = search_from_index + match_start_relative
                        best_end_pos = search_from_index + match_end_relative

                        # Inform the user about the correction
                        print(f"--- Fuzzy Match Found (Corrected) ---")
                        print(f"  Label: {label}")
                        print(f"  Original Text: '{text}'")
                        print(f"  Matched Text:  '{best_match_text}' (Score: {alignment.score:.1f}%)")
                        print(f"-------------------------------------\n")

                        annotations.append({
                            "Label": label,
                            "Text": best_match_text,  # Use the corrected text from the article
                            "Span": [best_start_pos, best_end_pos]
                        })
                        search_from_index = best_end_pos
                    else:
                        print(
                            f"Warning: Could not find a suitable match for the following text (Score < {MIN_SIMILARITY_THRESHOLD}%):\n'{text}'\n")

                except Exception as e:
                    print(f"An error occurred during fuzzy search for '{text}': {e}")
                except Exception as e:
                    print(f"An error occurred during fuzzy search for '{text}': {e}")

    # Convert the list of dictionaries into a nicely formatted JSON string
    return json.dumps(annotations)





def get_labels(input_json):
    prompt = ""
    with open(input_json, "r", encoding='utf-8') as f:
        data = json.load(f)
        tags = data["tag_sets"][0]
        for tag in tags["tags"]:
            prompt += f'Tag: "{tag["tag_name"]}"\nDescription:\n{tag["tag_description"]}\n\n'.replace('●', '•')
    return prompt


def get_examples(path, n=5):
    files = os.listdir(path)
    counter = 0
    article_prompts = []
    for f in files:
        dir = os.listdir(os.path.join(path, f))
        counter += 1
        if counter <= n:
            for sub_f in dir:
                dir2 = os.path.join(path, f, sub_f)
                files2 = os.listdir(dir2)
                for sub_sub_f in files2:
                    if sub_sub_f.endswith('.json'):
                        #print(f)
                        input_json = os.path.join(dir2, sub_sub_f)
                        with open(input_json, "r", encoding='utf-8') as jf:
                            article_prompt = ""
                            data = json.load(jf)
                            data.pop("_context")
                            data['_views'][ "_InitialView"].pop("FeatureDefinition" )
                            data['_views']["_InitialView"].pop("LayerDefinition")
                            data['_views']["_InitialView"].pop("DocumentMetaData")
                            data['_views']["_InitialView"].pop("TagsetDescription")

                            article_prompt += "\n--- News article ---\n"
                            annotations = data['_views']["_InitialView"]['Chunk']
                            full_text = data['_referenced_fss']['1']['sofaString']
                            article_prompt += full_text
                            article_prompt += "\n--- Annotations ---\n"
                            for anno in annotations:
                                try:
                                    begin_index = anno['begin']
                                except:
                                    begin_index = 0
                                end_index = anno['end']
                                if 'chunkValue' in anno:
                                    label = anno['chunkValue']
                                else:
                                    label = "unknown label"

                                # Extract the text snippet using the begin/end indices
                                snippet = full_text[begin_index:end_index]

                                article_prompt += f"Label: {label}\n"
                                article_prompt += f"Text: \"{snippet}\"\n\n"
                                #print(f"Position: (Characters {begin_index}:{end_index})\n")
                            article_prompts.append(article_prompt)
    return "\n".join(article_prompts)


def get_articles_from_corpus(path, language):
    if language == "french":
        df = pd.read_csv(path, encoding='utf8', sep=',')
        df = df.astype(str)
        article_texts = df['article_text'].tolist()
        article_names = df['date'] + "---" + df['id'].tolist()
    elif language == "chinese":
        df = pd.read_csv(path, encoding='utf8', sep=',')
        df = df.astype(str)
        article_texts = df['text'].tolist()
        article_names = (df['date'] + "---" + df['id']).tolist()
    else:
        df = pd.read_csv(path, encoding='utf8', sep=';')
        df = df.astype(str)
        article_texts = df['fulltext'].tolist()
        article_names = (df['date'] + "---" + df['id']).tolist()

    return article_texts, article_names


async def process_document(i, document, delay=1):
    """
    Process a single document: wait for a given delay, then send the prompt to the Gemini model,
    enforcing the API rate limit with a limiter.

    After receiving the response, the function attempts to clean and evaluate the text.
    If that fails, it retries the LLM call up to MAX_RETRIES times.
    """

    client = genai.Client(api_key=GOOGLE_API_KEY)

    prompt = document['prompt']


    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            await asyncio.sleep(delay)
            async with api_rate_limiter:
                result = await client.aio.models.generate_content(
                    model='gemini-2.5-pro',
                    contents=[
                        prompt,
                    ]
                )
            text = result.text
            return i, text, document

        except Exception as e:
            attempt += 1
            if attempt >= MAX_RETRIES:
                return i, f"Error after {MAX_RETRIES} retries: {e}"


async def process_grouped_documents(documents_grouped, output_path, current_chunk):
    """
    Processes all documents in parallel by creating tasks for each document.
    documents_grouped is a list of tuples (original_index, document).
    Uses asyncio.as_completed to yield tasks as they finish, updating a progress bar.
    Returns a dictionary mapping the original document indices to their generated text.
    """

    tasks = [
        asyncio.create_task(process_document(i, document))
        for i, document in documents_grouped
    ]
    output_file = open(output_path, 'a', encoding='utf-8')

    for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing Documents"):
        try:
            all_output = await future
            if len(all_output) == 3:
                i, output_text, document = all_output
            else:
                print('Error with output: ')
                print(all_output)
                continue
            example = {}
            example["id"] = document['id']
            example["article"] = document['article']
            processed_annotations = create_spanned_annotations_json(document['article'], output_text)
            example["annotations"] = processed_annotations
            json_line = json.dumps(example, ensure_ascii=False)  # Convert dictionary to a JSON string
            output_file.write(json_line + '\n')
        except Exception as e:
            print('Error in generation:')
            print(e)
            continue
    output_file.close()


if __name__ == '__main__':

    languages = {
        "english": {
            "input_json": "data/annotated_data/en/exportedproject8445656513168862557.json",
            "path_articles": "data/annotated_data/en/annotation",
            "output_path": "results/articles_en_corpus_annotated.jsonl",
            "corpus_path": "data/test_data/en/Child_Labor_2025-09-10_Corp.csv"
        },
        "french": {
            "input_json": "data/annotated_data/fr/exportedproject8149120778901053903.json",
            "path_articles": "data/annotated_data/fr/annotation",
            "output_path": "results/articles_fr_corpus_annotated.jsonl",
            "corpus_path": "data/test_data/fr/Travail_enfants_2025-09-10_Corp.csv"
        },
        "chinese": {
            "input_json": "data/annotated_data/ch/exportedproject4384858266144915893.json",
            "path_articles": "data/annotated_data/ch/annotation",
            "output_path": "results/articles_ch_corpus_annotated.jsonl",
            "corpus_path": "data/test_data/ch/Tonggong_2025-09-10_Corp.csv"
        }
    }

    for language in languages.keys():

        print("Processing", language)
        all_docs = []
        n = 5
        prompt1 = "You are a history expert specializing in the study of child labor. Your task is to annotate passages in historical newspaper articles that discuss child labor. You will tag segments of the text according to the specific aspect of the discourse they represent.\n"
        prompt1 += "Below is a list of tags with descriptions of what each tag covers. Use these tags to annotate the provided text.\n\nAnnotation Tags and Descriptions:\n\n"
        prompt1 += get_labels(languages[language]["input_json"])
        prompt2 = "Here are examples of news articles annotated according to the tags defined above:\n"
        examples = get_examples(languages[language]["path_articles"], n=n)
        prompt2 += examples
        instructions = prompt1 + prompt2
        articles, ids = get_articles_from_corpus(languages[language]["corpus_path"], language)
        for id, art in zip(ids, articles):
            #art = " ".join(art.split())
            whole_prompt = instructions
            whole_prompt += "Please annotate the news article below in the same manner as in the example above. Return only annotations and nothing else. Do not change the extracted text in any way.\n"
            whole_prompt += "\n--- News article ---\n"
            whole_prompt += art
            whole_prompt += "\n--- Annotations ---\n"
            doc = {'id': id, 'prompt': whole_prompt, 'article': art}
            all_docs.append(doc)
        chunks = 10
        for i in range(0, len(all_docs), chunks):
            print("Chunk:", i)
            asyncio.run(process_grouped_documents(enumerate(all_docs[i:i + chunks]), languages[language]["output_path"], i))


