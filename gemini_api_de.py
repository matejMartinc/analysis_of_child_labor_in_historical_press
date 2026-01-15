import os

import nest_asyncio
nest_asyncio.apply()

from google import genai
from rapidfuzz import fuzz

GOOGLE_API_KEY = ""

import asyncio
import json
from tqdm import tqdm
from aiolimiter import AsyncLimiter
import docx


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


def read_docx(docx_path):

    # --- Input Validation ---
    if not os.path.exists(docx_path):
        print(f"Error: The file '{docx_path}' does not exist.")
        return None
    if not docx_path.lower().endswith('.docx'):
        print(f"Error: The file '{docx_path}' is not a .docx file.")
        return None

    # Get the base name of the docx file and change the extension to .txt
    base_name = os.path.splitext(docx_path)[0]
    txt_path = base_name + '.txt'

    # --- Read the .docx file ---
    document = docx.Document(docx_path)

    # --- Extract Text ---
    # Create a list of all paragraphs in the document
    full_text = " ".join([para.text for para in document.paragraphs])
    full_text = " ".join(full_text.split())
    return full_text, base_name


def read_json(path, article):
    with open(path, "r", encoding='utf-8') as f:
        data = json.load(f)
        annotations = []
        for tag in data:
            if tag[ "document"] == article:
                annotations.append((tag["text"], tag["labels"]))
    return annotations

def get_examples(input_folder, json_with_labels, file_names, n=5):
    counter = 0
    article_prompts = []
    for idx, name in enumerate(file_names):
        counter += 1
        if counter <= n:
            article_prompt = ""
            full_text, base_name = read_docx(os.path.join(input_folder, name))
            base_name = base_name.split("/")[-1]
            annotations = read_json(json_with_labels, base_name)
            article_prompt += "--- News article ---\n"
            article_prompt += full_text
            article_prompt += "\n\n--- Annotations ---\n"
            for anno in annotations:
                snippet, label = anno
                snippet = snippet.split()
                if len(snippet[-1]) == 1:
                    snippet = snippet[:-1]
                if len(snippet[0]) == 1:
                    snippet = snippet[1:]
                snippet = ' '.join(snippet).strip()
                article_prompt += f"Label: {'; '. join(label)}\n"
                article_prompt += f"Text: \"{snippet}\"\n\n"
            article_prompts.append(article_prompt)
    return "\n".join(article_prompts)


def get_articles_from_corpus(files, path):
    article_texts = []
    article_names = []
    for f in files:
        if f.endswith('.txt'):
            with open(os.path.join(path, f), 'r', encoding='utf8') as fi:
                text = fi.read()
                text = " ".join(text.split()).strip()
                article_texts.append(text)
                article_names.append(f)
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
    print(prompt)


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
    n = 5
    output_path = 'results/articles_de_corpus_annotated.jsonl'
    input_folder = "data/annotated_data/de/atlasti_annotation-german"
    json_with_labels = "data/annotated_data/de/training_extended.json"
    file_names = os.listdir(input_folder)
    examples = get_examples(input_folder, json_with_labels, file_names, n)
    prompt1 = """You are a history expert specializing in the study of child labor. Your task is to annotate passages in historical newspaper articles that discuss child labor. You will tag segments of the text according to the specific aspect of the discourse they represent.
Below is a list of tags with descriptions of what each tag covers. Use these tags to annotate the provided text.

Annotation Tags and Descriptions:

Tag: "Class and Social Hierarchy"
Description:
    • Class-based framings, assumptions, or conflicts around child labor and politics.
    • Representations of the working class, poor, elites, or bourgeoisie.
    • Class-based paternalism or disdain.
    • Social mobility narratives (rags to riches vs. structural critique).
    • Framing of child labor as a "necessary evil" for the poor.

Tag: "Economic Context"
Description:
    • Family poverty and unemployment as drivers of child labor.
    • Employers’ economic incentives (cheap labor, efficiency).
    • Industrialization, modernization, and labor market pressures.
    • Framing child labor as preparation for future industrial roles (vocational training), future good citizens.

Tag: "Education"
Description:
    • Access to schooling as an alternative to child labor.
    • Vocational education vs. general education debates.
    • Role of schools in moral/social development (discipline, citizenship).
    • Educational reform as a political or ideological tool.
    • Critiques of schools (ineffectiveness, class bias, compulsion).
    • School attendance laws and enforcement (truancy, age limits).
    • Portrayals of teachers, curriculum, or school conditions.

Tag: "Gender"
Description:
    • Gendered framing of labor roles (e.g., boys as future breadwinners, girls as vulnerable or morally endangered).
    • Assumptions about appropriate work, protection, or education based on gender.
    • Use of gender to evoke emotional response or moral urgency (e.g., emphasis on the suffering of young girls).
    • Absence or marginalization of one gender in discussions of child labor, implying normative roles.

Tag: "Government Role"
Description:
    • State responsibility: inspections, enforcement, regulation, or neglect.
    • Municipal or local government involvement (工部局, prefectures, inspection bureaus).
    • Community or civil society initiatives (schools, associations, factory-led reforms).

Tag: "Health"
Description:
    • Physical effects: fatigue, illness, accidents, stunted growth.
    • Educational deprivation and its consequences.
    • Psychological, moral, and social effects (delinquency, loss of parental control, dignity).

Tag: "Ideological Framing"
Description:
    • Use of political ideologies (e.g., capitalism, socialism, communism, liberalism, democracy) to frame labor reform as part of a broader societal vision.
    • Arguments linking child labor to the failures or successes of particular economic systems.
    • Portrayal of reform as a threat to freedom (capitalist/democratic framing) or as a step toward justice and equality (socialist/communist framing).
    • Fear-mongering or aspirational rhetoric tying labor practices to ideological futures (e.g., "a socialist utopia" or "the decay of free enterprise").

Tag: "International Dimension"
Description:
    • Use of foreign models and precedents to argue for reform.
    • Comparisons between countries’ labor laws, inspection regimes, or industrial standards.
    • Concerns about competitiveness and industrial modernization.

Tag: "Labor Movement"
Description:
    • Strikes, worker consciousness, unionization, and collective demands.
    • Child labor as part of broader labor struggles for hours, wages, and dignity.
    • Links to adult labor rights (e.g., eight-hour day, union protections).

Tag: "Legal Framework"
Description:
    • References to laws, decrees, or regulations (existing, proposed, or debated).
    • Judicial treatment of child workers (age thresholds, liability, sentencing).
    • International comparisons of legal frameworks (e.g., British Factory Acts, French child labor laws, Chinese court cases).

Tag: "Metaphors"
Description:
    • Dramatic framing of child labor (e.g., “industrial revolution,” “terrible to visit the iron shops,” “the most pitiable little friends”).
    • Symbolic use of children as markers of national crisis, social backwardness, or progress.

Tag: "Political Affiliation"
Description:
    • Explicit or implicit references to political parties, movements, or ideologies (e.g., support from conservative or liberal factions).
    • Quotations or paraphrasing of political figures or party platforms to support or criticize labor reform (e.g., Lord Salsbury supports the reform)
    • Framing of labor issues in alignment with broader political agendas (e.g., national development, anti-socialism, class conflict).
    • Partisan bias in the portrayal of reform opponents or advocates (e.g., own party is praised, other parties are criticized – biased protrayal)

Tag: "Race, Ethnicity, and Colonialism"
Description:
    • Racial, ethnic, or national identity shaping labor discourse.
    • Depictions of minority or migrant children.
    • Racialized moral panic or stereotypes.
    • National superiority or backwardness in labor practices.
    • Reform as a means of “civilizing” or assimilating.

Tag: "Religion and Morality"
Description:
    • Religious institutions, beliefs, or moral philosophies influencing labor discourse.
    • Religious groups advocating for or against labor reform.
    • Theological framing of work, childhood, duty, or charity.
    • Sermons, missions, or church-led education/work alternatives.
    • Moral reformers invoking religious duty.

Tag: "Technological Change"
Description:
    • References to mechanization, industrial innovation, or new production methods impacting labor practices.
    • Arguments that technological advancement reduces the need for child labor or makes it more dangerous.
    • Concerns that reform may hinder industrial progress or competitiveness in a modernizing economy.
    • Framing of technology as a symbol of progress, efficiency, or disruption in labor relations.

Tag: "Social Attitudes"
Description:
    • Moral and ethical arguments (pity, compassion, social duty, hard work).
    • Portrayals of “honorable employers” vs. negligent ones.
    • Role of education and uplift in shaping “good citizens” or “pillars of industry.”
    • Rhetorical appeals to social progress, national development, or republican ideals.
    • Tying labor / non labor to crime

Tag: "Workplace" 
Description:
    • Description of factory, workshop, or street environments where children work.
    • Hours, tasks, tools, machinery, physical dangers, and accidents.
    • Cases of violence, abuse, or exploitation within workplaces.

Here are examples of news articles annotated according to the tags defined above. Note that the same text snippet can have more than one tag and that tags are separated by semicolon.\n\n"""
    instructions = prompt1 + examples
    corpus_path = "data/test_data/de/corpus_kinderarbeit_onb-labs"
    corpus_files = os.listdir(corpus_path)
    articles, ids = get_articles_from_corpus(corpus_files, corpus_path)
    all_docs = []
    print("All corpus files:", len(corpus_files))
    for id, art in zip(ids, articles):
        whole_prompt = instructions
        whole_prompt += "Please annotate the news article below in the same manner as in the example above. Return only annotations and nothing else. Do not change the extracted text in any way.\n\n"
        whole_prompt += "\n--- News article ---\n"
        whole_prompt += art
        whole_prompt += "\n--- Annotations ---\n"
        doc = {'id': id, 'prompt': whole_prompt, 'article': art}
        all_docs.append(doc)

    chunks = 10
    for i in range(0, len(all_docs), chunks):
        print("Chunk:", i)
        asyncio.run(process_grouped_documents(enumerate(all_docs[i:i + chunks]), output_path, i))


