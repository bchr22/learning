import pandas as pd
import tqdm
import json
import os
import boto3
import time

def initialize_bedrock():
    session1 = boto3.Session(profile_name='bedrock-profile-1')
    return session1.client("bedrock-runtime")

def load_data(file_path):
    return pd.read_excel(file_path)

def load_transcripts(transcript_path):
    with open(transcript_path, 'r', encoding='utf-8') as f:
        transcripts_data = json.load(f)
    return {item['Id']: item['Transcript'] for item in transcripts_data}

def setup_output_directory(output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    
def LLM(transcript_summary, flashcards_list):
    flashcard_texts = ""
    for idx, flashcard in enumerate(flashcards_list, start=1):
        flashcard_texts += f"Flashcard_{idx}: {flashcard}\n"

    user_prompt = f"""
Video Information:

Video Transcript: {transcript_summary}
Flashcard Information:

{flashcard_texts}

a. Primary Flashcard: The content is crucial for understanding the core concepts presented in the video. It directly relates to the main topic and is essential for learners to grasp the key points discussed in the transcript.

b. Secondary Flashcard: The content provides valuable supplementary information that enhances understanding of the video's topic. While not critical, it offers useful context, examples, or deeper insights that are directly related to the content of the video transcript.

c. Non-Relevant Flashcard: The content has little to no direct relevance to the video's main topic or the content discussed in the transcript. It may be generally medical-related but does not significantly contribute to understanding the specific video content.

Classification Guidelines:

1. Thoroughly review the video transcript to understand the context and main educational objectives.
2. Ensure each flashcard's content is explicitly mentioned or directly related to the video transcript. Do not infer connections beyond the provided content.
3. Evaluate the centrality of each flashcard's information to the video's main educational objectives.
4. Assess whether each flashcard's information would be essential for a quiz or test on the video content.
5. Only classify a flashcard as Primary or Secondary if it matches the context and content of the video transcript. If not, classify it as Non-Relevant.

6. Please do not provide any additional context.
7. Strictly provide your classifications in a JSON format as follows:
{{"Flashcard_1":"Non-Relevant","Flashcard_2":"Primary","Flashcard_3":"Secondary","Flashcard_4":"Non-Relevant","Flashcard_5":"Primary"}}

Your classifications should be based solely on the provided information and your expertise in medical education.
"""

    system_prompt = """
You are an expert in medical education with extensive experience in content curation and taxonomy. 
Your task is to accurately classify flashcards based on their relevance and importance to specific medical education videos. 
Pay close attention to the video transcript to ensure each flashcard's content aligns with the video's context. Only classify flashcards as Primary or Secondary if they directly match the content in the transcript.
Make sure the output is a valid JSON object and follows this structure exactly:
{
    "Flashcard_1": "Primary",
    "Flashcard_2": "Secondary",
    "Flashcard_3": "Non-Relevant",
    ...
}
"""

    messages = [
       
        {
            "role": "user",
            "content": [
                {
                    "text": user_prompt
                }
            ],
        }
    ]
   
    response = bedrock_runtime.converse(
        system = [
            {
                "text": system_prompt
            }
        ],
        modelId = "anthropic.claude-3-5-sonnet-20240620-v1:0",
        messages = messages,
        inferenceConfig={
            "maxTokens": 100,
            "temperature": 0.0
        }
    )
    response_text = response["output"]["message"]["content"][0]["text"]
    return response_text

if 'Flashcard Classification' not in data.columns:
    data['Flashcard Classification'] = None

def simplify_flashcard_classification(classification):
    try:
        classification_lower = classification.lower()
        if "primary" in classification_lower:
            return "Primary Flashcard"
        elif "secondary" in classification_lower:
            return "Secondary Flashcard"
        elif "non-relevant" in classification_lower or "non relevant" in classification_lower:
            return "Non-Relevant Flashcard"
        else:
            return "No Class"
    except Exception as e:
        print(f"Error during classification simplification: {e}")
        return "No Class"




def process_bb_video_titles(data, transcript_mapping):
    bb_video_titles_processed = 0

    data_grouped = data.groupby('videoTitle')

    for video_title, group in tqdm.tqdm(data_grouped, total=len(data_grouped), desc="Processing BB Video Titles"):
        output_file_path = os.path.join(output_dir, f'classified_{video_title}.csv')

        if os.path.exists(output_file_path):
            print(f"Skipping already processed BB Video Title: {video_title}")
            continue

        if group['Flashcard Classification'].notnull().all():
            print(f"Skipping already classified BB Video Title: {video_title}")
            continue

        # Get the cadmoreAlternateID from the CSV. Assuming the column is named 'cadmoreAlternateID'.
        cadmore_alternate_id = group['cadmoreAlternateID'].iloc[0]
        print("**cadmore_alternate_id: ",cadmore_alternate_id,"**")
        # Use cadmore_alternate_id to look up transcript by 'Id' in the JSON.
        transcript_text = transcript_mapping.get(cadmore_alternate_id, '')

        modified_rows = []

        group = group.reset_index()

        # Process flashcards in batches of up to 5
        for idx in tqdm.tqdm(range(0, len(group), 5), desc="Processing batches"):
            batch = group.iloc[idx:idx+5]
            flashcards_list = batch['Flashcard'].tolist()
            indices = batch['index'].tolist()

            try:
                classification = LLM(transcript_text, flashcards_list)
                print(classification)
                classification_dict = json.loads(classification)
            except json.JSONDecodeError as e:
                # Attempt to fix JSON if malformed
                try:
                    fixed_classification = classification.replace("'", '"')
                    if not fixed_classification.startswith('{'):
                        fixed_classification = '{' + fixed_classification
                    if not fixed_classification.endswith('}'):
                        fixed_classification = fixed_classification + '}'
                    classification_dict = json.loads(fixed_classification)
                except Exception as e2:
                    print(f"Error parsing JSON after fix attempt: {e2}")
                    for i in indices:
                        data.at[i, 'Flashcard Classification'] = 'No Class'
                    continue

            # Update the data with the classifications
            for i, idx_val in enumerate(indices):
                flashcard_key = f"Flashcard_{i+1}"
                classification_result = classification_dict.get(flashcard_key, 'No Class')
                simplified_classification = simplify_flashcard_classification(classification_result)
                data.at[idx_val, 'Flashcard Classification'] = simplified_classification
                modified_rows.append(idx_val)

        if modified_rows:
            data.loc[modified_rows].to_csv(output_file_path, index=False)
            print(f"Saved classification for BB Video Title '{video_title}' to {output_file_path}")

        bb_video_titles_processed += 1

    return bb_video_titles_processed

def main():
    file_path = '/home/ubuntu/projects/ANKI_MHE_imp/v2No_class_12Mar2025endocrinology_delta_v1.xlsx'
    transcript_path = '/home/ubuntu/projects/ANKI_MHE_imp/delta_endocrniology_file_output.json'
    output_dir = 'v2NoClass12mardelta_v1complete_step1'
    final_output_file_path = 'v2NoClass12mardelta_v1complete_step1/endocrinology_delta_v3_12mar2025final.csv'
    error_output_file_path = 'Final_complete_step1/12mar2025_progress_on_error.csv'

    bedrock_runtime = initialize_bedrock()
    data = load_data(file_path)
    transcript_mapping = load_transcripts(transcript_path)

    print(f"Total rows in data: {len(data)}")

    setup_output_directory(output_dir)

    retry_count = 0
    max_retries = 10
    delay_seconds = 600

    while retry_count < max_retries:
        try:
            process_bb_video_titles(data, transcript_mapping)
            break  # If processing completes without exceptions, exit the loop
        except Exception as e:
            print(f"An error occurred: {e}")
            data.to_csv(error_output_file_path, index=False)
            print(f"Saved progress due to error to {error_output_file_path}")
            retry_count += 1
            if retry_count < max_retries:
                print(f"Retrying in {delay_seconds / 60} minutes... ({retry_count}/{max_retries})")
                time.sleep(delay_seconds)
            else:
                print("Max retries reached. Exiting.")
                break

    data.to_csv(final_output_file_path, index=False)
    print(f"Saved final progress to {final_output_file_path} after processing all BB Video Titles.")

if __name__ == "__main__":
    main()

