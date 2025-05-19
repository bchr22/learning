import os
import time
import pandas as pd
import json
import tqdm
import boto3

def main():
    # Step 1: Process JSON & Excel data (Extract & Structure)
    bb_transcripts_file = "/path/to/bb_transcripts.json"
    bb_hierarchical_file = "/path/to/bb_hierarchical.json"
    anki_excel_file = "/path/to/anki_data.xlsx"
    output_dir = "product_files2"

    process_result = process_data(bb_transcripts_file, bb_hierarchical_file, anki_excel_file, output_dir)
    print(process_result)

    if process_result["status"] != "success":
        print("Error in Step 1: Exiting pipeline.")
        return  

    # Step 2: Flashcard Matching using Sentence Embeddings
    json_file_path = os.path.join(output_dir, "some_output.json")  # Replace with actual filename
    embeddings_pkl_path = "/path/to/flashcard_embeddings.pkl"
    output_excel_path = "flashcards_mapped.xlsx"

    step2_result = process_step2(json_file_path, embeddings_pkl_path, output_excel_path)
    print(step2_result)

    if step2_result["status"] != "success":
        print("Error in Step 2: Exiting pipeline.")
        return  

    # Step 3: Generate Flashcard Embeddings (For Step 2)
    flashcard_excel = "/path/to/anki_data.xlsx"
    flashcard_embeddings_output = "flashcard_embeddings.pkl"

    step3_result = create_flashcard_embeddings(flashcard_excel, flashcard_embeddings_output)
    print(step3_result)

    if step3_result["status"] != "success":
        print("Error in Step 3: Exiting pipeline.")
        return  

    # Step 4: LLM-Based Flashcard Classification
    classified_output_dir = "classified_flashcards_output"
    classified_output_file = os.path.join(classified_output_dir, "classified_flashcards.csv")

    setup_output_directory(classified_output_dir)
    bedrock_runtime = initialize_bedrock()
    data = load_data(output_excel_path)
    transcript_mapping = load_transcripts(bb_transcripts_file)

    step4_result = process_bb_video_titles(data, transcript_mapping)

    if step4_result == 0:
        print("No flashcards classified. Exiting pipeline.")
        return  

    data.to_csv(classified_output_file, index=False)
    print(f"Step 4 completed. Classified flashcards saved to {classified_output_file}")

    # Step 5: Clean and Merge Output
    input_files = [classified_output_file]
    final_output = "final_merged_output.xlsx"
    no_class_output = "no_class.xlsx"
    with_class_output = "with_class.xlsx"
    clean_and_merge_files(input_files, final_output, no_class_output, with_class_output)

    print("Pipeline execution completed successfully!")

if __name__ == "__main__":
    main()

