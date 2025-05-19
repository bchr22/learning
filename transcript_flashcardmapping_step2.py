import os
import json
import nltk
from nltk import sent_tokenize
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm
import numpy as np
import pickle
import torch
from sentence_transformers import SentenceTransformer

nltk.download('punkt')

# Automatically use GPU if available, otherwise fall back to CPU
device = "cuda" if torch.cuda.is_available() else "cpu"

def split_text_into_paragraphs(text, max_paragraph_length=150):
    """
    Split large text into paragraphs with a specified max length.
    """
    sentences = sent_tokenize(text)
    paragraphs = []
    current_paragraph = []
    current_length = 0

    for sentence in sentences:
        sentence_length = len(sentence.split())
        if current_length + sentence_length > max_paragraph_length:
            paragraphs.append(" ".join(current_paragraph))
            current_paragraph = []
            current_length = 0
        current_paragraph.append(sentence)
        current_length += sentence_length
    if current_paragraph:
        paragraphs.append(" ".join(current_paragraph))

    return paragraphs

embedding_model = SentenceTransformer('abhinand/MedEmbed-large-v0.1', device=device)

def process_step2(json_file_path: str, embeddings_pkl_path: str, output_excel_path: str, model_name: str = 'abhinand/MedEmbed-large-v0.1', threshold: float = 0.65):
    """
    Processes the JSON output from step 1 and finds matching flashcards.
    """
    try:
        # Load the embedding model with device selection
        embedding_model = SentenceTransformer(model_name, device=device)
        
        # Load the embeddings and flashcards
        with open(embeddings_pkl_path, 'rb') as f:
            embeddings_data = pickle.load(f)
            flashcard_ids = embeddings_data['flashcard_ids']
            flashcards = embeddings_data['flashcards']
            flashcard_embeddings_np = embeddings_data['flashcard_embeddings']
        
        # Ensure data alignment
        assert len(flashcard_ids) == len(flashcards) == len(flashcard_embeddings_np), "Mismatch in data lengths"
        
        # Load JSON file
        with open(json_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        all_results = []
        
        for entry in tqdm(data, desc="Processing entries"):
            category_title = entry.get("categoryTitle", "")
            subcategory_title = entry.get("subcategoryTitle", "")
            video_id = entry.get("videoID", "")
            cadmore_alternate_id = entry.get("Id", "")
            video_title = entry.get("videoTitle", "Unnamed")
            product = entry.get("product", "")
            updated_status = entry.get("updatedStatus", "")
            video_url = entry.get("videoUrl", "")
            transcript_text = entry.get("Transcript", "")
            
            # Split transcript into chunks
            transcript_chunks = split_text_into_paragraphs(transcript_text, max_paragraph_length=150)
            
            # Embed all transcript chunks at once for speed
            transcript_embeddings_np = embedding_model.encode(transcript_chunks)
            
            # Compute similarity
            similarity_matrix = cosine_similarity(transcript_embeddings_np, flashcard_embeddings_np)
            
            for i, transcript_chunk in enumerate(transcript_chunks):
                scores = similarity_matrix[i]
                indices = np.where(scores >= threshold)[0]
                
                flash_card_matches = [{
                    "Flashcard": flashcards[j],
                    "Flashcard ID": flashcard_ids[j],
                    "Score": round(scores[j], 4),
                } for j in indices]
                
                flash_card_matches = sorted(flash_card_matches, key=lambda x: x["Score"], reverse=True)[:20]
                
                for fc in flash_card_matches:
                    all_results.append({
                        "categoryTitle": category_title,
                        "subcategoryTitle": subcategory_title,
                        "videoID": video_id,
                        "cadmoreAlternateID": cadmore_alternate_id,
                        "videoTitle": video_title,
                        "product": product,
                        "videoUrl": video_url,
                        "Update status": updated_status,
                        "Transcript Chunk": transcript_chunk,
                        "Flashcard": fc["Flashcard"],
                        "Flashcard ID": fc["Flashcard ID"],
                        "Score": fc["Score"],
                    })
        
        df_all = pd.DataFrame(all_results)
        df_all.to_excel(output_excel_path, index=False)
        
        return {"status": "success", "message": "Step 2 processing completed", "output_file": output_excel_path}
    except Exception as e:
        return {"status": "error", "message": str(e)}

