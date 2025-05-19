import pickle
import numpy as np
import pandas as pd
from tqdm import tqdm
from sentence_transformers import SentenceTransformer

def create_flashcard_embeddings(excel_file_path: str, output_pkl_path: str, model_name: str = 'abhinand/MedEmbed-large-v0.1', batch_size: int = 10):
    """
    Creates embeddings for flashcards and saves them to a .pkl file.
    """
    try:
        # Load model
        embedding_model = SentenceTransformer(model_name)
        
        # Load Excel file
        flashcards_df = pd.read_excel(excel_file_path, sheet_name=1)
        
        # Prepare data
        flashcards_df['Flashcard ID'] = flashcards_df['id'].astype(str)
        flashcard_ids = flashcards_df['Flashcard ID'].tolist()
        flashcards = flashcards_df['text_and_extra'].tolist()
        
        # Generate embeddings
        flashcard_embeddings = []
        for i in tqdm(range(0, len(flashcards), batch_size), desc='Generating Flashcard Embeddings'):
            batch_flashcards = flashcards[i:i+batch_size]
            batch_embeddings = embedding_model.encode(batch_flashcards)
            flashcard_embeddings.extend(batch_embeddings)
        
        flashcard_embeddings_np = np.array(flashcard_embeddings)
        
        # Save embeddings to a pickle file
        embeddings_data = {
            'flashcard_ids': flashcard_ids,
            'flashcards': flashcards,
            'flashcard_embeddings': flashcard_embeddings_np
        }
        
        with open(output_pkl_path, 'wb') as f:
            pickle.dump(embeddings_data, f)
        
        return {"status": "success", "message": "Embeddings saved successfully", "output_file": output_pkl_path}
    except Exception as e:
        return {"status": "error", "message": str(e)}

