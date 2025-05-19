import json
import pandas as pd
import os

def process_data(bb_transcripts_file: str, bb_hierarchical_file: str, anki_excel_file: str, output_dir: str = "product_files2"):
    """
    Processes the input JSON and Excel files, linking relevant data and saving structured JSON files.
    """
    try:
        # Load JSON files
        with open(bb_transcripts_file, 'r') as f:
            bb_transcripts = json.load(f)
        with open(bb_hierarchical_file, 'r') as f:
            bb_hierarchical = json.load(f)
        
        # Load Excel file
        anki_df = pd.read_excel(anki_excel_file)
        
        # Create mappings
        id_to_transcript = {item['Id']: item for item in bb_transcripts}
        cadmore_id_to_data = {}
        for item in bb_hierarchical:
            cadmore_id = item['cadmoreAlternateID']
            cadmore_id_to_data.setdefault(cadmore_id, []).append(item)
        
        # Process and link data
        result = []
        for transcript in bb_transcripts:
            title = transcript['Title']
            cadmore_id = transcript['Id']
            transcript_text = transcript['Transcript']
            
            hierarchical_data_list = cadmore_id_to_data.get(cadmore_id, [])
            
            for hierarchical_data in hierarchical_data_list:
                result_item = {
                    'categoryTitle': hierarchical_data['categoryTitle'],
                    'subcategoryTitle': hierarchical_data['subcategoryTitle'],
                    'videoID': hierarchical_data['videoID'],
                    'cadmoreAlternateID': cadmore_id,
                    'videoTitle': hierarchical_data['videoTitle'],
                    'product': hierarchical_data['product'],
                    'videoUrl': hierarchical_data['videoUrl'],
                    'Transcript': transcript_text,
                    'FA4 Launch URL': None,
                }
                
                # Match product with Exam Type in the Excel file
                for _, row in anki_df.iterrows():
                    if (
                        str(row['Exam Type']).lower() in result_item['product'].lower() and
                        row['BB Video Title'] == result_item['videoTitle']
                    ):
                        result_item['FA4 Launch URL'] = row['FA4 Launch URL']
                        break
                
                result.append(result_item)
        
        # Ensure the output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Organize data by unique products
        product_to_records = {}
        for record in result:
            product = record.get('product')
            if not product:
                continue
            if product not in product_to_records:
                product_to_records[product] = []
            product_to_records[product].append(record)
        
        # Write records to separate JSON files based on product
        for product, records in product_to_records.items():
            safe_product_name = ''.join(e for e in product if e.isalnum() or e in (' ', '_')).replace(' ', '_')
            file_path = os.path.join(output_dir, f"{safe_product_name}.json")
            with open(file_path, 'w') as f:
                json.dump(records, f, indent=4)
        
        return {"status": "success", "message": "JSON files created successfully", "output_directory": output_dir}
    except Exception as e:
        return {"status": "error", "message": str(e)}

