import pandas as pd
import json
import os

def create_excel_file(input_file1, input_file2, output_filename="output.xlsx"):
    column_mapping = {
        "Product": "product",
        "Category": "categoryTitle",
        "Subcategory": "subcategoryTitle",
        "Title": "videoTitle",
        "Video ID": "videoID",
        "Cadmore Alternate ID": "cadmoreAlternateID",
        "Cadmore Alternate ID": "Id"
    }

    df1 = pd.read_excel(input_file1, engine="openpyxl")
    df1 = df1.rename(columns=column_mapping)
    df1 = df1[list(column_mapping.values())]

    df2 = pd.read_excel(input_file2, engine="openpyxl")
    additional_columns_mapping = {
        "Dev Video ID": "videoID",
        "Update Status": "updatedStatus",
        "URL": "videoUrl"
    }
    df2 = df2.rename(columns=additional_columns_mapping)
    df2 = df2[list(additional_columns_mapping.values())]

    df2 = df2[df2["videoID"].isin(df1["videoID"])]
    combined_df = df1.merge(df2, on="videoID", how="left")
    combined_df.to_excel(output_filename, index=False, engine="openpyxl")
    print(f"Excel file '{output_filename}' created successfully.")

def map_transcripts_to_excel(excel_file, json_file, output_file="updated_excel.xlsx"):
    df = pd.read_excel(excel_file)
    with open(json_file, "r", encoding="utf-8") as f:
        json_data = json.load(f)
    id_transcript_mapping = {item["Id"]: item["Transcript"] for item in json_data}
    df["Transcript"] = df["Id"].map(id_transcript_mapping)
    df.to_excel(output_file, index=False)
    print(f"Updated Excel file saved as {output_file}")

def excel_to_json(file_path, sheet_name=0, output_path="output.json"):
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    data = df.to_dict(orient='records')
    json_data = json.dumps(data, indent=4)
    with open(output_path, "w") as json_file:
        json_file.write(json_data)
    print(f"JSON file saved as {output_path}")
    return json_data

# Example execution
input_file1 = "/content/Boards & Beyond - Endocrinology new video details (1).xlsx"
input_file2 = "/content/Endocrinology Old vs new videos (1).xlsx"
json_file = "/content/BoardsAndBeyondTranscriptExport.json"
output_excel = "new_file.xlsx"
updated_excel = "updated_excel.xlsx"
output_json = "delta_endocrniology_file_output.json"

create_excel_file(input_file1, input_file2, output_excel)
map_transcripts_to_excel(output_excel, json_file, updated_excel)
excel_to_json(updated_excel, output_path=output_json)

