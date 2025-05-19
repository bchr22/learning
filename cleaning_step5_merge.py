import pandas as pd

def clean_and_merge_files(input_files, merged_output="finalmerged_output.xlsx", no_class_output="no_class.xlsx", with_class_output="with_class.xlsx"):
    """
    Cleans and merges input Excel and CSV files by:
    - Separating rows with "No Class" from the "Flashcard Classification" column.
    - Saving separate files for "No Class" and classified rows.
    - Merging all input files into a final cleaned output.

    Parameters:
    input_files (list): List of file paths to process.
    merged_output (str): Path to save the final merged file.
    no_class_output (str): Path to save entries with "No Class".
    with_class_output (str): Path to save entries without "No Class".
    """

    # List to store DataFrames
    dfs = []

    # Read and append each file
    for file in input_files:
        if file.endswith(".xlsx") or file.endswith(".xls"):
            df = pd.read_excel(file)
        elif file.endswith(".csv"):
            df = pd.read_csv(file)
        else:
            print(f"Skipping unsupported file format: {file}")
            continue
        dfs.append(df)

    # Merge all DataFrames
    merged_df = pd.concat(dfs, ignore_index=True)
    print(f"Total records after merging: {len(merged_df)}")

    # Filter "No Class" entries
    df_no_class = merged_df[merged_df["Flashcard Classification"] == "No Class"]
    df_with_class = merged_df[merged_df["Flashcard Classification"] != "No Class"]

    # Save separate files
    df_no_class.to_excel(no_class_output, index=False)
    df_with_class.to_excel(with_class_output, index=False)
    merged_df.to_excel(merged_output, index=False)

    print("Cleaning and merging completed successfully!")

# Example usage
input_files = [
    "/content/merged_output.xlsx",
    "/content/13_1endocrinology_delta_v3_13mar2025final.csv"
]

clean_and_merge_files(input_files)

