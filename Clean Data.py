import pandas as pd
import re

# Load the CSV file
file_path = '/Users/skemprecos.22/Desktop/FUF Internship/Tableau/FUS-IO PRIVATE.csv'
df = pd.read_csv(file_path)

# Keywords to search for
keywords = [
    "FUS-Thermal Ablation", 
    "High Intensity Focused Ultrasound",
    "HIFU",
    "Focused Ultrasound Thermal Ablation", 
    "Mechanical -FUS", 
    "Mechanical Focused Ultrasound", 
    "Histotripsy", 
    "Pulsed-FUS", 
    "Pulsed Focused Ultrasound", 
    "FUS-Hyperthermia", 
    "Focused Ultrasound Hyperthermia", 
    "LOFU", 
    "MB", 
    "SDT", 
    "FUS-BBBo",
    "Focused Ultrasound Blood-Brain Barrier Opening",
    "FUS-alone",
    "FUS-Drug",
    "Pulsed",
    "Mechanical",
    "Blood-Brain Barrier",
    "Hyperthermia",
    "Thermal Ablation",
    "Thermal",
    "Ablation",
    "thermalablation"
]

# Normalization mappings
normalization_mappings = {
    "FUS-BBBo": "Blood-Brain Barrier Opening",
    "Blood Brain Barrier": "Blood-Brain Barrier Opening",
    "Focused Ultrasound Blood-Brain Barrier Opening": "Blood-Brain Barrier Opening",
    "FUS-Thermal Ablation": "Thermal Ablation",
    "Thermal Ablation": "Thermal Ablation",
    "Ablation": "Thermal Ablation",
    "Thermal": "Thermal Ablation",
    "Focused Ultrasound Thermal Ablation": "Thermal Ablation",
    "FUS-Hyperthermia": "Hyperthermia",
    "Focused Ultrasound Hyperthermia": "Hyperthermia",
    "Pulsed-FUS": "Pulsed Focused Ultrasound",
    "Pulsed Focused Ultrasound": "Pulsed Focused Ultrasound",
    "Pulsed": "Pulsed Focused Ultrasound",
    "Mechanical -FUS": "Mechanical Focused Ultrasound",
    "Mechanical Focused Ultrasound": "Mechanical Focused Ultrasound",
    "LOFU": "Low Intensity Focused Ultrasound",
    "HIFU": "High Intensity Focused Ultrasound",
    "thermalablation": "Thermal Ablation",
    "MB": "Microbubbles"
}

# Normalize function to handle case insensitivity and remove non-alphanumeric characters except spaces
def normalize(text):
    return re.sub(r'[^a-z0-9\s]', '', text.lower().replace('-', ' ').replace('_', ' ').strip())

# Normalize keywords
normalized_keywords = [normalize(keyword) for keyword in keywords]

# Initialize the 'Modality' column with empty strings
df['Modality'] = ''

# Function to find the keywords in the manual tags
def find_keywords(manual_tags):
    found_keywords = []
    normalized_tags = [normalize(tag) for tag in manual_tags]
    for keyword, normalized_keyword in zip(keywords, normalized_keywords):
        if any(normalized_keyword in tag for tag in normalized_tags):
            found_keywords.append(keyword)
    return ', '.join(found_keywords)

# Apply the function to each row in the 'Manual Tags' column
df['Modality'] = df['Manual Tags'].apply(lambda x: find_keywords(x.split(';')))

# Function to normalize the modality column and remove duplicates
def normalize_modality(modality):
    if modality:
        keywords_list = modality.split(', ')
        normalized_list = list({normalization_mappings.get(keyword, keyword) for keyword in keywords_list})
        return ', '.join(sorted(normalized_list))
    return ''

# Apply the normalization function to the 'Modality' column
df['Modality'] = df['Modality'].apply(normalize_modality)

# Remove empty columns
df = df.dropna(axis=1, how='all')

# Remove duplicates before exploding the dataframe
df['Modality'] = df['Modality'].apply(lambda x: ', '.join(sorted(set(x.split(', ')))))

# Expand the dataframe 
df = df.assign(Modality=df['Modality'].str.split(', ')).explode('Modality')

# Save the modified dataframe to a new CSV file
output_file_path = '/Users/skemprecos.22/Desktop/FUF Internship/Tableau/FUS-IO_expanded.csv'
df.to_csv(output_file_path, index=False)

################################## for the tumor type ################################## 
file_path = '/Users/skemprecos.22/Desktop/FUF Internship/Tableau/FUS-IO_expanded.csv'
df = pd.read_csv(file_path)

# New list of keywords for tumor types
tumor_keywords = [
    "Melanoma", 
    "GBM", 
    "Neuroblastoma", 
    "Sarcoma", 
    "Pancreatic Cancer", 
    "Adenocarcinoma", 
    "Hepatocellular Carcinoma", 
    "Colon Carcinoma", 
    "ColonCarcinoma",
    "Carcinoma",
    "Colon Adenocarcinoma", 
    "Lymphoma", 
    "Uterine Fibroids", 
    "Normal Brain", 
    "Thyroid Nodule", 
    "Renal Cell Carcinoma", 
    "Breast Cancer",
    "Breast", 
    "BreastCancer",
    "Leukemia", 
    "Prostate Cancer", 
    "ColoRectal Cancer", 
    "Multiple Sclerosis", 
    "Autoimmune Encephalomyelitis", 
    "Cholangiocarcinoma", 
    "Osteosarcoma", 
    "Bladder Cancer", 
    "Liver Cancer", 
    "Solid Tumor"
]

normalized_indicators = {
    "Breast": "Breast Cancer",
    "ColonCarcinoma": "Colon Carcinoma",
    "breast": "Breast Cancer",
    "BreastCancer": "Breast Cancer"
}

# Normalize keywords for tumor types
normalized_tumor_keywords = [normalize(key) for key in tumor_keywords]

# Initialize the 'Tumor Type' column with empty strings
df['Tumor Type'] = ''

# Function to find the tumor keywords in the manual tags
def find_tumor_keywords(manual_tags):
    found_keywords = []
    normalized_tags = [normalize(tag) for tag in manual_tags]
    for keyword, normalized_keyword in zip(tumor_keywords, normalized_tumor_keywords):
        if any(normalized_keyword in tag for tag in normalized_tags):
            found_keywords.append(keyword)
    return ', '.join(found_keywords)

# Apply the function to each row in the 'Manual Tags' column
df['Tumor Type'] = df['Manual Tags'].apply(lambda x: find_tumor_keywords(x.split(';')))

# Function to normalize the tumor type column and remove duplicates
def normalize_tumor_type(tumor_type):
    if tumor_type:
        keywords_list = tumor_type.split(', ')
        normalized_list = list({normalized_indicators.get(keyword, keyword) for keyword in keywords_list})
        return ', '.join(sorted(normalized_list))
    return ''

# Apply the normalization function to the 'Tumor Type' column
df['Tumor Type'] = df['Tumor Type'].apply(normalize_tumor_type)

# Remove empty columns
df = df.dropna(axis=1, how='all')

# Remove duplicates before exploding the dataframe
df['Tumor_Type'] = df['Tumor Type'].apply(lambda x: ', '.join(sorted(set(x.split(', ')))))

# Split the 'Tumor Type' column into individual rows
df = df.assign(Tumor_Type=df['Tumor_Type'].str.split(', ')).explode('Tumor_Type')

# Save the expanded dataframe to a new CSV file
final_output_path = '/Users/skemprecos.22/Desktop/FUF Internship/Tableau/merged_data.csv'
df.to_csv(final_output_path, index=False)


####### -------------- Publication Type ---------
file_path = '/Users/skemprecos.22/Desktop/FUF Internship/Tableau/merged_data.csv'
df = pd.read_csv(file_path)

publication_keywords = [
    "Research Article",
    "Review"
]

normalized_pubs = {
    "Research Article": "Research Article",
    "Review": "Review"
}


# Normalize function to handle case insensitivity and remove non-alphanumeric characters except spaces
def normalize(text):
    return re.sub(r'[^a-z0-9\s]', '', text.lower().replace('-', ' ').replace('_', ' ').strip())

# Function to specifically find "Research Article" in the manual tags
def find_research_articles(manual_tags):
    normalized_tags = [normalize(tag) for tag in manual_tags]
    if "researcharticle" in normalized_tags:
        return "Research Article"
    return ""

# Function to find other publication keywords in the manual tags
def find_other_pub_keywords(manual_tags):
    found_keywords = []
    normalized_tags = [normalize(tag) for tag in manual_tags]
    for keyword, normalized_keyword in zip(publication_keywords, [normalize(k) for k in publication_keywords]):
        if any(normalized_keyword in tag for tag in normalized_tags):
            found_keywords.append(keyword)
    return ', '.join(found_keywords)

# Apply the functions to each row in the 'Manual Tags' column
df['Publication Type'] = df['Manual Tags'].apply(lambda x: find_research_articles(x.split(';')))

# Update the 'Publication Type' with other publication keywords if not already "Research Article"
df['Publication Type'] = df.apply(
    lambda row: row['Publication Type'] if row['Publication Type'] == "Research Article" else find_other_pub_keywords(row['Manual Tags'].split(';')),
    axis=1
)

# Debugging: Print rows where publication type is found
print("Debugging: Rows with identified publication types")
print(df[df['Publication Type'] != ''])

# Remove empty columns
df = df.dropna(axis=1, how='all')

# Remove duplicates before exploding the dataframe
df['Publication_Type'] = df['Publication Type'].apply(lambda x: ', '.join(sorted(set(x.split(', ')))))

# Debugging: Check the expanded publication types
print("Debugging: Expanded publication types")
print(df[['Manual Tags', 'Publication Type', 'Publication_Type']].head(10))

# Split the 'Publication Type' column into individual rows
df = df.assign(Publication_Type=df['Publication_Type'].str.split(', ')).explode('Publication_Type')

# Debugging: Check the dataframe after exploding
print("Debugging: Dataframe after exploding")
print(df[['Manual Tags', 'Publication_Type']].head(10))

# Save the expanded dataframe to a new CSV file
final_output_path = '/Users/skemprecos.22/Desktop/FUF Internship/Tableau/pub_process.csv'
df.to_csv(final_output_path, index=False)