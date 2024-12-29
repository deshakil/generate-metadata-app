
import json
import openai
import os
import pytesseract
from PIL import Image
import textract
from PyPDF2 import PdfReader
from pptx import Presentation
from docx import Document
from textract import process
from flask import Flask, request, jsonify
from io import BytesIO
from azure.storage.blob import BlobServiceClient, ContentSettings

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True  # Format JSON response nicely
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 16 MB limit for content size

AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
CONTAINER_NAME = 'documents'

blob_service_client = BlobServiceClient.from_connection_string(os.getenv('AZURE_STORAGE_CONNECTION_STRING'))
container_client = blob_service_client.get_container_client(CONTAINER_NAME)


# Set your OpenAI API key
openai.api_key = os.getenv('OPENAI_API_KEY')

# File identification based on extension
image_extensions = (
    '.png', '.jpg', '.jpeg', '.bmp', '.tiff', '.gif', '.svg', '.webp', 
    '.heic', '.ico', '.psd', '.eps', '.raw', '.ai'
)
document_extensions = (
    '.pdf', '.docx', '.doc', '.txt', '.rtf', '.odt', '.xlsx', '.xls', 
    '.pptx', '.ppt', '.csv', '.epub', '.mobi', '.html', '.md', '.tex', '.xml'
)
coding_extensions = (
    '.py', '.c', '.cpp', '.java', '.js', '.ts', '.go', '.swift', '.rb', '.r', 
    '.php', '.cs', '.kotlin', '.scala', '.rs', '.dart', '.m', '.h', '.pl', 
    '.vb', '.lua', '.asm', '.sh', '.bat', '.sql', '.ipynb'
)

def generate_and_save_metadata(metadata,file_name, user_id):
      # Replace with actual metadata generation logic
    metadata_blob_client = container_client.get_blob_client(f"{user_id}/{file_name}.json")
    metadata_blob_client.upload_blob(
        data=json.dumps(metadata),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json")
    )



def read_blob_to_memory(container_name, blob_name):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    stream = BytesIO()
    blob_client.download_blob().readinto(stream)
    stream.seek(0)  # Reset the stream position to the start
    return stream

# 1. Detect file type based on extension
def get_file_type(file_name):
    if file_name.endswith(image_extensions):
        return "image"
    elif file_name.endswith(document_extensions):
        return "document"
    elif file_name.endswith(coding_extensions):
        return "code"
    else:
        return "others"

# 2. Text extraction logic based on file type
def extract_text(file_stream, file_type):
     file_stream.seek(0)
     if file_type == "image":
        return extract_text_from_image(file_stream)
     elif file_type == "document":
        return extract_text_from_document(file_stream)
     elif file_type == "code":
        return extract_text_from_code(file_stream)
     else:
        return None

# OCR for images
def extract_text_from_image(image_stream):
    img = Image.open(image_stream)
    return pytesseract.image_to_string(img)

# Textract for documents
def extract_text_from_document(doc_stream):
    return textract.process(doc_stream).decode('utf-8')

# Direct read for coding files
def extract_text_from_code(code_stream):
     return code_stream.read().decode('utf-8')

# 3. Summarization/Code analysis logic
def process_text_for_summarization_or_analysis(file_type, file_stream):
    if file_type == "code":
        return analyze_code(file_stream)
    else:
        return summarize_text(file_stream)


def summarize_text(file_stream):
    messages = [
        {"role": "user", "content": f"Summarize the following text in 20 words:\n\n{file_stream[:3000]}"}
    ]
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=messages,
        max_tokens=100
    )
    return response['choices'][0]['message']['content'].strip()

def analyze_code(file_stream):
    messages = [
        {"role": "user", "content": f"Tell me for what purpose this code is meant for, give directly the purpose only (in 20 words):\n\n{file_stream}"}
    ]
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=messages,
        max_tokens=100
    )
    return response['choices'][0]['message']['content'].strip()

# 4. Extract IDs and classify document as "Normal" or "Receipt/Invoice"
"""""
def extract_ids_and_classify(data):
     messages = [
         {"role": "user", "content": f"Find all the IDs (e.g., transaction id, customer id, etc.) from the text:\n\n{data}"}
     ]
     response = openai.ChatCompletion.create(
         model="gpt-3.5-turbo",
         messages=messages,
         max_tokens=150
    )
     ids = response['choices'][0]['message']['content'].strip().splitlines()
     ids = [id.strip() for id in ids if id]
     document_type = "Receipt/Invoice" if len(ids) > 1 else "Normal"
     return {
        "ids": ids,
        "document_type": document_type
    }
"""
def extract_ids_and_classify(file_stream):
    messages = [
        {"role": "user", "content": f"""Your work is to find out the necessary ids present in the text it can be transaction id, customer id, receipt id, GSTIN id based on the text. 
         Hence retrieve the ids from the text ans just output those id present in the text and nothing else. (ignore the address and phone no. also addresses) format of output 
         e.g Receipt ID:'[Receipt ID], you need to output only the ids that are present in the text, you should be wise enough to differentiate between a receipt/invoice type of text 
         and a normal text.
         :\n\n{file_stream[:3000]}"""}
    ]
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=messages,
        max_tokens=200
    )
    
    # Response might include ID names and values in a list or formatted way
    ids_info = response['choices'][0]['message']['content'].strip().splitlines()
    
    ids = {}
    for i, line in enumerate(ids_info):
        if ": " in line:
            key, value = line.split(": ")
            ids[key.strip()] = value.strip()
    
    # Determine if it's a normal document or an invoice/receipt based on number of IDs
    document_type = "Receipt/Invoice" if len(ids) > 1 else "Normal"
    
    return {
        "ids": ids,
        "document_type": document_type
    }



# 5. Topic extraction
def extract_single_topic(file_stream):
    prompt = f"""
    Analyze the following text and accurately identify the primary subject being discussed. 
    Focus only on the central idea of the entire text, ignoring any minor details. 
    Specifically, return:
    - Topic name: A concise phrase that captures the main topic of the text.
    - Top words: The most significant and relevant words associated with the main topic.[let this be a list]
    
    Ensure that the response reflects the actual subject and key terms present in the text. 

    Text:
    {file_stream[:2000]}

    Format the result as a valid JSON object (don't include escape characters).
    """
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=150
    )
    return response['choices'][0]['message']['content'].strip()

# 6. Generate contextual tags
def generate_contextual_tags(file_stream):
    prompt = f"""
    Analyze the following text and provide a list of concise contextual tags (in single words or short phrases)
    that represent the main themes and key points. Do not provide long descriptions or explanations—just the tags.(just include top 5)
    
    Text:
    {file_stream[:2000]}
    
    Return the tags as a comma-separated list without any additional formatting or descriptions.
    """
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=100
    )
    tags = response['choices'][0]['message']['content'].strip().split(",")
    return [tag.strip() for tag in tags]

# 7. Check document importance
def check_document_importance(file_stream):
    prompt = f"""
    is this important document, if the document contains some deadlines or 
    important message  or important information about something upcoming consider it as important. 
    Answer the importance in "YES" or "NO". Assume you are a working professional or student, and 
    tell me whether this document is important or not. Answer in only "YES" or "NO". Perform your 
    job very precisely and accurately
    
    Text:
    {file_stream[:3000]}
    """
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=10
    )
    return response['choices'][0]['message']['content'].strip()



def get_file_extension(file_name):
    file_extension = os.path.splitext(file_name)[1].lower()
    return file_extension.lstrip('.')

def get_file_size_in_mb(file_stream):
     file_stream.seek(0, os.SEEK_END)  # Seek to end of stream to get size
     file_size_bytes = file_stream.tell()
     file_stream.seek(0)  # Reset to start for further processing
    
    # Size conversion logic
     if file_size_bytes >= 1073741824:
        return f"{file_size_bytes / 1073741824:.2f} GB"
     elif file_size_bytes >= 1048576:
        return f"{file_size_bytes / 1048576:.2f} MB"
     elif file_size_bytes >= 1024:
        return f"{file_size_bytes / 1024:.2f} KB"
     return f"{file_size_bytes} Bytes"




def get_number_of_pages(file_stream):
    # Get file extension
    file_extension = get_file_extension(file_stream)
    
    # Check for different document types and count the number of pages
    if file_extension == 'pdf':
        # For PDF files, use PyPDF2 to count pages
        with open(file_stream, 'rb') as file:
            reader = PdfReader(file)
            return len(reader.pages)

    elif file_extension in ['docx', 'doc']:
        # For DOCX or DOC files, use python-docx to get page count (approximated by number of paragraphs)
        doc = Document(file_stream)
        return len(doc.paragraphs) // 50  # Approximate 50 paragraphs per page

    elif file_extension == 'pptx':
        # For PowerPoint presentations, count the number of slides (1 slide = 1 page)
        presentation = Presentation(file_stream)
        return len(presentation.slides)

    elif file_extension in ['txt']:
        # For text files, assume 1 page
        return 1
    
    elif file_extension in ['rtf']:
        # For RTF (Rich Text Format) files, count by text length
        text = process(file_stream).decode('utf-8')
        return len(text) // 3000  # Approximate based on character length per page

    else:
        return None  # No pages for unsupported file types or non-document files



# 8. Generate document title
def generate_document_title(file_stream):
    prompt = f"""
    Based on the content provided below, generate a concise, meaningful, and relevant title that reflects the essence of the document.
    
    Text:
    {file_stream[:2000]}  # Only include the first 1000 characters for efficiency
    """
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=50
    )
    return response['choices'][0]['message']['content'].strip()


def getFileName(file_path):
    return os.path.basename(file_path)


# 9. Metadata generation based on document type
def generate_metadata(file_name, file_path, data, file_stream):
    file_type = get_file_type(file_name)
    
    #document_size = os.path.getsize(file_stream)
    
    if file_type == "document" or file_type=="image":
        ids_info = extract_ids_and_classify(data)
        
        metadata = {
            "file_path":file_path,
            "default_file_name":file_name , #getFileName(file_path),
            "document_title": generate_document_title(data),
            "file_type": get_file_extension(file_name),
            "document_size": get_file_size_in_mb(file_stream),
            "number_of_pages": f"{get_number_of_pages(data)}, Page",
            "data_summary": summarize_text(data),
            "topics": extract_single_topic(data),
            "contextual_tags": generate_contextual_tags(data),
            "importance": check_document_importance(data)
        }
        
        # Add dynamically extracted IDs to the metadata
        if ids_info["document_type"] == "Receipt/Invoice":
            for id_name, id_value in ids_info["ids"].items():
                metadata[id_name] = id_value
        
        return metadata
    
    elif file_type == "code":
        return {
            "file_path":file_path,
            "default_file_name" : file_name, #getFileName(file_path),
            "document_title": generate_document_title(data),
            "file_type": get_file_extension(data),
            "document_size": f"{get_file_size_in_mb(data)}",
            "data_summary": analyze_code(data),
            "topics": extract_single_topic(data),
            "contextual_tags": generate_contextual_tags(data),
            "importance": check_document_importance(data)
        }
    else:
        return None


@app.route('/generate-metadata', methods=['POST'])
def generate_metadata_endpoint():
    data = request.get_json()
    user_id = data.get('userID')
    file_name = data.get('fileName')
    file_path=data.get('filePath')
    if not user_id or not file_name:
        return jsonify({'error': 'userID and fileName are required'}), 400
    blob_name=f"{user_id}/{file_name}.json"
    file_stream = read_blob_to_memory(CONTAINER_NAME, blob_name)
    file_type = get_file_type(file_name)
    extracted_text = extract_text(file_stream, file_type)
    metadata = generate_metadata(file_name, file_path, extracted_text, file_stream)
    generate_and_save_metadata(metadata, file_name, user_id)
    return jsonify(metadata)


# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True, port=5000)
