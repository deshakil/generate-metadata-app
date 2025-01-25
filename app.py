import json
import openai
import os
import pytesseract
from PIL import Image
#import textract
from PyPDF2 import PdfReader
from pptx import Presentation
from docx import Document
#from textract import process
from flask import Flask, request, jsonify
from io import BytesIO
from azure.storage.blob import BlobServiceClient, ContentSettings
#import io
#import tempfile
import openpyxl

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True  # Format JSON response nicely
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 16 MB limit for content size

AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING_1')
CONTAINER_NAME = 'weez-user-data'
AZURE_METADATA_STORAGE_CONNECTION_STRING = os.getenv('AZURE_METADATA_STORAGE_CONNECTION_STRING')
METADATA_CONTAINER_NAME = 'weez-files-metadata'

blob_service_client = BlobServiceClient.from_connection_string(os.getenv('AZURE_STORAGE_CONNECTION_STRING_1'))
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

metadata_blob_service_client = BlobServiceClient.from_connection_string(os.getenv('AZURE_METADATA_STORAGE_CONNECTION_STRING'))
metadata_container_client = metadata_blob_service_client.get_container_client(METADATA_CONTAINER_NAME)

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


def generate_and_save_metadata(metadata, file_name, user_id):
    # Replace with actual metadata generation logic
    metadata_blob_client = metadata_container_client.get_blob_client(f"{user_id}/{file_name}.json")
    if not metadata_blob_client.exists():
        # If it doesn't exist, upload the metadata and create the blob
        metadata_blob_client.upload_blob(
            data=json.dumps(metadata),
            overwrite=False,  # Prevents overwriting in case of race conditions
            content_settings=ContentSettings(content_type="application/json")
        )
    else:
        print(f"Blob {user_id}/{file_name}.json already exists. Metadata not uploaded.")
        return
    original_blob_client = metadata_blob_service_client.get_blob_client(blob=f"{user_id}/{file_name}",container="weez-user-data")

    # Delete the original file
    try:
        original_blob_client.delete_blob()
        print(f"Original file {file_name} deleted successfully from the weez-user-file container.")
    except Exception as e:
        print(f"Failed to delete the original file {file_name}: {e}")


def read_blob_to_memory(container_name, blob_name):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    stream = BytesIO()
    blob_client.download_blob().readinto(stream)
    stream.seek(0)  # Reset the stream position to the start
    return stream


# 1. Detect file type based on extension
def get_file_type(file_name):
    """if file_name.endswith(image_extensions):
        return "image"
    elif file_name.endswith(document_extensions):
        return "document"
    elif file_name.endswith(coding_extensions):
        return "code"
    else:
        return "others"""
    file_name = file_name.lower()

    # Check file type based on extensions
    if file_name.endswith(image_extensions):
        return "image"
    elif file_name.endswith(document_extensions):
        # Return the specific document type with the leading dot
        return next((ext for ext in document_extensions if file_name.endswith(ext)), "others")
    elif file_name.endswith(coding_extensions):
        # Return the specific code file type with the leading dot
        return next((ext for ext in coding_extensions if file_name.endswith(ext)), "others")
    else:
        return "others"


# 2. Text extraction logic based on file type
def extract_text(file_stream, file_type):
    file_stream.seek(0)
    if file_type in image_extensions:
        return extract_text_from_image(file_stream)
    elif file_type in document_extensions:
        return extract_text_from_document(file_stream, file_type)
    elif file_type in coding_extensions:
        return extract_text_from_code(file_stream)
    else:
        return None


# OCR for images
def extract_text_from_image(image_stream):
    img = Image.open(image_stream)
    return pytesseract.image_to_string(img)


# Textract for documents
#def extract_text_from_document(doc_stream):"""
   #"""with tempfile.NamedTemporaryFile(delete=True) as temp_file:
        # Write the file stream content to the temp file
        #temp_file.write(doc_stream.read())
        #temp_file.flush()
    #return textract.process(doc_stream).decode('utf-8')""
    #with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        # Write the content of the BytesIO stream to the temp file
        #temp_file.write(doc_stream.read())
        #temp_file_path = temp_file.name  # Get the path of the temporary file

    #try:
        # Pass the temporary file path to textract
        #file_bytes = doc_stream.read()
        #text = textract.process(io.BytesIO(file_bytes), encoding='utf-8').decode('utf-8') #first parameter was temp_file_path
    #except UnicodeDecodeError:
        #try:
            #file_bytes = doc_stream.read()
            # If UTF-8 fails, fall back to ISO-8859-1 encoding
            #text = textract.process(io.BytesIO(file_bytes), encoding='ISO-8859-1').decode('ISO-8859-1') #here also see just above
        #except UnicodeDecodeError:
            #try:
                #file_bytes = doc_stream.read()
                # If ISO-8859-1 fails, fall back to cp1252 encoding
                #text = textract.process(io.BytesIO(file_bytes), encoding='cp1252').decode('cp1252') # here also the same see above
            #except UnicodeDecodeError:
                # If all fallback encodings fail, handle the error appropriately (e.g., log, empty string)
                #print("Error: Unable to decode text with common encodings.")
                #text = ""  # Return an empty string if all decoding attempts fail

    #finally:
        # Ensure the temporary file is deleted after processing
        #os.remove(temp_file_path)

    #return text
    #"""
   #"""
   #try:
        # Read the content from the BytesIO stream
        #file_bytes = doc_stream.read()

        # Create a temporary file to write the file content
       # with tempfile.NamedTemporaryFile(delete=False) as temp_file:
       #    temp_file.write(file_bytes)
       #     temp_file.flush()  # Ensure that the content is written to disk
      #      temp_file_path = temp_file.name  # Get the temporary file path

        # Pass the temporary file path to textract for text extraction
     #   text = textract.process(temp_file_path, encoding='utf-8').decode('utf-8')

        # Clean up the temporary file
    #    os.remove(temp_file_path)

   # except UnicodeDecodeError:
   #     try:
   #         # If UTF-8 fails, fall back to ISO-8859-1 encoding
   #         text = textract.process(temp_file_path, encoding='ISO-8859-1').decode('ISO-8859-1')
   #     except UnicodeDecodeError:
  #          try:
 #               # If ISO-8859-1 fails, fall back to cp1252 encoding
 #               text = textract.process(temp_file_path, encoding='cp1252').decode('cp1252')
#            except UnicodeDecodeError:
#                print("Error: Unable to decode text with common encodings.")
#                text = ""  # Return an empty string if all decoding attempts fail
#
#    except Exception as e:
#        # Handle any unexpected exceptions
 #       print(f"Error processing document: {e}")
#        text = ""  # Return an empty string in case of error

    #return text"""
def extract_text_from_document(doc_stream, file_type):
  text = ""
  file_type = file_type.lower()

  try:
    if file_type == ".pdf":
        # Extract text from PDF
        reader = PdfReader(doc_stream)
        for page in reader.pages:
            text += page.extract_text() or ""

    elif file_type == ".docx":
        # Extract text from Word document
        doc = Document(doc_stream)
        for paragraph in doc.paragraphs:
            text += paragraph.text + "\n"

    elif file_type == ".pptx":
        # Extract text from PowerPoint presentation
        presentation = Presentation(doc_stream)
        for slide in presentation.slides:
            for shape in slide.shapes:
                if shape.has_text_frame:
                    for paragraph in shape.text_frame.paragraphs:
                        for run in paragraph.runs:
                            text += run.text
                        text += "\n"

    elif file_type == "xlsx":
        # Extract text from Excel spreadsheet
        workbook = openpyxl.load_workbook(doc_stream)
        for sheet in workbook.sheetnames:
            sheet_obj = workbook[sheet]
            text += f"\n--- Sheet: {sheet} ---\n"
            for row in sheet_obj.iter_rows(values_only=True):
                row_text = " ".join([str(cell) if cell is not None else "" for cell in row])
                text += row_text + "\n"

    else:
        raise ValueError("Unsupported file type: " + file_type)

  except Exception as e:
    return f"Error processing the file: {e}"

  return text





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
        {"role": "user", "content": f"""Summarize the main content of the following document in a precise and concise manner, focusing only on the core details. 
    Ensure the summary is structured in a single paragraph and is useful for metadata generation.\n\n{file_stream[:5000]}
    Format the summary as a single sentence of no more than 20 words.
    """}
    ]
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=messages,
        max_tokens=100
    )
    return response['choices'][0]['message']['content'].strip()


def analyze_code(file_stream):
    messages = [
        {"role": "user",
         "content": f"Tell me for what purpose this code is meant for, give directly the purpose only (in 20 words):\n\n{file_stream}"}
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
    Analyze the following text and identify 3-4 key sub-topics that summarize the content of the document. 
    Focus on extracting meaningful, specific, and relevant topics or ideas discussed in the text. Avoid generic or overly broad terms and be consistent and specific to the text.

    Your output should be a valid JSON object in the following structure:
    {{
      "sub_topics": ["Topic 1", "Topic 2", "Topic 3", "Topic 4"]
    }}

    Examples:
    - For a resume: 
      {{
        "sub_topics": ["Cloud Computing", "Full-Stack Development", "AWS Expertise", "Leadership"]
      }}
    - For a project report:
      {{
        "sub_topics": ["Algorithms", "Sorting Techniques", "Graph Theory", "Tree Structures"]
      }}
    - For a presentation:
      {{
        "sub_topics": ["Digital Transformation", "Key Performance Indicators", "Employee Engagement", "Future Strategies"]
      }}
    - For an invoice:
      {{
        "sub_topics": ["Payment Details", "Software Development Services", "Invoice #12345", "January 2025"]
      }}

    Text:
    {file_stream[:5000]}

    Provide only the JSON object as output with no additional explanations or text.
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
    Analyze the following document and determine whether it contains critical information such as deadlines, important messages, 
    or key updates. Consider the perspective of a working professional or college student or school student. 

    Return your response as "YES" (important) or "NO" (not important).

    Text:
    {file_stream[:5000]}
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

"""
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
"""
def get_number_of_pages(file_stream):
    # Get file extension
    file_extension = get_file_extension(file_stream)

    # Check for different document types and count the number of pages
    if file_extension == 'pdf':
        # For PDF files, use PyPDF2 to count pages
        file_stream.seek(0)  # Reset the file stream position to the start
        reader = PdfReader(file_stream)
        return len(reader.pages)

    elif file_extension in ['docx', 'doc']:
        # For DOCX or DOC files, use python-docx to get page count (approximated by number of paragraphs)
        file_stream.seek(0)  # Reset the file stream position to the start
        doc = Document(file_stream)
        return len(doc.paragraphs) // 50  # Approximate 50 paragraphs per page

    elif file_extension == 'pptx':
        # For PowerPoint presentations, count the number of slides (1 slide = 1 page)
        file_stream.seek(0)  # Reset the file stream position to the start
        presentation = Presentation(file_stream)
        return len(presentation.slides)

    elif file_extension in ['txt']:
        # For text files, assume 1 page
        file_stream.seek(0)  # Reset the file stream position to the start
        return 1

    elif file_extension in ['rtf']:
        # For RTF (Rich Text Format) files, count by text length
        file_stream.seek(0)  # Reset the file stream position to the start
        text = file_stream.read().decode('utf-8')
        return len(text) // 3000  # Approximate based on character length per page

    else:
        return None

# 8. Generate document title
def generate_document_title(file_stream):
    prompt = f"""
     Analyze the following text and identify the primary topic of the document. Focus on determining:
    - The nature of the document (e.g., Resume, Invoice, Project Report, Research Paper, etc.).
    - The associated person, company, or entity (if applicable).
    - The subject or key purpose of the document.

    Return a **single descriptive sentence** combining these elements to serve as a new, meaningful file name. For example:
    - If it’s Harshith's report on Data Structures, return: "Data Structures Report of Harshith".
    - If it’s a resume for Shokat Ahmed, return: "Resume of Shokat Ahmed".
    - If it’s an invoice for Acme Corp, return: "Invoice for Acme Corp".
    - If it’s a generic research paper, return: "Research Paper on Climate Change".

    Text:
    {file_stream[:5000]}

    Provide only the single descriptive sentence as output, with no additional text or formatting.
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

    # document_size = os.path.getsize(file_stream)

    if file_type in document_extensions or file_type in image_extensions:
        ids_info = extract_ids_and_classify(data)

        metadata = {
            "file_path": file_path,
            "default_file_name": file_name,  # getFileName(file_path),
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
        """if ids_info["document_type"] == "Receipt/Invoice":
            for id_name, id_value in ids_info["ids"].items():
                metadata[id_name] = id_value"""

        return metadata

    elif file_type in coding_extensions:
        return {
            "file_path": file_path,
            "default_file_name": file_name,  # getFileName(file_path),
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


def check_metadata_if_exists(user_id, file_name):
    blob_client = metadata_container_client.get_blob_client(f"{user_id}/{file_name}.json")
    return blob_client.exists()


@app.route('/generate-metadata', methods=['POST'])
def generate_metadata_endpoint():
    data = request.get_json()
    user_id = data.get('userID')
    file_name = data.get('fileName')
    file_path = data.get('filePath')
    if not user_id or not file_name:
        return jsonify({'error': 'userID and fileName are required'}), 400
    exists = check_metadata_if_exists(file_name, user_id)
    if exists:
        return jsonify({'error': 'Metadata Already Exists'}), 400
    blob_name = f"{user_id}/{file_name}"
    file_stream = read_blob_to_memory(CONTAINER_NAME, blob_name)
    print("File Read Done")
    file_type = get_file_type(file_name)
    print("Got the file type")
    extracted_text = extract_text(file_stream, file_type)
    print("Extracted the Data")
    metadata = generate_metadata(file_name, file_path, extracted_text, file_stream)
    print("Generated the Metadata")
    generate_and_save_metadata(metadata, file_name, user_id)
    print("Saved the Metadata also.. All done")
    return jsonify(metadata)


print(generate_metadata_endpoint)
# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True, port=5000)
