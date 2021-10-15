import json
import requests
import hashlib
from pathlib import Path
from defusedxml.lxml import tostring
from lxml.builder import E
import mimetypes


SITE_URL = 'https://library.tind.io/'
API_KEY = ''
OBJECT_STORE_NAME = 'TOS'
CALLBACK_EMAIL = 'demo@tind.io'


APPEND_METADATA = {"245__a": "Test record",
                   "269__a": "2021-10-14",
                   "980__a": "DIGITIZED"}


def request_presigned_object():
    """Returns a presigned object"""
    url = SITE_URL + 'storage/presigned_post?location=' + OBJECT_STORE_NAME
    headers = {'Authorization': 'Token ' + API_KEY}
    r = requests.post(url, headers=headers)
    response_object = json.loads(r.content)
    return response_object


def upload_single_file(file_name, presigned_response):
    """Post a file to S3 and returns the upload response"""
    url = presigned_response["data"]["url"]
    headers = {'x-amz-acl': 'private'}
    data = presigned_response["data"]["fields"]
    files = {'file': open(file_name, 'rb')}
    upload_response = requests.post(url, headers=headers, files=files, data=data)

    print(upload_response.status_code)
    return upload_response


def verify_checsum(file_path, upload_response):
    """
    Comparing the checksum of the local file with the returned checksum/Etag
    stored in the upload response
    """
    local_checksum = hashlib.md5(open(file_path, 'rb').read()).hexdigest()

    etag = upload_response.headers.get('Etag', '')
    upload_checksum = etag.replace('"', '')

    if local_checksum != upload_checksum:
        print('Checksum is not the same! %s' % (file_path,))
        return False
    return local_checksum


def create_subfield(sub_code):
    """
    Create an empty subfield with a subfield code
    """
    new_subfield = E.subfield(code='{}'.format(sub_code))
    return new_subfield


def create_datafield(marc_key, subfield_tuple_lists=False):
    """
    marc_key: Five letter value: E.g. 245__
    The subfield_tuple_list is on the form:
    [('a', 'subfield text A'),
     ('b', 'subfield text B')]
    """
    new_datafield = E.datafield(TAG='{}'.format(marc_key[:3]),
                                ind1='{}'.format(marc_key[3].replace("_", "")),
                                ind2='{}'.format(marc_key[4].replace("_", "")))

    for subfield in subfield_tuple_lists:
        new_subfield = create_subfield(subfield[0])
        new_subfield.text = subfield[1]
        if new_subfield.text:
            new_datafield.append(new_subfield)

    return new_datafield


def create_fft_datafield(presigned_response, local_checksum, file_name):
    """Create the necessary FFT tag to link a file to a record.
    Calculates the mime type on the fly.
    If the mime type is not found, it should be added to a
    list similar to the hocr example.
    """
    mime_type = mimetypes.guess_type(file_name)[0]
    if not mime_type:
        if file_name.endswith('.hocr'):
            mime_type = 'text/vnd.hocr+html'
        else:
            return

    return create_datafield('FFT__',
                            [('a', presigned_response["data"]["fields"]['key']),
                             ('c', local_checksum),
                             ('e', mime_type),
                             ('l', OBJECT_STORE_NAME),
                             ('n', file_name)])


def upload_metadata(string_xml):
    """
    Used the record API to upload metadata together with the FFT tag.
    The FFT tag will link the file to the record.
    """
    url = SITE_URL + 'api/v1/record?&mode=insertorreplace&callback_email=' + CALLBACK_EMAIL
    headers = {'Authorization': 'Token ' + API_KEY,
               'Content-Type': 'application/xml'}

    r = requests.post(url, headers=headers, data=string_xml)

    print("Metadata upload status: %s" % (r.text,))


def upload_and_save_xml(collection):
    """
    Upload the MARCXML with the record API and save a local copy of the file.
    """
    # Convert to string, replace
    string_xml = tostring(collection, pretty_print=True, encoding="unicode")

    # Replace capital TAG to lowercase tag in data fields
    string_xml = string_xml.replace('datafield TAG="', 'datafield tag="')

    string_xml_name = data_folder.parent / 'export_{}.xml'.format(data_folder.name)

    # Save xml file
    with open(string_xml_name, 'w') as xmlfile:
        xmlfile.write(string_xml)

    # Upload xml file
    try:
        upload_metadata(string_xml)
    except UnicodeEncodeError as e:
        print("Cannot import XML file: %s. Error message: \n %s" % (string_xml_name, e))


if __name__ == '__main__':
    # Get the path to the files.
    folder_path = input('Insert folder path for the files to upload:\n')
    folder_path = folder_path.replace('\\', '').strip()
    data_folder = Path(folder_path)
    if not data_folder.is_dir():
        print('The provided path is not a folder. Try again!')
        exit()

    # Create xml record object
    new_record = E.record()

    # Append metadata to record
    for key, val in APPEND_METADATA.items():
        if len(key) == 6:
            new_record.append(
                create_datafield(key[0:5],
                                 [(key[5], val)]))

    # Loop through all files in path, except DS_Store (MacOS specific files).
    for i, file_path in enumerate(sorted(data_folder.rglob('*.[!DS_Store]*'))):

        # Ignore directories
        if file_path.is_dir():
            continue

        # Step 1: Get AWS presigned object
        try:
            presigned_response = request_presigned_object()
        except ConnectionResetError:
            print("Presign - File failed to upload due: %s" % (file_path,))
            presigned_response = None
            continue

        # Step 2: Upload file
        try:
            upload_response = upload_single_file(str(file_path), presigned_response)
        except:
            print("File failed to upload: %s" % (file_path,))
            continue

        # Step 3: Verify checksum
        local_checksum = verify_checsum(str(file_path), upload_response)

        # Step 4: Create FFT datafield and attach to record.
        if local_checksum:
            new_record.append(create_fft_datafield(presigned_response,
                                                   local_checksum,
                                                   file_path.name))

        # print a status per 10 files
        if i + 1 % 10 == 0:
            print('Processed %s files' % (i + 1,))

    # When all the files are uploaded to AWS S3, upload the metadata record to link the files.
    upload_and_save_xml(new_record)
