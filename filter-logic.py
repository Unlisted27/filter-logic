#Created by: Unlisted_dev
import os,hashlib,boto3,magic
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
#Dev peramaters
dev = input("DEV MODE: ").lower().strip()
if dev == "y":
    devmode = True
else:
    devmode = False
    directory = input("FILE PATH: ")
#Colour vars
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
RESET = "\033[0m"
#minio variables
if devmode:
    MINIO_URL = "null"
    ACCESS_KEY = "null"
    SECRET_KEY = "null"
    BUCKET_NAME = "null"
else:
    MINIO_URL = input("minio url: ")
    ACCESS_KEY = input("access key: ")
    SECRET_KEY = input("secret key: ")
    BUCKET_NAME = input("bucket name: ")
#MinIO functions

def config_minio_client():
    print("CONFIGURING MINIO CLIENT")
    try:
        client = boto3.client(
            's3',
            endpoint_url = MINIO_URL,
            aws_access_key_id = ACCESS_KEY,
            aws_secret_access_key = SECRET_KEY
        )
        return(client)
    except Exception as e:
        print(f"Error configuring MinIO client: {e}")
        return False

def check_bucket(client,bucket_name):#Checks if the provided bucket exists
    print("CHECKING FOR BUCKET")
    try:
        if bucket_name not in [bucket["Name"] for bucket in client.list_buckets()["Buckets"]]:
            answer = input(f"Bucket {bucket_name} does not exist. Would you like to create it? [y/n]: ").lower().split()
            if answer == "y":
                client.create_bucket(Bucket=bucket_name)
                print(f"Bucket created: {bucket_name}")
                return True
            else:
                print("Ok bye")
                return False
        else:
            print(f"Bucket exists!")
            return True
    except Exception as e:
        print(f"Error checking the bucket's existence: {e}")
        return False

def upload_to_minio(client, file_path, bucket_name):
    try:
        #get the file name
        file_name = file_path.split("/")[-1]
        client.upload_file(file_path,bucket_name,file_name)
        print(f"File uploaded to MinIO: {bucket_name}/{file_name}")
        return f"{bucket_name}/{file_name}"
    except FileNotFoundError:
        print("File not found.")
        return None
    except NoCredentialsError:
        print("Credentials not available.")
        return None
    except Exception as e:
        print(f"Error uploading file: {e}")
        if devmode:
            print("!!!DEVMODE EXPECTED FAILURE!!!")
        return None

#All the rest
def get_filetype(path):
    try:
        mime = magic.Magic(mime=True) #Get a magic object
        file_type = mime.from_file(path) #Get filetype
        if file_type == "text/plain":
            file_type = "text/txt"
        return file_type
    except Exception as e:
        print(f"{RED}!Cannot detect filetype:{RESET}{e}")
        return None

def discover_files(directory): #This is the file discovery step. It is a generator so will only execute it's contents when iterated through, example: in a for loop
    for entry in os.scandir(directory):
        #print("TRYING") #bug testing line to check for execution of the following code.
        if entry.is_file():
            yield entry
        else:
            #print("==NOT CODED TO HANDLE NON FILES==") #debugging line, can be un commented, but will take up some room in the termianl
            try: #Error handling for incompatible files.
                print(f"NON FILE: {entry.path}")
            except Exception as e: 
                print(f"Error displaying problematic entry: {e}")

def hashfile(path): #Calculate and return SHA256 and SHA1 hashes for a given file
    sha256_hash = hashlib.sha256()
    sha1_hash = hashlib.sha1()
    try:
        with open(path,"rb") as file: #Opens the file in binary mode, We will read file in chunks to save memory
            for chunk in iter(lambda: file.read(4096), b""): #Iterate through the file in 4KB (kilobyte, 4096 Bytes) chunks, only stops when an empty byte string (b"") is reached, aka end of the file. 
                #We use update() to avoid calling the entire file into memory and instead iterate through it chunk by chunk, updating the hash as we go.
                sha256_hash.update(chunk) #Takes the current chunk and adds it to the ongoing SHA256 hash calculation
                sha1_hash.update(chunk) #Takes the current chunk and adds it to the ongoing SHA1 hash calculation
        return sha256_hash.hexdigest(), sha1_hash.hexdigest()
    except Exception as e:
        print(f"ERROR WHILE HASHING FILE! {path} : {e}")

def rename(file_path):
    try:
        directory, file_name = os.path.split(file_path) #Directory is the directory of the file, excluding the file itself. file_name is the name of the file with extension
        name, dummy = os.path.splitext(file_name) #name is the name of the file without extension. extension is the file extension if it exists.
        extension = get_filetype(file_path).split("/")[1]
        new_file_name = f"{hashfile(file_path)[0]}.{extension}" #renames the file
        new_path = os.path.join(directory, new_file_name)
        os.rename(file_path,new_path)
        if devmode:
            print(f"DEVTESTER: NEW NAME OF FILE: {new_file_name}") #dev testing line
            print(f"DEVTESTER: Renamed to: {new_file_name}")
    except AttributeError as e:
        print(f"ERROR DETECTING FILE TYPE DURING RENAMING PROCESS: {e}")
    except Exception as e:
        print(f"ERROR RENAMING FILE: {e}")

#Main loop
client = config_minio_client()
if not client:
    if devmode:
        print(f"{YELLOW}!!!DEVMODE EXPECTED FAILURE!!!{RESET}")
    else:
        exit("Exit: client could not be established")
if not check_bucket(client,BUCKET_NAME): #Check for the buckets existence
    if devmode:
        print(f"{YELLOW}!!!DEVMODE EXPECTED FAILURE!!!{RESET}")
    else:
        exit("Exit: no bucket found")
for file in discover_files(directory): #Iterate through the discover_files generator function.
    print(f"hashing: {file.name}:")
    print(f"filetype: {get_filetype(file.path)}")
    print(rename(file.path))
    upload_to_minio(client,file.path,BUCKET_NAME)
    #NOTE FOR FUTURE DEV: DATABASE CHECK GOES HERE!!!
