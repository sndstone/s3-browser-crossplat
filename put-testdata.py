import boto3
import concurrent.futures
import os
import uuid
from prettytable import PrettyTable
import json
import logging
from threading import Thread, Lock
from queue import Queue
import argparse
import time
import multiprocessing
from botocore.config import Config
import hashlib
import base64
import zlib

# Setup logging
LOG_FILENAME = 's3_upload.log'
file_logger = logging.getLogger('file_logger')
file_handler = logging.FileHandler(LOG_FILENAME)
file_logger.addHandler(file_handler)

# Console logging
console_handler = logging.StreamHandler()
console_format = logging.Formatter('%(message)s')
console_handler.setFormatter(console_format)
file_logger.addHandler(console_handler)

# Log buffering
log_buffer = []
log_buffer_lock = Lock()
LOG_BUFFER_SIZE = 100

# Function to read credentials from JSON file
def read_credentials_from_json(file_path):
    try:
        with open(file_path, "r") as json_file:
            credentials = json.load(json_file)
            return credentials
    except Exception as e:
        file_logger.error(f'Error reading JSON: {e}')
        return None

# Optimized logging function with buffering
def log_thread(q):
    global log_buffer
    
    while True:
        message = q.get()
        if message is None:
            # Flush any remaining logs
            with log_buffer_lock:
                if log_buffer and args.debug:
                    for msg in log_buffer:
                        file_logger.info(msg)
                    log_buffer = []
            break
            
        # Only print important messages to console
        if "Put object" in message and message.endswith("200"):
            print(message)
        
        # Only log to file if debug is enabled
        if args.debug:
            # Buffer logs to reduce I/O operations
            with log_buffer_lock:
                log_buffer.append(message)
                if len(log_buffer) >= LOG_BUFFER_SIZE:
                    for msg in log_buffer:
                        file_logger.info(msg)
                    log_buffer = []
                
        q.task_done()

# Function to get integer input
def get_integer_input(prompt):
    while True:
        try:
            return int(input(prompt))
        except ValueError:
            print("Please enter a valid integer.")

# Argument parser
parser = argparse.ArgumentParser(description="Upload files to S3")
parser.add_argument("--import_json", help="Path to JSON file for configuration", default=None)
parser.add_argument("--bucket_name", help="Name of the bucket", default=None)
parser.add_argument("--s3_endpoint_url", help="S3 endpoint URL", default=None)
parser.add_argument("--aws_access_key_id", help="AWS access key ID", default=None)
parser.add_argument("--aws_secret_access_key", help="AWS secret access key", default=None)
parser.add_argument("--object_size", help="Size of the objects in bytes", type=int, default=None)
parser.add_argument("--versions", help="Number of versions to be created", type=int, default=None)
parser.add_argument("--objects_count", help="Number of objects to be placed", type=int, default=None)
parser.add_argument("--object_prefix", help="Prefix for the objects", default=None)
parser.add_argument("--threads", help="Number of upload threads", type=int, default=None)
parser.add_argument("--simple_data", help="Use single data object for all uploads", action="store_true")
parser.add_argument("--debug", help="Enable debug logging to file", action="store_true")
parser.add_argument("--checksum", help="Checksum algorithm to use (md5, crc32, crc32c, sha1, sha256)", default="md5")
args = parser.parse_args()

# Configuration parsing
BUCKET_NAME = args.bucket_name
S3_ENDPOINT_URL = args.s3_endpoint_url
AWS_ACCESS_KEY_ID = args.aws_access_key_id
AWS_SECRET_ACCESS_KEY = args.aws_secret_access_key

if args.import_json:
    credentials = read_credentials_from_json(args.import_json)
    if not BUCKET_NAME:
        BUCKET_NAME = credentials.get("bucket_name")
    if not S3_ENDPOINT_URL:
        S3_ENDPOINT_URL = credentials.get("s3_endpoint_url")
    if not AWS_ACCESS_KEY_ID:
        AWS_ACCESS_KEY_ID = credentials.get("aws_access_key_id")
    if not AWS_SECRET_ACCESS_KEY:
        AWS_SECRET_ACCESS_KEY = credentials.get("aws_secret_access_key")
else:
    if not (BUCKET_NAME and S3_ENDPOINT_URL and AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY):
        JSON_IMPORT = input("Do you want to import JSON file for configuration? (yes/no): ")
        if JSON_IMPORT.lower() == "yes":
            JSON_FILE_PATH = input("Enter the JSON file path: ")
            credentials = read_credentials_from_json(JSON_FILE_PATH)
            BUCKET_NAME = credentials["bucket_name"]
            S3_ENDPOINT_URL = credentials["s3_endpoint_url"]
            AWS_ACCESS_KEY_ID = credentials["aws_access_key_id"]
            AWS_SECRET_ACCESS_KEY = credentials["aws_secret_access_key"]
        else:
            BUCKET_NAME = BUCKET_NAME or input("Enter the bucket name: ")
            S3_ENDPOINT_URL = S3_ENDPOINT_URL or input("Enter the S3 endpoint URL, (EXAMPLE http://example.com:443): ")
            AWS_ACCESS_KEY_ID = AWS_ACCESS_KEY_ID or input("Enter the AWS access key ID: ")
            AWS_SECRET_ACCESS_KEY = AWS_SECRET_ACCESS_KEY or input("Enter the AWS secret access key: ")

# Object parameters
OBJECT_SIZE = args.object_size or get_integer_input("Enter the size of the objects in bytes: ")
VERSIONS = args.versions or get_integer_input("Enter the number of versions to be created: ")
OBJECTS_COUNT = args.objects_count or get_integer_input("Enter the number of objects to be placed: ")
OBJECT_PREFIX = args.object_prefix

# Thread count - default to CPU count * 2 for IO-bound tasks if not specified
THREAD_COUNT = args.threads or (multiprocessing.cpu_count() * 2)

# Use simple data option
USE_SIMPLE_DATA = args.simple_data

# Set checksum algorithm
CHECKSUM_ALGORITHM = args.checksum.lower()
valid_checksums = ["md5", "crc32", "crc32c", "sha1", "sha256", "none"]
if CHECKSUM_ALGORITHM not in valid_checksums:
    print(f"Warning: Invalid checksum algorithm '{CHECKSUM_ALGORITHM}'. Defaulting to 'md5'.")
    print(f"Valid options are: {', '.join(valid_checksums)}")
    CHECKSUM_ALGORITHM = "md5"
print(f"Using checksum algorithm: {CHECKSUM_ALGORITHM}")

# Configure logging
LOG_LEVEL = logging.DEBUG if args.debug else logging.INFO
file_logger.setLevel(LOG_LEVEL)

# Only log to file if debug is enabled
if not args.debug:
    file_logger.removeHandler(file_handler)

# Optimized S3 client configuration
boto_config = Config(
    retries={'max_attempts': 3, 'mode': 'standard'},
    max_pool_connections=THREAD_COUNT,
    connect_timeout=5,
    read_timeout=60
)

# Checksum calculation functions
def calculate_checksum(data, algorithm):
    """Calculate checksum based on selected algorithm"""
    if algorithm == "md5":
        return base64.b64encode(hashlib.md5(data).digest()).decode('ascii')
    elif algorithm == "crc32":
        # CRC32 checksum as per S3 spec
        return base64.b64encode(zlib.crc32(data).to_bytes(4, byteorder='big')).decode('ascii')
    elif algorithm == "crc32c":
        # Implementation may vary, this uses Python's zlib implementation
        # In production, consider using google-crc32c for better CRC32C implementation
        crc = zlib.crc32c(data) if hasattr(zlib, 'crc32c') else zlib.crc32(data)
        return base64.b64encode(crc.to_bytes(4, byteorder='big')).decode('ascii')
    elif algorithm == "sha1":
        return base64.b64encode(hashlib.sha1(data).digest()).decode('ascii')
    elif algorithm == "sha256":
        return base64.b64encode(hashlib.sha256(data).digest()).decode('ascii')
    elif algorithm == "none":
        return None
    else:
        # Default to MD5 if unknown algorithm specified
        return base64.b64encode(hashlib.md5(data).digest()).decode('ascii')

# Generate a single reusable data object for all uploads
def generate_simple_data():
    """Generate one data object to be reused for all uploads"""
    print(f"Generating single data object of {OBJECT_SIZE} bytes...")
    return os.urandom(OBJECT_SIZE)

# Generate test data keys
def generate_object_key():
    """Generate a unique object key"""
    return f"{OBJECT_PREFIX or ''}{str(uuid.uuid4())}"

# Data generation thread that feeds the queue
def data_generator_thread(data_queue, stop_event, simple_content=None):
    """Generates object data as needed and places it in the queue"""
    objects_generated = 0
    prefill_count = min(OBJECTS_COUNT, 10000)  # Prefill with a reasonable amount
    
    try:
        # Generate some initial objects to give upload threads a head start
        print(f"Pre-generating {prefill_count} objects to start...")
        
        for _ in range(prefill_count):
            if stop_event.is_set():
                break
                
            object_key = generate_object_key()
            
            # Either use the simple content or generate new content
            if simple_content is not None:
                data = (object_key, simple_content)
            else:
                data = (object_key, os.urandom(OBJECT_SIZE))
                
            data_queue.put(data)
            objects_generated += 1
            
        print(f"Initial batch of {objects_generated} objects generated")
        
        # Continue generating the rest as needed
        while objects_generated < OBJECTS_COUNT and not stop_event.is_set():
            object_key = generate_object_key()
            
            # Either use the simple content or generate new content
            if simple_content is not None:
                data = (object_key, simple_content)
            else:
                data = (object_key, os.urandom(OBJECT_SIZE))
                
            data_queue.put(data)
            objects_generated += 1
            
    except Exception as e:
        print(f"Error in data generator: {e}")
    finally:
        # Signal end of data
        data_queue.put(None)
        print(f"Data generator finished after creating {objects_generated} objects")

# Create per-thread S3 client
def get_s3_client():
    """Create a new S3 client with optimized configuration"""
    return boto3.client(
        "s3",
        verify=False,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url=S3_ENDPOINT_URL,
        config=boto_config
    )

# Define a function that creates a single object and its versions in the bucket
def upload_object(data, q):
    try:
        object_key, object_content = data
        if args.debug:
            q.put(f"Processing object key: {object_key}")
        
        # Create a thread-local S3 client
        s3_client = get_s3_client()
        
        # Calculate checksum if needed
        checksum_value = calculate_checksum(object_content, CHECKSUM_ALGORITHM) if CHECKSUM_ALGORITHM != "none" else None
        
        # Prepare extra arguments based on checksum algorithm
        extra_args = {}
        if checksum_value:
            if CHECKSUM_ALGORITHM == "md5":
                extra_args["ContentMD5"] = checksum_value
            elif CHECKSUM_ALGORITHM == "sha256":
                extra_args["ChecksumAlgorithm"] = "SHA256"
                extra_args["ChecksumSHA256"] = checksum_value
            elif CHECKSUM_ALGORITHM == "sha1":
                extra_args["ChecksumAlgorithm"] = "SHA1"
                extra_args["ChecksumSHA1"] = checksum_value
            elif CHECKSUM_ALGORITHM == "crc32":
                extra_args["ChecksumAlgorithm"] = "CRC32"
                extra_args["ChecksumCRC32"] = checksum_value
            elif CHECKSUM_ALGORITHM == "crc32c":
                extra_args["ChecksumAlgorithm"] = "CRC32C"
                extra_args["ChecksumCRC32C"] = checksum_value
        
        # Put the initial object in the bucket
        start_time = time.time()
        response = s3_client.put_object(
            Bucket=BUCKET_NAME, 
            Key=object_key, 
            Body=object_content,
            **extra_args
        )
        status_code = response['ResponseMetadata']['HTTPStatusCode']
        duration = time.time() - start_time
        
        q.put(f"Put object {object_key} to the bucket with HTTP status: {status_code} in {duration:.3f}s")
        
        # Skip versions if none requested
        if VERSIONS <= 0:
            return response
            
        # Create versions of the object in parallel using a nested thread pool
        version_futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, VERSIONS)) as version_executor:
            for _ in range(VERSIONS):
                version_futures.append(
                    version_executor.submit(
                        s3_client.put_object,
                        Bucket=BUCKET_NAME, 
                        Key=object_key, 
                        Body=object_content,
                        **extra_args
                    )
                )
                
        # Wait for all versions to complete
        for future in concurrent.futures.as_completed(version_futures):
            try:
                future.result()
            except Exception as e:
                if args.debug:
                    q.put(f"Error creating version: {e}")
        
        if args.debug:
            q.put(f"Created {VERSIONS} versions for object {object_key}")
        return response

    except Exception as e:
        q.put(f'Error creating object: {e}')
        return None

# Worker function that gets data from the queue and uploads it
def worker_function(data_queue, log_queue, results_queue):
    """Worker that takes object data from the queue and uploads it"""
    while True:
        data = data_queue.get()
        if data is None:
            # Signal end of data
            data_queue.put(None)  # Forward the signal to other workers
            break
            
        # Upload the object
        result = upload_object(data, log_queue)
        results_queue.put(1)  # Signal a completion

def main():
    print(f"Starting S3 upload test with {THREAD_COUNT} threads...")
    start_time = time.time()
    
    # Create queues
    data_queue = Queue(maxsize=100000)  # Queue for object data
    log_queue = Queue(maxsize=10000)    # Queue for logging
    results_queue = Queue()             # Queue for completion signals
    stop_event = multiprocessing.Event()  # Event to signal threads to stop
    
    # Start the logging thread
    log_thread_handle = Thread(target=log_thread, args=(log_queue,), daemon=True)
    log_thread_handle.start()
    
    # Generate a single data object for all uploads if simple_data is enabled
    simple_content = None
    if USE_SIMPLE_DATA:
        simple_content = generate_simple_data()
    
    # Start the data generator thread
    data_gen_thread = Thread(
        target=data_generator_thread, 
        args=(data_queue, stop_event, simple_content), 
        daemon=True
    )
    data_gen_thread.start()
    
    # Create worker threads
    workers = []
    for _ in range(THREAD_COUNT):
        worker = Thread(
            target=worker_function,
            args=(data_queue, log_queue, results_queue),
            daemon=True
        )
        workers.append(worker)
        worker.start()
    
    # Monitor progress
    completed = 0
    try:
        while completed < OBJECTS_COUNT:
            try:
                # Get completion signal (non-blocking)
                results_queue.get(timeout=0.1)
                completed += 1
                
                # Print progress periodically
                if completed % 100 == 0 or completed == OBJECTS_COUNT:
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    print(f"Progress: {completed}/{OBJECTS_COUNT} objects ({completed/OBJECTS_COUNT*100:.1f}%) - Rate: {rate:.2f} obj/sec")
            except:
                # No completion signal available
                pass
                
            # Check if all workers are done
            if all(not worker.is_alive() for worker in workers):
                if completed < OBJECTS_COUNT:
                    print(f"Warning: Workers finished but only completed {completed}/{OBJECTS_COUNT} objects")
                break
    except KeyboardInterrupt:
        print("Upload interrupted. Shutting down...")
        stop_event.set()
    
    # Wait for workers to finish
    stop_event.set()
    for worker in workers:
        worker.join(timeout=1.0)
    
    # Signal the logging thread to finish
    log_queue.put(None)
    log_thread_handle.join(timeout=1.0)
    
    # Final stats
    total_time = time.time() - start_time
    print(f"\nUpload completed in {total_time:.2f} seconds")
    print(f"Average upload rate: {completed / total_time:.2f} objects/second")
    print(f"Total objects created: {completed}")
    print(f"Total versions created: {completed * VERSIONS}")

if __name__ == "__main__":
    # Display checksum option banner
    if CHECKSUM_ALGORITHM != "md5" and CHECKSUM_ALGORITHM != "none":
        print(f"""
╔════════════════════════════════════════════════════════════╗
║ CHECKSUM MODE: {CHECKSUM_ALGORITHM.upper().ljust(43)} ║
║ Using non-default checksum algorithm may affect performance ║
╚════════════════════════════════════════════════════════════╝
""")
    
    main()
