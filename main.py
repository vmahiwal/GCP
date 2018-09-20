
from google.cloud import storage
import datetime
storage_client = storage.Client()

def gcs_copy(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
     
    file = event
    
    source_bucket_name = "sync_files"
    destination_bucket_name = "process_files"
           
    print(f"Processing file for transfer to bucket: {file['name']}.")
    triggered_file_name = file['name']
    #triggered_file_name = "4484_message_blast_updates.20180912.json"
    
  
    source_bucket = storage_client.get_bucket(source_bucket_name)
    source_blob = source_bucket.blob(triggered_file_name)
    destination_bucket = storage_client.get_bucket(destination_bucket_name)
    today = datetime.date.today()
    #yesterday = today - datetime.timedelta(1)
    
    ### Condition to move files
    
    if  "message_blast" in triggered_file_name and today.strftime('%Y%m%d') in triggered_file_name and "-" not in triggered_file_name:
        
                
        if "message_blast" in triggered_file_name and "message_blast_update" not in triggered_file_name :
            destinaion_file_name = triggered_file_name[0:4] + "_message_blast" + ".json.gz"
            print( "Case 1 insert : " + destinaion_file_name)
            destination_blob = source_bucket.copy_blob(source_blob, destination_bucket, destinaion_file_name)
            print('File {} in bucket {} copied to file {} in bucket {}.'.format(source_blob.name, source_bucket.name, destination_blob.name, destination_bucket.name))
        
        elif "message_blast_update" in triggered_file_name :
            destinaion_file_name = triggered_file_name[0:4] + "_message_blast_update" + ".json.gz"
            print( "Case 2 update: " + destinaion_file_name)
            destination_blob = source_bucket.copy_blob(source_blob, destination_bucket, destinaion_file_name)
            print('File {} in bucket {} copied to file {} in bucket {}.'.format(source_blob.name, source_bucket.name, destination_blob.name, destination_bucket.name))
   
    else:
        print("Its not todays file")