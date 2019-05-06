import botocore
from pywren_ibm_cloud.storage.exceptions import StorageNoSuchKeyError

class S3Backend:
    """
    A wrap-up around S3 boto3 APIs.
    """

    def __init__(self, s3config):
        self.session = botocore.session.get_session()
        self.s3_client = self.session.create_client(
            's3', config=botocore.client.Config(max_pool_connections=200))

    def put_object(self, bucket_name, key, data):
        """
        Put an object in S3. Override the object if the key already exists.
        :param key: key of the object.
        :param data: data of the object
        :type data: str/bytes
        :return: None
        """
        self.s3_client.put_object(Bucket=bucket_name, Key=key, Body=data)

    def get_object(self, bucket_name, key, stream=False, extra_get_args={}):
        """
        Get object from S3 with a key. Throws StorageNoSuchKeyError if the given key does not exist.
        :param key: key of the object
        :return: Data of the object
        :rtype: str/bytes
        """
        try:
            r = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            data = r['Body'].read()
            return data
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                raise StorageNoSuchKeyError(key)
            else:
                raise e

    def key_exists(self, key):
        """
        Check if a key exists in S3.
        :param key: key of the object
        :return: True if key exists, False if not exists
        :rtype: boolean
        """
        try:
            self.s3_client.head_object(Bucket=bucket_name, Key=key)
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                return False
            else:
                raise e

    def head_object(self, bucket_name, key):
        """
        Head object from S3 with a key. Throws StorageNoSuchKeyError if the given key does not exist.
        :param key: key of the object
        :return: Data of the object
        :rtype: str/bytes
        """
        try:
            self.s3_client.head_object(Bucket=bucket_name, Key=key)
            return metadata['ResponseMetadata']['HTTPHeaders']
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                raise StorageNoSuchKeyError(key)
            else:
                raise e

    def delete_object(self, bucket_name, key):
        """
        Delete an object from storage.
        :param bucket: bucket name (not used in S3, bucket name set in config)
        :param key: data key
        """
        return self.s3_client.delete_object(Bucket=bucket_name, Key=key)

    def delete_objects(self, bucket_name, key_list):
        """
        Delete a list of objects from storage.
        :param bucket: bucket name
        :param key_list: list of keys
        """
        result = []
        max_keys_num = 1000
        for i in range(0, len(key_list), max_keys_num):
            delete_keys = {'Objects': []}
            delete_keys['Objects'] = [{'Key': k} for k in key_list[i:i+max_keys_num]]
            result.append(self.s3_client.delete_objects(Bucket=bucket_name, Delete=delete_keys))
        return result

    def list_objects(self, bucket_name, prefix=None):
        paginator = self.s3_client.get_paginator('list_objects_v2')
        try:
            if (prefix is not None):
                page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            else:
                page_iterator = paginator.paginate(Bucket=bucket_name)

            object_list = []
            for page in page_iterator:
                if 'Contents' in page:
                    for item in page['Contents']:
                        object_list.append(item)
            return object_list
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise StorageNoSuchKeyError(bucket_name)
            else:
                raise e

    def list_keys_with_prefix(self, bucket_name, prefix):
        """
        Return a list of keys for the given prefix.
        :param prefix: Prefix to filter object names.
        :return: List of keys in bucket that match the given prefix.
        :rtype: list of str
        """
        paginator = self.s3_client.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': bucket_name,
                                'Prefix': prefix}
        page_iterator = paginator.paginate(**operation_parameters)

        key_list = []
        for page in page_iterator:
            if 'Contents' in page:
                for item in page['Contents']:
                    key_list.append(item['Key'])

        return key_list
