from typing import Dict

import aiobotocore.session
from aiobotocore.session import AioSession
import aiofiles
from types_aiobotocore_s3 import S3Client
from typing import Optional
import muty.file
import muty.crypto
from muty.log import MutyLogger
from urllib.parse import urlparse

from pyparsing.common import Any

from gulp.config import GulpConfig

class GulpS3:
    """
    implements API to connect to S3-compatible storage (e.g. minio) and perform operations on it.
    """

    _instance: "GulpS3" = None

    def __new__(cls, *args, **kwargs):
        """Singleton pattern to ensure one client per process"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(
        self,
    ):
        # prevent re-initialization
        if self._initialized:
            return

        # get configuration options
        uri: str = GulpConfig.get_instance().s3_url()
        verify_certs: bool = GulpConfig.get_instance().s3_verify_certs()
        self._region: str = GulpConfig.get_instance().s3_region()
        self._minio: bool = GulpConfig.get_instance().s3_minio()

        # extract url/credentials from uri
        parsed = urlparse(uri)
        url: str = "%s://%s" % (parsed.scheme, parsed.hostname)
        if parsed.port:
            url += ":%s" % parsed.port
        if parsed.path:
            url += parsed.path

        self._init_params: dict = {
            "region_name": self._region,
            "endpoint_url": url,
            "aws_access_key_id": parsed.username,
            "aws_secret_access_key": parsed.password,
            "verify": verify_certs,
        }
        if self._minio:
            # minio-specific configuration
            pass
            # self._init_params["config"] = aiobotocore.config.AioConfig(signature_version="s3v4")

        self._session: AioSession = None
        self._client: S3Client = None
        self._initialized = True
    
    @classmethod
    def get_instance(cls) -> "GulpS3":
        """
        returns the singleton instance

        Returns:
            GulpServer: the singleton instance
        """
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    async def _get_session(self) -> AioSession:
        """Get or create aiohttp session"""
        if not self._session:
            # create a new session (singleton per process)
            self._session = aiobotocore.session.get_session()
        return self._session
    
    async def _get_client(self):
        """Get or create S3 client (singleton per process)"""
        if not self._client:
            # create a new client using the session and configuration
            session = await self._get_session()
            self._client = session.create_client(
                's3',
                **self._init_params
            )
            # enter the async context manager
            self._client = await self._client.__aenter__()
            from gulp.process import GulpProcess
            MutyLogger.get_instance().debug("CONNECTED to s3-compatible storage: %s, main_process=%r", self._client, GulpProcess.get_instance().is_main_process())

        return self._client
    
    async def shutdown(self):
        """Close the client and cleanup resources"""
        if self._client is not None:
            await self._client.__aexit__(None, None, None)
            from gulp.process import GulpProcess
            MutyLogger.get_instance().debug("SHUTDOWN s3-compatible storage client=%sDONE!, main_process=%r", self._client, GulpProcess.get_instance().is_main_process())
            self._client = None

        self._session = None
        GulpS3._instance = None
        self._initialized = False

    async def __aenter__(self):
        await self._get_client()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

    @staticmethod
    def build_object_name(file_path: str, operation_id: str, context_id: str, source_id: str,  user_id: str, req_id: str) -> str:
        """
        Build a unique object name for a file in the storage

        Args:
            file_path (str): The local path of the file.
            operation_id (str): The ID of the operation being performed.
            context_id (str): The ID of the context in which the operation is performed.
            source_id (str): The ID of the source of the file, currently unused
            user_id (str): The ID of the user performing the operation.
            req_id (str): The ID of the request, currently unused
            Returns:
                str: A unique object name for the file in the storage, structured as:
                {user_id}/{operation_id}/context_id{file_name}
        """
        return "%s/%s/%s/%s" % (user_id, operation_id, context_id, file_path.split("/")[-1])

    async def create_bucket(self, bucket_name: str=None) -> dict[str, Any]:
        """
        Create a bucket if it doesn't exist.

        Args:
            bucket_name (str, optional): The name of the bucket to create. Defaults to "gulp".
        Returns:
                dict:
                - status: "created" if bucket was created, "exists" if it already exists
                - bucket: the name of the bucket
                - location: the location of the bucket (if created)
        """
        client = await self._get_client()
        if not bucket_name:
            bucket_name = "gulp"

        try:
            # Check if bucket exists first
            try:
                await client.head_bucket(Bucket=bucket_name)
                d = {
                    "status": "exists",
                    "bucket": bucket_name
                }
                MutyLogger.get_instance().debug("bucket %s already exists in s3-compatible storage: %s", bucket_name, d)
                return d
            except client.exceptions.ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code != '404':
                    raise
            
            # Create bucket with appropriate configuration
            params = {"Bucket": bucket_name}
            
            # Only us-east-1 doesn't require LocationConstraint
            if self._region != "us-east-1":
                params["CreateBucketConfiguration"] = {
                    "LocationConstraint": self.region_name
                }
            
            response = await client.create_bucket(**params)
            d = {
                "status": "created",
                "bucket": bucket_name,
                "location": response.get('Location')
            }
            MutyLogger.get_instance().debug("created bucket %s in s3-compatible storage: %s", bucket_name, d)
            return d
            
        except Exception as e:
            raise Exception(f"failed to create bucket {bucket_name}: {str(e)}")

    async def delete_bucket(self, bucket_name: str = None) -> dict[str, Any]:
        """
        Delete a bucket.

        Args:
            bucket_name (str, optional): The name of the bucket to delete. Defaults to "gulp".
        Returns:
                dict:
                - status: "deleted" if bucket was deleted, "not_found" if it doesn't exist
                - bucket: the name of the bucket
        """
        client = await self._get_client()
        if not bucket_name:
            bucket_name = "gulp"
        try:
            await client.delete_bucket(Bucket=bucket_name)
            d = {
                "status": "deleted",
                "bucket": bucket_name
            }
            MutyLogger.get_instance().debug("deleted bucket %s from s3-compatible storage: %s", bucket_name, d)
            return d
        except client.exceptions.NoSuchBucket:
            d = {
                "status": "not_found",
                "bucket": bucket_name
            }
            MutyLogger.get_instance().debug("bucket %s not found in s3-compatible storage for deletion: %s", bucket_name, d)
            return d
        except Exception as e:
            raise Exception(f"failed to delete bucket {bucket_name}: {str(e)}")
        
    async def list_buckets(self) -> dict[str, Any]:
        """
        List all buckets.

        Returns:
                dict:
                - buckets: list of bucket names
        """
        client = await self._get_client()
        try:
            response = await client.list_buckets()
            buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
            d = {
                "buckets": buckets
            }
            MutyLogger.get_instance().debug("listed buckets in s3-compatible storage: %s", d)
            return d
        except Exception as e:
            raise Exception(f"failed to list buckets: {str(e)}")
    
    async def list_by_tags(
        self,
        tags: dict[str, str] = None,
        bucket_name: str = None,
        continuation_token: str = None,
        max_results: int = 1000,
    ) -> dict[str, Any]:
        """
        List objects in a bucket that have at least the specified tags.

        This method is **paginated** and will return at most ``max_results``
        results per call.  The caller may pass a ``continuation_token`` to
        retrieve subsequent pages; the response will include
        ``next_continuation_token`` when there are more results available.

        Each returned object entry includes its key name, size in bytes, and
        the full set of tags attached to it.  Objects are only returned if all
        of the requested tags are present (objects may have additional tags
        beyond the filter).

        Args:
            tags (dict[str, str], optional): A dictionary of tags to match for
                listing.  Objects that have at least these tags (can have more)
                will be listed.  Defaults to ``None`` (no tags, list all).
            bucket_name (str, optional): The name of the bucket to list from.
            continuation_token (str, optional): Token previously returned by a
                prior call; used to continue listing from where it left off.
            max_results (int, optional): Maximum number of objects to return in
                this page.  Defaults to 1000.
        Returns:
            dict:
            - objects: list of dicts each containing ``storage_id``, ``size`` and
                ``tags`` for the matched files
            - bucket: the bucket name
            - tags: copy of the requested tag filter
            - next_continuation_token: token to pass to the next call if more
                results exist, otherwise ``None``
        """
        client = await self._get_client()
        if bucket_name is None:
            bucket_name = "gulp"
        if not tags:
            tags = {}

        try:
            params: dict[str, Any] = {
                "Bucket": bucket_name,
                "MaxKeys": max_results,
            }
            if continuation_token:
                params["ContinuationToken"] = continuation_token

            response = await client.list_objects_v2(**params)
            objects = response.get("Contents", [])

            matched_objects: list[dict[str, Any]] = []
            for obj in objects:
                key = obj.get("Key")
                if not key:
                    continue
                size = obj.get("Size")

                # fetch tags for the object
                tagresp = await client.get_object_tagging(Bucket=bucket_name, Key=key)
                objtags = {t["Key"]: t["Value"] for t in tagresp.get("TagSet", [])}

                # only include if all requested tags are present
                if all(objtags.get(k) == v for k, v in tags.items()):
                    matched_objects.append({
                        "storage_id": key,
                        "size": size,
                        "tags": objtags,
                    })

            next_token = response.get("NextContinuationToken")
            d = {
                "objects": matched_objects,
                "bucket": bucket_name,
                "tags": tags,
                "next_continuation_token": next_token,
            }
            MutyLogger.get_instance().debug(
                "listed objects with tags %s from bucket %s in s3-compatible storage: %s",
                tags,
                bucket_name,
                d,
            )
            return d
        except Exception as e:
            raise Exception(
                f"failed to list objects with tags {tags} from bucket {bucket_name}: {str(e)}"
            )
        
    async def delete_by_tags(self, tags: dict[str, str] = None, bucket_name: str = None) -> dict[str, Any]:
        """
        Delete objects in a bucket that have at least the specified tags.

        If no tags are provided (``tags`` is ``None`` or empty), all objects in
        the bucket will be removed.
        Args:
            tags (dict[str, str], optional): A dictionary of tags to match for deletion, default=None (delete all)
            bucket_name (str, optional): The name of the bucket to delete from.

        Returns:
            dict:
            - status: "deleted" if any objects were removed, "not_found" if
                none matched (or the bucket was empty)
            - bucket: the bucket name
            - tags: copy of the requested tag filter
        """
        client = await self._get_client()
        if bucket_name is None:
            bucket_name = "gulp"

        # normalize tags and determine if we should delete everything
        delete_all = False
        if not tags:
            # empty/None means "delete everything" under the new behaviour
            tags = {}
            delete_all = True

        try:
            deleted_any = False
            to_delete: list[dict] = []
            continuation_token: Optional[str] = None
            num_deleted = 0

            while True:
                params: dict[str, Any] = {
                    "Bucket": bucket_name,
                    "MaxKeys": 1000,
                }
                if continuation_token:
                    params["ContinuationToken"] = continuation_token

                response = await client.list_objects_v2(**params)
                objects = response.get("Contents", [])
                if not objects:
                    break

                for obj in objects:
                    key = obj.get("Key")
                    if not key:
                        continue

                    if delete_all:
                        # no need to fetch tags if we're deleting everything
                        deleted_any = True
                        to_delete.append({"Key": key})
                    else:
                        # fetch tags for the object
                        tagresp = await client.get_object_tagging(Bucket=bucket_name, Key=key)
                        objtags = {t["Key"]: t["Value"] for t in tagresp.get("TagSet", [])}

                        # only delete if all requested tags are present
                        match = all(objtags.get(k) == v for k, v in tags.items())
                        if match:
                            deleted_any = True
                            to_delete.append({"Key": key})

                    # flush a batch when we hit 1000
                    if len(to_delete) >= 1000:
                        await client.delete_objects(Bucket=bucket_name, Delete={"Objects": to_delete})
                        num_deleted += len(to_delete)

                        to_delete.clear()

                continuation_token = response.get("NextContinuationToken")
                if not continuation_token:
                    break

            if to_delete:
                # delete any remaining objects in the final batch
                await client.delete_objects(Bucket=bucket_name, Delete={"Objects": to_delete})
                num_deleted += len(to_delete)

            if not deleted_any:
                d = {"status": "not_found", "bucket": bucket_name, "tags": tags, "num_deleted": 0}
                MutyLogger.get_instance().debug(
                    "no objects found with tags %s in bucket %s for deletion in s3-compatible storage: %s",
                    tags,
                    bucket_name,
                    d,
                )
                return d

            d = {"status": "deleted", "bucket": bucket_name, "tags": tags, "num_deleted": num_deleted}
            MutyLogger.get_instance().debug(
                "deleted %d objects with tags %s from bucket %s in s3-compatible storage: %s",
                num_deleted,
                tags,
                bucket_name,
                d,
            )
            return d
        except Exception as e:
            raise Exception(
                f"failed to delete objects with tags {tags} from bucket {bucket_name}: {str(e)}"
            )
        
    async def upload_file(
        self,
        file_path: str,
        bucket_name: str = None,
        object_name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> dict[str, Any]:
        """
        Upload a file to a bucket by either performing a simple put or a
        multipart upload when the file is large.

        Args:
            file_path (str): The local path of the file to upload.
            bucket_name (str, optional): The name of the bucket to upload to.
                Defaults to "gulp".
            object_name (str, optional): The name of the object in the bucket.
                Defaults to the file name (derived from file_path).
            tags (dict or str, optional): Optional tags to apply to the object.
                If a dict is provided it will be url-encoded.  Can also be a
                pre-formatted tagging string ("key1=value1&key2=value2").

        Returns:
            dict:
            - status: "uploaded" if file was uploaded successfully
            - bucket: the name of the bucket
            - object: the name of the object in the bucket
        """
        client = await self._get_client()
        if not bucket_name:
            bucket_name = "gulp"

        if not object_name:
            object_name = file_path.split("/")[-1]

        # normalize tags parameter to the string expected by the S3 API
        tagging: Optional[str] = None
        if tags:
            if isinstance(tags, dict):
                # urlencode will correctly quote any reserved characters
                from urllib.parse import urlencode

                tagging = urlencode(tags)
            else:
                tagging = tags

        # aws minimum part size for multipart uploads is 5MB
        chunk_size: int = 5 * 1024 * 1024
        uploaded_chunks: int = 0
        try:
            size: int = await muty.file.get_size(file_path)
            MutyLogger.get_instance().debug("uploading file %s (%d bytes) to bucket %s in s3-compatible storage ...", file_path, size, bucket_name)

            if size <= chunk_size:
                # small file, single put
                async with aiofiles.open(file_path, "rb") as f:
                    data = await f.read()
                params = {"Bucket": bucket_name, "Key": object_name, "Body": data}
                if tagging:
                    params["Tagging"] = tagging
                await client.put_object(**params)
                uploaded_chunks += 1
            else:
                # multipart upload
                create_params = {"Bucket": bucket_name, "Key": object_name}
                if tagging:
                    create_params["Tagging"] = tagging
                resp = await client.create_multipart_upload(**create_params)
                upload_id = resp["UploadId"]
                parts = []
                part_number = 1

                try:
                    async with aiofiles.open(file_path, "rb") as f:
                        while True:
                            chunk = await f.read(chunk_size)
                            if not chunk:
                                break
                            part_resp = await client.upload_part(
                                Bucket=bucket_name,
                                Key=object_name,
                                PartNumber=part_number,
                                UploadId=upload_id,
                                Body=chunk,
                            )
                            parts.append({
                                "ETag": part_resp["ETag"],
                                "PartNumber": part_number,
                            })
                            MutyLogger.get_instance().debug("UPLOADED chunk %d for file %s to bucket %s in s3-compatible storage ...", part_number, file_path, bucket_name)
                            part_number += 1
                            uploaded_chunks+=1

                    await client.complete_multipart_upload(
                        Bucket=bucket_name,
                        Key=object_name,
                        UploadId=upload_id,
                        MultipartUpload={"Parts": parts},
                    )
                except Exception:
                    # abort on failure and re-raise
                    await client.abort_multipart_upload(
                        Bucket=bucket_name,
                        Key=object_name,
                        UploadId=upload_id,
                    )
                    raise

            d = {
                "status": "uploaded",
                "bucket": bucket_name,
                "object": object_name,
            }
            MutyLogger.get_instance().debug("DONE uploaded file %s (%d chunks) to bucket %s in s3-compatible storage: %s", file_path, uploaded_chunks, bucket_name, d)
            return d
        except Exception as e:
            raise Exception(f"failed to upload file {file_path} to bucket {bucket_name}: {str(e)}")
        
    async def upload_data(
        self,
        data: bytes,
        object_name: str,
        bucket_name: str = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> dict[str, Any]:
        """
        Upload data to a bucket as an object.

        Optional tagging works the same way as :meth:`upload_file` – a dict
        will be url-encoded and a preformatted string is also accepted.

        Args:
            data (bytes): The data to upload.
            object_name (str): The name of the object in the bucket.
            bucket_name (str, optional): The name of the bucket to upload to.
                Defaults to "gulp".
            tags (dict or str, optional): Tags to apply to the new object.
        Returns:
            dict:
            - status: "uploaded" if data was uploaded successfully
            - bucket: the name of the bucket
            - object: the name of the object in the bucket
        """
        client = await self._get_client()
        if not bucket_name:
            bucket_name = "gulp"

        tagging: Optional[str] = None
        if tags:
            if isinstance(tags, dict):
                from urllib.parse import urlencode

                tagging = urlencode(tags)
            else:
                tagging = tags

        try:
            MutyLogger.get_instance().debug("uploading data(len=%d) to object %s in bucket %s in s3-compatible storage ...", len(data), object_name, bucket_name)
            params = {"Bucket": bucket_name, "Key": object_name, "Body": data}
            if tagging:
                params["Tagging"] = tagging
            await client.put_object(**params)
            d = {
                "status": "uploaded",
                "bucket": bucket_name,
                "object": object_name
            }
            MutyLogger.get_instance().debug("uploaded data to object %s in bucket %s in s3-compatible storage: %s", object_name, bucket_name, d)
            return d
        except Exception as e:
            raise Exception(f"failed to upload data to object {object_name} in bucket {bucket_name}: {str(e)}")
            
    async def download_file(self, object_name: str, file_path: str, bucket_name: str = None) -> dict[str, Any]:
        """
        Download a file from a bucket.

        Args:
            object_name (str): The name of the object in the bucket to download.
            file_path (str): The local path to save the downloaded file.
            bucket_name (str, optional): The name of the bucket to download from. Defaults to "gulp".
        Returns:
                dict:
                - status: "downloaded" if file was downloaded successfully
                - bucket: the name of the bucket
                - object: the name of the object in the bucket
                - file_path: the local path of the downloaded file
        """
        client = await self._get_client()
        if not bucket_name:
            bucket_name = "gulp"

        try:
            MutyLogger.get_instance().debug(
                "downloading object %s from bucket %s in s3-compatible storage to file %s ...",
                object_name,
                bucket_name,
                file_path,
            )

            # fetch the object
            resp = await client.get_object(Bucket=bucket_name, Key=object_name)
            body = resp.get("Body")
            if body is None:
                raise Exception("received no body when downloading object")

            # write the stream to the target file in chunks to avoid
            # buffering the entire object in memory.
            async with aiofiles.open(file_path, "wb") as f:
                while True:
                    chunk = await body.read(1024 * 1024)
                    if not chunk:
                        break
                    await f.write(chunk)
                # ensure the body is closed when we're done
                try:
                    await body.close()
                except Exception:
                    pass

            d = {
                "status": "downloaded",
                "bucket": bucket_name,
                "object": object_name,
                "file_path": file_path,
            }
            MutyLogger.get_instance().debug(
                "downloaded file %s from bucket %s in s3-compatible storage: %s",
                object_name,
                bucket_name,
                d,
            )
            return d
        except Exception as e:
            raise Exception(
                f"failed to download file {object_name} from bucket {bucket_name}: {str(e)}"
            )
            
    async def delete_file(self, object_name: str, bucket_name: str = None) -> dict[str, Any]:
        """
        Delete a file from a bucket.

        Args:
            object_name (str): The name of the object in the bucket to delete.
            bucket_name (str, optional): The name of the bucket to delete from. Defaults to "gulp".
        Returns:
                dict:
                - status: "deleted" if file was deleted successfully, "not_found" if it
                    doesn't exist
                - bucket: the name of the bucket
                - object: the name of the object in the bucket
        """
        client = await self._get_client()
        if not bucket_name:
            bucket_name = "gulp"

        try:
            MutyLogger.get_instance().debug("deleting object %s from bucket %s in s3-compatible storage ...", object_name, bucket_name)
            await client.delete_object(Bucket=bucket_name, Key=object_name)
            d = {
                "status": "deleted",
                "bucket": bucket_name,
                "object": object_name
            }
            MutyLogger.get_instance().debug("deleted file %s from bucket %s in s3-compatible storage: %s", object_name, bucket_name, d)
            return d
        except client.exceptions.NoSuchKey:
            d = {
                "status": "not_found",
                "bucket": bucket_name,
                "object": object_name
            }
            MutyLogger.get_instance().debug("file %s not found in bucket %s for deletion in s3-compatible storage: %s", object_name, bucket_name, d)
            return d
        except Exception as e:
            raise Exception(f"failed to delete file {object_name} from bucket {bucket_name}: {str(e)}")
        
    