import aiobotocore.session
from aiobotocore.session import AioSession
from muty.log import MutyLogger
from urllib.parse import urlparse

from gulp.config import GulpConfig

class GulpS3:
    """
    implements API to connect to S3-compatible storage (e.g. minio) and perform operations on it.
    """

    _instance: "GulpS3" = None

    def __init__(self):
        # get configuration options
        uri: str = GulpConfig.get_instance().s3_url()
        verify_certs: bool = GulpConfig.get_instance().s3_verify_certs()
        region: str = GulpConfig.get_instance().s3_region()
        minio: bool = GulpConfig.get_instance().s3_minio()

        # extract url/credentials from uri
        parsed = urlparse(uri)
        url: str = "%s://%s" % (parsed.scheme, parsed.hostname)
        if parsed.port:
            url += ":%s" % parsed.port
        if parsed.path:
            url += parsed.path
        boto_sess: AioSession = aiobotocore.session.get_session()
        d: dict = {
            "region_name": region,
            "endpoint_url": url,
            "aws_access_key_id": parsed.username,
            "aws_secret_access_key": parsed.password,
            "verify": verify_certs,
        }
        if minio:
            # minio-specific configuration
            d["config"] = aiobotocore.config.AioConfig(signature_version="s3v4")

        self._boto_client = boto_sess.create_client(
            "s3",
            **d            
        )
        from gulp.process import GulpProcess
        MutyLogger.get_instance().debug("initialized s3-compatible storage client: %s, main_process=%r", self._boto_client, GulpProcess.get_instance().is_main_process())

        # must call connect() before using the client
        self._initialized = False

    def __new__(cls):
        """
        Create a new instance of the class.
        """
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls) -> "GulpS3":
        """
        returns the singleton instance of the S3 client.

        Returns:
            GulpS3: The singleton instance of the S3 client.
        """
        if not cls._instance:
            cls._instance = cls()
        return cls._instance


    async def connect(self):
        """
        Connect to the S3-compatible storage.

        Returns:
            None
        """
        await self._boto_client.__aenter__()
        self._initialized = True
        from gulp.process import GulpProcess
        MutyLogger.get_instance().debug("CONNECTED to s3-compatible storage: %s, main_process=%r", self._boto_client, GulpProcess.get_instance().is_main_process())

    async def shutdown(self) -> None:
        """
        Shutdown the S3 client.

        Returns:
            None
        """
        await self._boto_client.__aexit__(None, None, None) 
        self._initialized = False
        from gulp.process import GulpProcess
        MutyLogger.get_instance().debug("shutdown s3-compatible storage client=%sDONE!, main_process=%r", self._boto_client, GulpProcess.get_instance().is_main_process())

    async def create_bucket(self, bucket_name: str=None) -> None:
        """
        Creates the bucket used by gulp to store data in the S3-compatible storage, if it does not exist already.

        Args:
            bucket_name (str, optional): The name of the bucket to create. If not provided, defaults to "gulp".
        Returns:
            None
        """
        if not bucket_name:
            bucket_name="gulp"

        if not self._initialized:
            raise Exception("S3 client not initialized, call connect() before using it")
        try:
            await self._boto_client.head_bucket(Bucket=bucket_name)
            MutyLogger.get_instance().debug("bucket %s already exists in s3-compatible storage", bucket_name)
        except self._boto_client.exceptions.NoSuchBucket:
            await self._boto_client.create_bucket(Bucket=bucket_name)
            MutyLogger.get_instance().debug("created bucket %s in s3-compatible storage", bucket_name)