######################################################################
#
# File: b2sdk/transferer/transferer.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import logging
import six

from ..download_dest import DownloadDestProgressWrapper
from ..exception import (
    AlreadyFailed,
    B2Error,
    ChecksumMismatch,
    InvalidRange,
    MaxFileSizeExceeded,
    MaxRetriesExceeded,
    TruncatedOutput,
    UnexpectedCloudBehaviour,
)
from ..file_version import FileVersionInfoFactory
from ..part import PartFactory
from ..progress import (
    DoNothingProgressListener, RangeOfInputStream, ReadingStreamWithProgress, StreamWithHash
)
from ..raw_api import HEX_DIGITS_AT_END, SRC_LAST_MODIFIED_MILLIS
from ..unfinished_large_file import UnfinishedLargeFile
from ..utils import B2TraceMetaAbstract, choose_part_ranges, hex_sha1_of_stream, interruptible_get_result
from .file_metadata import FileMetadata
from .large_file_upload_state import LargeFileUploadState
from .parallel import ParallelDownloader
from .progress_reporter import PartProgressReporter
from .simple import SimpleDownloader

try:
    import concurrent.futures as futures
except ImportError:
    import futures

logger = logging.getLogger(__name__)


@six.add_metaclass(B2TraceMetaAbstract)
class Transferer(object):
    """
    Handle complex actions around downloads and uploads to free raw_api from that responsibility.
    """

    # how many chunks to break a downloaded file into
    DEFAULT_MAX_STREAMS = 8

    # minimum size of a download chunk
    DEFAULT_MIN_PART_SIZE = 100 * 1024 * 1024

    # block size used when downloading file. If it is set to a high value, progress reporting will be jumpy, if it's too low, it impacts CPU
    MIN_CHUNK_SIZE = 8192  # ~1MB file will show ~1% progress increment
    MAX_CHUNK_SIZE = 1024**2

    MAX_UPLOAD_ATTEMPTS = 5
    MAX_LARGE_FILE_SIZE = 10 * 1000 * 1000 * 1000 * 1000  # 10 TB

    def __init__(self, session, account_info, max_upload_workers=10):
        """
        :param max_streams: limit on a number of streams to use when downloading in multiple parts
        :param min_part_size: the smallest part size for which a stream will be run
                              when downloading in multiple parts
        """
        self.session = session
        self.account_info = account_info

        self.strategies = [
            ParallelDownloader(
                max_streams=self.DEFAULT_MAX_STREAMS,
                min_part_size=self.DEFAULT_MIN_PART_SIZE,
                min_chunk_size=self.MIN_CHUNK_SIZE,
                max_chunk_size=self.MAX_CHUNK_SIZE,
            ),
            #IOTDownloader(),  # TODO: curl -s httpbin.org/get | tee /dev/stderr 2>ble | sha1sum | cut -c -40
            SimpleDownloader(
                min_chunk_size=self.MIN_CHUNK_SIZE,
                max_chunk_size=self.MAX_CHUNK_SIZE,
            ),
        ]
        self.upload_executor = None
        self.max_workers = max_upload_workers

    def set_thread_pool_size(self, max_workers):
        """
        Set the size of the thread pool to use for uploads and downloads.

        Must be called before any work starts, or the thread pool will get
        the default size of 1.

        :param int max_workers: maximum allowed number of workers in a pool
        """
        if self.transferer.upload_executor is not None:
            raise Exception('thread pool already created')
        self.max_workers = max_workers

    @property
    def _get_thread_pool(self):
        """
        Return the thread pool executor to use for uploads and downloads.
        """
        if self.upload_executor is None:
            self.upload_executor = futures.ThreadPoolExecutor(max_workers=self.max_workers)
        return self.upload_executor

    def download_file_from_url(
        self,
        url,
        download_dest,
        progress_listener=None,
        range_=None,
    ):
        """
        :param url: url from which the file should be downloaded
        :param download_dest: where to put the file when it is downloaded
        :param progress_listener: where to notify about progress downloading
        :param range_: 2-element tuple containing data of http Range header
        """
        progress_listener = progress_listener or DoNothingProgressListener()
        download_dest = DownloadDestProgressWrapper(download_dest, progress_listener)
        with self.session.download_file_from_url(
            url,
            url_factory=self.account_info.get_download_url,
            range_=range_,
        ) as response:
            metadata = FileMetadata.from_response(response)
            if range_ is not None:
                if 'Content-Range' not in response.headers:
                    raise UnexpectedCloudBehaviour('Content-Range header was expected')
                if (range_[1] - range_[0] + 1) != metadata.content_length:
                    raise InvalidRange(metadata.content_length, range_)

            mod_time_millis = int(
                metadata.file_info.get(
                    SRC_LAST_MODIFIED_MILLIS,
                    response.headers['x-bz-upload-timestamp'],
                )
            )

            with download_dest.make_file_context(
                metadata.file_id,
                metadata.file_name,
                metadata.content_length,
                metadata.content_type,
                metadata.content_sha1,
                metadata.file_info,
                mod_time_millis,
                range_=range_,
            ) as file:

                for strategy in self.strategies:
                    if strategy.is_suitable(metadata, progress_listener):
                        bytes_read, actual_sha1 = strategy.download(
                            file, response, metadata, self.session
                        )
                        break
                else:
                    assert False, 'no strategy suitable for download was found!'

                self._validate_download(
                    range_, bytes_read, actual_sha1, metadata
                )  # raises exceptions
                return metadata.as_info_dict()

    def _validate_download(self, range_, bytes_read, actual_sha1, metadata):
        if range_ is None:
            if bytes_read != metadata.content_length:
                raise TruncatedOutput(bytes_read, metadata.content_length)

            if metadata.content_sha1 != 'none' and \
                actual_sha1 != metadata.content_sha1:  # no yapf
                raise ChecksumMismatch(
                    checksum_type='sha1',
                    expected=metadata.content_length,
                    actual=actual_sha1,
                )
        else:
            desired_length = range_[1] - range_[0] + 1
            if bytes_read != desired_length:
                raise TruncatedOutput(bytes_read, desired_length)

    def list_parts(self, file_id, start_part_number=None, batch_size=None):
        """
        Generator that yields a :py:class:`b2sdk.v1.Part` for each of the parts that have been uploaded.

        :param str file_id: the ID of the large file that is not finished
        :param int start_part_number: the first part number to return; defaults to the first part
        :param int batch_size: the number of parts to fetch at a time from the server
        :rtype: generator
        """
        batch_size = batch_size or 100
        while True:
            response = self.session.list_parts(file_id, start_part_number, batch_size)
            for part_dict in response['parts']:
                yield PartFactory.from_list_parts_dict(part_dict)
            start_part_number = response.get('nextPartNumber')
            if start_part_number is None:
                break

    def list_unfinished_large_files(
        self, bucket_id, start_file_id=None, batch_size=None, prefix=None
    ):
        """
        A generator that yields an :py:class:`b2sdk.v1.UnfinishedLargeFile` for each
        unfinished large file in the bucket, starting at the given file.

        :param str bucket_id: bucket id
        :param str,None start_file_id: a file ID to start from or None to start from the beginning
        :param int,None batch_size: max file count
        :param str,None prefix: file name prefix filter
        :rtype: generator[b2sdk.v1.UnfinishedLargeFile]
        """
        batch_size = batch_size or 100
        while True:
            batch = self.session.list_unfinished_large_files(
                bucket_id, start_file_id, batch_size, prefix
            )
            for file_dict in batch['files']:
                yield UnfinishedLargeFile(file_dict)
            start_file_id = batch.get('nextFileId')
            if start_file_id is None:
                break

    def start_large_file(self, bucket_id, file_name, content_type=None, file_info=None):
        """
        Start a large file transfer.

        :param str file_name: a file name
        :param str,None content_type: the MIME type, or ``None`` to accept the default based on file extension of the B2 file name
        :param dict,None file_infos: a file info to store with the file or ``None`` to not store anything
        """
        return UnfinishedLargeFile(
            self.session.start_large_file(bucket_id, file_name, content_type, file_info)
        )

    def upload(
        self, bucket_id, upload_source, min_part_size, file_name, content_type, file_info,
        progress_listener
    ):
        # We don't upload any large files unless all of the parts can be at least
        # the minimum part size.
        min_part_size = max(min_part_size or 0, self.account_info.get_minimum_part_size())
        min_large_file_size = min_part_size * 2

        if upload_source.get_content_length() < min_large_file_size:
            # Run small uploads in the same thread pool as large file uploads,
            # so that they share resources during a sync.
            f = self._get_thread_pool.submit(
                self._upload_small_file,
                bucket_id,
                upload_source,
                file_name,
                content_type,
                file_info,
                progress_listener,
            )
            return f.result()
        else:
            return self._upload_large_file(
                bucket_id,
                upload_source,
                file_name,
                content_type,
                file_info,
                progress_listener,
            )

    def _upload_small_file(
        self, bucket_id, upload_source, file_name, content_type, file_info, progress_listener
    ):
        content_length = upload_source.get_content_length()
        exception_info_list = []
        progress_listener.set_total_bytes(content_length)
        with progress_listener:
            for _ in six.moves.xrange(self.MAX_UPLOAD_ATTEMPTS):
                try:
                    with upload_source.open() as file:
                        input_stream = ReadingStreamWithProgress(file, progress_listener)
                        hashing_stream = StreamWithHash(input_stream)
                        length_with_hash = content_length + hashing_stream.hash_size()
                        response = self.session.upload_file(
                            bucket_id, None, file_name, length_with_hash, content_type,
                            HEX_DIGITS_AT_END, file_info, hashing_stream
                        )
                        assert hashing_stream.hash == response['contentSha1']
                        return FileVersionInfoFactory.from_api_response(response)

                except B2Error as e:
                    if not e.should_retry_upload():
                        raise
                    exception_info_list.append(e)
                    self.account_info.clear_bucket_upload_data(bucket_id)

        raise MaxRetriesExceeded(self.MAX_UPLOAD_ATTEMPTS, exception_info_list)

    def _upload_large_file(
        self, bucket_id, upload_source, file_name, content_type, file_info, progress_listener
    ):
        content_length = upload_source.get_content_length()
        if self.MAX_LARGE_FILE_SIZE < content_length:
            raise MaxFileSizeExceeded(content_length, self.MAX_LARGE_FILE_SIZE)
        minimum_part_size = self.account_info.get_minimum_part_size()

        # Set up the progress reporting for the parts
        progress_listener.set_total_bytes(content_length)

        # Select the part boundaries
        part_ranges = choose_part_ranges(content_length, minimum_part_size)

        # Check for unfinished files with same name
        unfinished_file, finished_parts = self._find_unfinished_file_if_possible(
            bucket_id,
            upload_source,
            file_name,
            file_info,
            part_ranges,
        )

        # Tell B2 we're going to upload a file if necessary
        if unfinished_file is None:
            unfinished_file = self.start_large_file(bucket_id, file_name, content_type, file_info)
        file_id = unfinished_file.file_id

        with progress_listener:
            large_file_upload_state = LargeFileUploadState(progress_listener)
            # Tell the executor to upload each of the parts
            part_futures = [
                self._get_thread_pool.submit(
                    self._upload_part,
                    bucket_id,
                    file_id,
                    part_index + 1,  # part number
                    part_range,
                    upload_source,
                    large_file_upload_state,
                    finished_parts,
                ) for (part_index, part_range) in enumerate(part_ranges)
            ]

            # Collect the sha1 checksums of the parts as the uploads finish.
            # If any of them raised an exception, that same exception will
            # be raised here by result()
            part_sha1_array = [interruptible_get_result(f)['contentSha1'] for f in part_futures]

        # Finish the large file
        response = self.session.finish_large_file(file_id, part_sha1_array)
        return FileVersionInfoFactory.from_api_response(response)

    def _find_unfinished_file_if_possible(
        self, bucket_id, upload_source, file_name, file_info, part_ranges
    ):
        """
        Find an unfinished file that may be used to resume a large file upload.  The
        file is found using the filename and comparing the uploaded parts against
        the local file.

        This is only possible if the application key being used allows ``listFiles`` access.
        """
        if 'listFiles' in self.account_info.get_allowed()['capabilities']:
            for file_ in self.list_unfinished_large_files(bucket_id):
                if file_.file_name == file_name and file_.file_info == file_info:
                    files_match = True
                    finished_parts = {}
                    for part in self.list_parts(file_.file_id):
                        # Compare part sizes
                        offset, part_length = part_ranges[part.part_number - 1]
                        if part_length != part.content_length:
                            files_match = False
                            break

                        # Compare hash
                        with upload_source.open() as f:
                            f.seek(offset)
                            sha1_sum = hex_sha1_of_stream(f, part_length)
                        if sha1_sum != part.content_sha1:
                            files_match = False
                            break

                        # Save part
                        finished_parts[part.part_number] = part

                    # Skip not matching files or unfinished files with no uploaded parts
                    if not files_match or not finished_parts:
                        continue

                    # Return first matched file
                    return file_, finished_parts
        return None, {}

    def _upload_part(
        self,
        bucket_id,
        file_id,
        part_number,
        part_range,
        upload_source,
        large_file_upload_state,
        finished_parts=None
    ):
        # Check if this part was uploaded before
        if finished_parts is not None and part_number in finished_parts:
            # Report this part finished
            part = finished_parts[part_number]
            large_file_upload_state.update_part_bytes(part.content_length)

            # Return SHA1 hash
            return {'contentSha1': part.content_sha1}

        # Set up a progress listener
        part_progress_listener = PartProgressReporter(large_file_upload_state)

        # Retry the upload as needed
        exception_list = []
        for _ in six.moves.xrange(self.MAX_UPLOAD_ATTEMPTS):
            # if another part has already had an error there's no point in
            # uploading this part
            if large_file_upload_state.has_error():
                raise AlreadyFailed(large_file_upload_state.get_error_message())

            try:
                with upload_source.open() as file:
                    offset, content_length = part_range
                    file.seek(offset)
                    range_stream = RangeOfInputStream(file, offset, content_length)
                    input_stream = ReadingStreamWithProgress(range_stream, part_progress_listener)
                    hashing_stream = StreamWithHash(input_stream)
                    length_with_hash = content_length + hashing_stream.hash_size()
                    response = self.session.upload_part(
                        bucket_id, file_id, part_number, length_with_hash, HEX_DIGITS_AT_END,
                        hashing_stream
                    )
                    assert hashing_stream.hash == response['contentSha1']
                    return response

            except B2Error as e:
                if not e.should_retry_upload():
                    raise
                exception_list.append(e)
                self.account_info.clear_bucket_upload_data(bucket_id)

        large_file_upload_state.set_error(str(exception_list[-1]))
        raise MaxRetriesExceeded(self.MAX_UPLOAD_ATTEMPTS, exception_list)
