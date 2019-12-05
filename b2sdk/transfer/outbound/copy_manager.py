import logging
import six

from b2sdk.exception import (
    AlreadyFailed,
    MaxFileSizeExceeded,
)

from b2sdk.file_version import FileVersionInfoFactory
from b2sdk.raw_api import MetadataDirectiveMode
from b2sdk.utils import B2TraceMetaAbstract, choose_part_ranges, interruptible_get_result

from .large_file_upload_state import LargeFileUploadState

try:
    import concurrent.futures as futures
except ImportError:
    import futures

from .copy_source import CopySourcePart

logger = logging.getLogger(__name__)


@six.add_metaclass(B2TraceMetaAbstract)
class CopyManager:
    MAX_LARGE_FILE_SIZE = 10 * 1000 * 1000 * 1000 * 1000  # 10 TB

    def __init__(self, session, services, max_copy_workers=10):
        self.session = session
        self.services = services

        self.copy_executor = None
        self.max_workers = max_copy_workers

    @property
    def account_info(self):
        return self.session.account_info

    def set_thread_pool_size(self, max_workers):
        """
        Set the size of the thread pool to use for uploads and downloads.

        Must be called before any work starts, or the thread pool will get
        the default size of 1.

        :param int max_workers: maximum allowed number of workers in a pool
        """
        if self.copy_executor is not None:
            raise Exception('thread pool already created')
        self.max_workers = max_workers

    def get_thread_pool(self):
        """
        Return the thread pool executor to use for uploads and downloads.
        """
        if self.copy_executor is None:
            self.copy_executor = futures.ThreadPoolExecutor(max_workers=self.max_workers)
        return self.copy_executor

    def copy(
        self,
        copy_source,
        file_name,
        content_type=None,
        file_info=None,
        progress_listener=None,
        destination_bucket_id=None,
        min_large_file_size=None
    ):
        # We don't upload any large files unless all of the parts can be at least
        # the minimum part size.
        if min_large_file_size is None:
            min_large_file_size = self.account_info.get_minimum_part_size() * 2

        copy_small_file = False
        if copy_source.content_length is None:
            if copy_source.small_file_promise:
                copy_small_file = True
            else:
                # TODO: should be supported
                raise NotImplementedError('auto mode not supported')
        elif copy_source.content_length < min_large_file_size:
            copy_small_file = True

        if copy_small_file:
            # Run small copies in the same thread pool as large file copies,
            # so that they share resources during a sync.
            f = self.get_thread_pool().submit(
                self._copy_small_file,
                copy_source,
                file_name,
                content_type=content_type,
                file_info=file_info,
                destination_bucket_id=destination_bucket_id,
            )
            # TODO: validate `small_file_promise` with actual copied file size?
            return f.result()
        else:
            if progress_listener is None:
                # TODO: proper logic or proper error
                raise RuntimeError('progress listener is required for large file copy')
            return self._copy_large_file(
                copy_source,
                file_name,
                progress_listener,
                content_type=content_type,
                file_info=file_info,
                destination_bucket_id=destination_bucket_id,
            )

    def split_copy_source(self, copy_source):
        if copy_source.content_length is None:
            raise ValueError('cannot split CopySource of unknown length')
        source_offset = copy_source.offset or 0
        minimum_part_size = self.account_info.get_minimum_part_size()
        part_ranges = choose_part_ranges(copy_source.content_length, minimum_part_size)
        for part_number, (part_offset, part_length) in enumerate(part_ranges, 1):
            yield CopySourcePart(copy_source, source_offset + part_offset, part_length, part_number)

    def copy_part(
        self, large_file_id, copy_source_part, large_file_upload_state, finished_parts=None
    ):
        # Check if this part was uploaded before
        if finished_parts is not None and copy_source_part.part_number in finished_parts:
            # Report this part finished
            part = finished_parts[copy_source_part.part_number]
            large_file_upload_state.update_part_bytes(part.content_length)

            # Return SHA1 hash
            return {'contentSha1': part.content_sha1}

        # if another part has already had an error there's no point in
        # uploading this part
        if large_file_upload_state.has_error():
            raise AlreadyFailed(large_file_upload_state.get_error_message())

        response = self.session.copy_part(
            copy_source_part.file_id,
            large_file_id,
            copy_source_part.part_number,
            bytes_range=copy_source_part.get_bytes_range(),
        )
        # TODO: large_file_upload_state.update_part_bytes
        return response

    def _copy_small_file(
        self, copy_source, file_name, content_type=None, file_info=None, destination_bucket_id=None
    ):
        # no progress report - because there is nothing to report
        if copy_source.content_length is None and copy_source.offset is not None:
            raise NotImplementedError('copy offset of unknown length is not supported yet')

        bytes_range = copy_source.get_bytes_range()

        if content_type is None:
            metadata_directive = MetadataDirectiveMode.COPY
        else:
            if file_info is None:
                raise ValueError('file_info can be None only when content_type is also None')
            metadata_directive = MetadataDirectiveMode.REPLACE

        response = self.session.copy_file(
            copy_source.file_id,
            file_name,
            bytes_range=bytes_range,
            metadata_directive=metadata_directive,
            content_type=content_type,
            file_info=file_info,
            destination_bucket_id=destination_bucket_id
        )
        return FileVersionInfoFactory.from_api_response(response)

    def _copy_large_file(
        self,
        copy_source,
        file_name,
        progress_listener,
        content_type=None,
        file_info=None,
        destination_bucket_id=None
    ):
        if destination_bucket_id is None:
            # TODO: should be supported
            raise NotImplementedError(
                'checking for bucket_id of copy source file_id is not supported'
            )

        if content_type is None:
            # TODO: should be supported
            raise NotImplementedError('metadata copy directive for large files is not supported')

        if self.MAX_LARGE_FILE_SIZE < copy_source.content_length:
            raise MaxFileSizeExceeded(copy_source.content_length, self.MAX_LARGE_FILE_SIZE)

        # Set up the progress reporting for the parts
        progress_listener.set_total_bytes(copy_source.content_length)

        # Select the part boundaries
        copy_source_parts = list(self.split_copy_source(copy_source))

        # TODO: match unfinished large file and match parts - how?
        unfinished_file, finished_parts = None

        if unfinished_file is None:
            unfinished_file = self.services.large_file.start_large_file(
                destination_bucket_id, file_name, content_type, file_info
            )
        file_id = unfinished_file.file_id

        with progress_listener:
            large_file_upload_state = LargeFileUploadState(progress_listener)
            # Tell the executor to upload each of the parts
            part_futures = [
                self.get_thread_pool().submit(
                    self.copy_part,
                    file_id,
                    copy_source_part,
                    large_file_upload_state,
                    finished_parts=finished_parts,
                ) for copy_source_part in copy_source_parts
            ]

            # Collect the sha1 checksums of the parts as the uploads finish.
            # If any of them raised an exception, that same exception will
            # be raised here by result()
            part_sha1_array = [interruptible_get_result(f)['contentSha1'] for f in part_futures]

        # Finish the large file
        response = self.session.finish_large_file(file_id, part_sha1_array)
        return FileVersionInfoFactory.from_api_response(response)
