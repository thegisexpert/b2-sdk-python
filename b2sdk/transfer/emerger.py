import logging

import six

from b2sdk.file_version import FileVersionInfoFactory
from b2sdk.utils import B2TraceMetaAbstract, interruptible_get_result

from .outbound.large_file_upload_state import LargeFileUploadState

logger = logging.getLogger(__name__)


@six.add_metaclass(B2TraceMetaAbstract)
class Emerger:
    def __init__(self, session, services, download_manager, upload_manager, copy_manager):
        self.session = session
        self.services = services
        self.download_manager = download_manager
        self.upload_manager = upload_manager
        self.copy_manager = copy_manager

    def emerge(
        self, bucket_id, emerge_ranges_iterator, file_name, content_type, file_info,
        progress_listener
    ):
        # TODO: this is only a draft implementation that support concat interface only for files > 5mb

        # TODO: we assume that there is more than one emerge range on iterator
        unfinished_file = self.services.large_file.start_large_file(
            bucket_id, file_name, content_type, file_info
        )
        file_id = unfinished_file.file_id

        total_bytes = 0
        part_futures = []
        with progress_listener:
            large_file_upload_state = LargeFileUploadState(progress_listener)
            for emerge_range in emerge_ranges_iterator:
                if emerge_range.destination_offset != total_bytes:
                    raise NotImplementedError('only non overlapping ranges supported')

                emerge_source_type = emerge_range.emerge_source.get_type()

                # TODO: remeber - this is only a non working draft
                if emerge_source_type == 'upload':
                    # TODO: probably UploadSource and CopySource should inherit from EmergeSource
                    #   then `get_source()` wouldn't be required here
                    upload_source = emerge_range.emerge_source.get_source()
                    total_bytes += upload_source.get_content_length()

                    # FIXME: what we actually should do here???
                    progress_listener.set_total_bytes(total_bytes)

                    upload_source_parts = self.upload_manager.split_upload_source(upload_source)
                    part_futures.extend(
                        self.upload_manager.get_thread_pool().submit(
                            self.upload_manager.upload_part,
                            bucket_id,
                            file_id,
                            upload_source_part,
                            large_file_upload_state,
                        ) for upload_source_part in upload_source_parts
                    )
                elif emerge_source_type == 'copy':
                    copy_source = emerge_range.emerge_source.get_source()
                    if copy_source.content_length is None:
                        raise NotImplementedError('unknonwn length copy sources not supported')

                    # FIXME: what we actually should do here???
                    total_bytes += copy_source.content_length

                    copy_source_parts = self.copy_manager.split_copy_source(copy_source)
                    part_futures.extend(
                        self.get_thread_pool().submit(
                            self.copy_manager.copy_part,
                            file_id,
                            copy_source_part,
                            large_file_upload_state,
                        ) for copy_source_part in copy_source_parts
                    )
                else:
                    raise NotImplementedError(
                        'not implemented emerge source: {0}'.format(emerge_source_type)
                    )

            part_sha1_array = [interruptible_get_result(f)['contentSha1'] for f in part_futures]

        # Finish the large file
        response = self.session.finish_large_file(file_id, part_sha1_array)
        return FileVersionInfoFactory.from_api_response(response)

    def concat(
        self, bucket_id, emrege_sources_iterator, file_name, content_type, file_info,
        progress_listener
    ):
        def emerge_ranges_generator():
            current_position = 0
            for emerge_source in emrege_sources_iterator:
                current_position, upload_range = self._wrap_emerge_range(
                    current_position, emerge_source
                )
                yield upload_range

        return self.emerge(
            emerge_ranges_generator, file_name, content_type, file_info, progress_listener
        )

    def _wrap_emerge_range(self, current_position, emerge_source):
        pass
