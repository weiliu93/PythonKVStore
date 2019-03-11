class MemoryBlock(object):
    """
    Append only memory block
    """

    def __init__(self, block_id, block_size, memory_segments):
        self.__block_id = block_id
        self.__memory_segments = memory_segments
        # segment we are working on
        self.__current_segment_index = 0
        # offset in current segment
        self.__current_segment_offset = memory_segments[0].start_offset
        # prepare all segment's length prefix sum for binary search
        self.__segment_length_prefix_sum = []
        for segment in self.__memory_segments:
            self.__segment_length_prefix_sum.append(
                segment.end_offset - segment.start_offset
            )
            if len(self.__segment_length_prefix_sum) > 1:
                self.__segment_length_prefix_sum[
                    -1
                ] += self.__segment_length_prefix_sum[-2]
        self.__block_size = block_size

    @property
    def block_id(self):
        return self.__block_id

    @property
    def memory_segments(self):
        return self.__memory_segments

    @property
    def used_memory(self):
        used = (
            0
            if self.__current_segment_index == 0
            else self.__segment_length_prefix_sum[self.__current_segment_index - 1]
        )
        used += (
            self.__current_segment_offset
            - self.__memory_segments[self.__current_segment_index].start_offset
        )
        return used

    @property
    def free_memory(self):
        return self.__block_size - self.used_memory

    @property
    def block_size(self):
        return self.__block_size

    @property
    def current_offset(self):
        ans = (
            self.__segment_length_prefix_sum[self.__current_segment_index - 1]
            if self.__current_segment_index > 0
            else 0
        )
        ans += (
            self.__current_segment_offset
            - self.__memory_segments[self.__current_segment_index].start_offset
        )
        return ans

    def write(self, byte_data):
        """
        Since block's write is append-only, so we don't need some argument like offset,
        all result will be appended to the tail
        """

        # accept byte data only
        if isinstance(byte_data, str):
            byte_data = byte_data.encode("utf-8")
        assert isinstance(byte_data, bytes)

        write_offset, total_length = 0, len(byte_data)
        assert (
            total_length <= self.free_memory
        ), "total length of data is {}, free memory in current block is {}".format(
            total_length, self.free_memory
        )

        # all data are written or out of memory
        while total_length > 0 and self.__current_segment_index < len(
            self.__memory_segments
        ):
            current_segment = self.__memory_segments[self.__current_segment_index]
            segment_free_memory = (
                current_segment.end_offset - self.__current_segment_offset
            )
            if total_length <= segment_free_memory:
                # write data
                write_data = byte_data[write_offset : write_offset + total_length]
                current_segment.pool.write(self.__current_segment_offset, write_data)
                # update offset in current segment
                self.__current_segment_offset += total_length
                write_offset += total_length
                total_length = 0
            else:
                # write data
                write_data = byte_data[
                    write_offset : write_offset + segment_free_memory
                ]
                current_segment.pool.write(self.__current_segment_offset, write_data)
                # need to use a new segment
                total_length -= segment_free_memory
                self.__current_segment_index += 1
                write_offset += segment_free_memory
                if self.__current_segment_index < len(self.__memory_segments):
                    self.__current_segment_offset = self.__memory_segments[
                        self.__current_segment_index
                    ].start_offset
                else:
                    self.__current_segment_offset = 0

        return write_offset

    def read(self, offset, length):
        assert offset >= 0 and length >= 0, "offset and length should be positive"
        # binary search the starting segment
        low, high = 0, len(self.__segment_length_prefix_sum)
        while low < high:
            mid = low + (high - low) // 2
            if self.__segment_length_prefix_sum[mid] < offset + 1:
                low = mid + 1
            else:
                high = mid
        # delegate read operations to underlying pools
        offset -= self.__segment_length_prefix_sum[low - 1] if low - 1 >= 0 else 0
        read_data = bytearray()
        while low < len(self.__memory_segments) and length > 0:
            data = self.__memory_segments[low].pool.read(offset, length)
            read_data.extend(data)
            low += 1
            # offset will always be zero besides first pool
            offset = 0
            length -= len(data)
        return bytes(read_data)

    def rewind(self, offset):
        """
        rewind write pointer to offset position
        """
        assert (
            offset >= 0 and offset < self.__block_size
        ), "offset should be in range(%d -> %d)".format(0, self.__block_size - 1)
        low, high = 0, len(self.__memory_segments)
        while low < high:
            mid = low + (high - low) // 2
            if self.__segment_length_prefix_sum[mid] < offset + 1:
                low = mid + 1
            else:
                high = mid
        offset -= self.__segment_length_prefix_sum[low - 1] if low - 1 >= 0 else 0
        self.__current_segment_index = low
        self.__current_segment_offset = (
            self.__memory_segments[self.__current_segment_index].start_offset + offset
        )

    def __str__(self):
        return "block id is: {}, used memory is: {}, free memory is: {}, block size is: {}".format(
            self.__block_id, self.used_memory, self.free_memory, self.__block_size
        )

    def __repr__(self):
        return self.__str__()
