class MemorySegment(object):
    """
    Memory segment in one pool
    """

    def __init__(self, pool, start_offset, end_offset, length):
        self.pool = pool
        self.start_offset = start_offset
        self.end_offset = end_offset
        self.length = length

    def __str__(self):
        return 'Pool: {}, pool start offset: {}, pool end offset: {}, segment length: {}' \
            .format(self.pool, self.start_offset, self.end_offset, self.length)