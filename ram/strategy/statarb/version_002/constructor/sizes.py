
class SizeContainer(object):

    def __init__(self, n_days):
        self.n_days = n_days
        self.sizes = {}
        self.index = 0

    def update_sizes(self, sizes):
        self.sizes[self.index] = sizes
        # Clean out old
        if (self.index - self.n_days) in self.sizes:
            del self.sizes[self.index - self.n_days]
        self.index += 1

    def get_sizes(self):
        # Init output with all seccods
        output = {x: 0 for x in set(sum([x.keys() for x
                                         in self.sizes.values()], []))}
        for i in self.sizes.keys():
            for j in self.sizes[i].keys():
                output[j] += self.sizes[i][j]

        return output

    def to_json(self):
        packet = {
            'n_days': self.n_days,
            'sizes': self.sizes,
            'index': self.index
        }
        return packet

    def from_json(self, packet):
        self.n_days = packet['n_days']
        self.sizes = packet['sizes']
        self.index = packet['index']
