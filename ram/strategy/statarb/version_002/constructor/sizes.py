import datetime as dt


class SizeContainer(object):

    def __init__(self, n_days):
        self.n_days = n_days
        self.sizes = {}

    def update_sizes(self, sizes, date):
        """
        Note: this implementation prevents two entries for same date.
        """
        self.sizes[date] = sizes
        # Clean out old
        dates = list(set(self.sizes.keys()))
        dates.sort()
        if len(dates) > self.n_days:
            d = dates.pop(0)
            del self.sizes[d]

    def get_sizes(self):
        # Init output with all seccods
        output = {x: 0 for x in set(sum([x.keys() for x
                                         in self.sizes.values()], []))}
        for i in self.sizes.keys():
            for j in self.sizes[i].keys():
                output[j] += self.sizes[i][j]

        return output

    def to_json(self):
        # Convert datetime to strings
        out_sizes = {}
        for k in self.sizes.keys():
            new_k = k.strftime('%Y%m%d')
            out_sizes[new_k] = self.sizes[k]
        packet = {
            'n_days': self.n_days,
            'sizes': out_sizes,
        }
        return packet

    def from_json(self, packet):
        self.n_days = packet['n_days']
        # Convert strings to datetime
        for k, v in packet['sizes'].iteritems():
            new_k = dt.date(int(k[:4]), int(k[4:6]), int(k[6:]))
            self.sizes[new_k] = v

    # Implementation-specific functionality
    def kill_seccode(self, seccode):
        for s in self.sizes.values():
            if seccode in s:
                del s[seccode]
        return

    # Checking/debugging functionality
    def _check_seccode(self, seccode):
        seccode_size = 0
        flag = False
        for s in self.sizes.values():
            if seccode in s:
                flag = True
                seccode_size += s[seccode]
        if flag:
            return seccode_size
        return False
