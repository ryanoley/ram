import datetime as dt


class SizeContainer(object):

    def __init__(self, n_days):
        self.n_days = n_days
        self.sizes = {}
        self.dates = []

    def update_sizes(self, sizes, date):
        self.sizes[date] = sizes
        self.dates.append(date)
        # Clean out old
        if len(self.dates) > self.n_days:
            d = self.dates.pop(0)
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
        out_dates = []
        for d in self.dates:
            out_dates.append(d.strftime('%Y%m%d'))
        packet = {
            'n_days': self.n_days,
            'sizes': out_sizes,
            'dates': out_dates
        }
        return packet

    def from_json(self, packet):
        self.n_days = packet['n_days']
        # Convert strings to datetime
        for k, v in packet['sizes'].iteritems():
            new_k = dt.date(int(k[:4]), int(k[4:6]), int(k[6:]))
            self.sizes[new_k] = v

        for d in packet['dates']:
            self.dates.append(dt.date(int(d[:4]), int(d[4:6]), int(d[6:])))
