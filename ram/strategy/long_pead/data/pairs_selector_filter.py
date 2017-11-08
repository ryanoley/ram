

class PairSelectorFilter(object):

    def __init__(self, n_pairs_per_seccode):
        self.n_pairs_per_seccode = n_pairs_per_seccode

    def filter(self, pair_info, spreads, zscores):
        pair_info = self._get_top_n_pairs_per_seccode(pair_info)
        spreads = self._double_flip_frame(spreads)
        spreads = spreads[pair_info.pair]
        zscores = self._double_flip_frame(zscores)
        zscores = zscores[pair_info.pair]
        return pair_info, spreads, zscores

    def _get_top_n_pairs_per_seccode(self, pair_info):
        temp = pair_info.copy()
        temp.columns = ['Leg2', 'Leg1', 'distances']
        pair_info = pair_info.append(temp).reset_index(drop=True)
        pair_info['distance_rank'] = \
            pair_info.groupby('Leg1')['distances'].rank()
        pair_info = pair_info[
            pair_info.distance_rank <= self.n_pairs_per_seccode]
        pair_info['pair'] = pair_info[['Leg1', 'Leg2']].apply(
            lambda x: '~'.join(x), axis=1)
        # SORT
        pair_info = pair_info.sort_values(['Leg1', 'distances'])
        return pair_info

    def _double_flip_frame(self, data):
        temp = data.copy()
        temp = temp * -1
        temp.columns = ['{}~{}'.format(y, x) for x, y in
                        [x.split('~') for x in temp.columns]]
        return data.join(temp)
