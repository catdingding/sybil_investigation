class IoUCalculator:
    def __init__(self, contributions, object_key_extractor, object_contribution_extractor, pair_txs_count_extractor=None):
        self.object_key_extractor = object_key_extractor
        self.object_contribution_extractor = object_contribution_extractor
        self.pair_txs_count_extractor = pair_txs_count_extractor
        self.objects = self.preprocess(contributions)

    def preprocess(self, contributions):
        objects = {}

        for contribution in contributions:
            key = self.object_key_extractor(contribution)
            objects.setdefault(key, {'key': key, 'contributions': set()})

            objects[key]['contributions'].add(self.object_contribution_extractor(contribution))

        return objects

    def extract_object_name(self, x):
        raise NotImplementedError

    def extract_object_contributions(self, x):
        raise NotImplementedError

    def compute_suspicious_pairs(self, min_iou, min_contribution_count):
        pairs = []
        filtered_accounts = [x for x in self.objects.values() if len(x['contributions']) >= min_contribution_count]

        for i, account_profile_x in enumerate(filtered_accounts):
            for account_profile_y in filtered_accounts[i + 1:]:

                iou = (
                    len(account_profile_x['contributions'] & account_profile_y['contributions']) /
                    len(account_profile_x['contributions'] | account_profile_y['contributions'])
                )

                if iou >= min_iou:
                    pair = [
                        account_profile_x['key'], account_profile_y['key'], iou,
                        min(len(account_profile_x['contributions']), len(account_profile_y['contributions']))
                    ]

                    if self.pair_txs_count_extractor:
                        pair.append(self.pair_txs_count_extractor(account_profile_x['key'], account_profile_y['key']))

                    pairs.append(pair)

        return pairs
