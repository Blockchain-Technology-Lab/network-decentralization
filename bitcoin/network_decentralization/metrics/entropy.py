from math import log
from network_decentralization.metrics.total_entities import compute_total_entities


def compute_entropy(distribution, alpha):
    """
    Calculates the entropy of an entity distribution.
    Pi is the relative frequency of each entity.
    Renyi entropy: 1/(1-alpha) * log2 (sum (Pi**alpha))
    Shannon entropy (alpha=1): −sum P(Si) log2 (Pi)
    Min entropy (alpha=-1): -log max Pi
    :param distribution: list of non-negative counts per entity, sorted in descending order
    :param alpha: the entropy parameter (depending on its value the corresponding entropy measure is used)
    :returns: a float that represents the entropy of the data or None if the data is empty
    """
    total = sum(distribution)
    if total == 0:
        return None
    if alpha == 1:
        entropy = 0
        for value in distribution:
            rel_freq = value / total
            if rel_freq > 0:
                entropy -= rel_freq * log(rel_freq, 2)
    else:
        if alpha == -1:
            entropy = -log(max(distribution) / total, 2)
        else:
            sum_freqs = 0
            for entry in distribution:
                sum_freqs += pow(entry / total, alpha)
            entropy = log(sum_freqs, 2) / (1 - alpha)

    return entropy


def compute_max_entropy(num_entities, alpha):
    return compute_entropy([1 for i in range(num_entities)], alpha)


def compute_entropy_percentage(distribution, alpha):
    if sum(distribution) == 0:
        return None
    try:
        total_entities = compute_total_entities(distribution)
        return compute_entropy(distribution, alpha) / compute_max_entropy(total_entities, alpha)
    except ZeroDivisionError:
        return 0
