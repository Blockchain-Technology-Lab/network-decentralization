from network_decentralization.metrics.tau_index import compute_tau_index


def compute_nakamoto_coefficient(distribution):
    """
    Calculates the Nakamoto coefficient of an entity distribution.
    :param distribution: list of non-negative counts per entity, sorted in descending order
    :returns: int that represents the Nakamoto coefficient of the given distribution, or None if the data is empty
    """
    return compute_tau_index(distribution, 0.5)
