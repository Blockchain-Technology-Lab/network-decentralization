def compute_concentration_ratio(distribution, topn):
    """
    Calculates the n-concentration ratio of a distribution
    :param distribution: list of non-negative counts per entity, sorted in descending order
    :param topn: the number of top entities to consider
    :returns: float that represents the ratio of total count held by the top n entities (0 if total is 0)
    """
    total = sum(distribution)
    return sum(distribution[:topn]) / total if total else 0
