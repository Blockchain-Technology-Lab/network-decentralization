def compute_total_entities(distribution):
    """
    Computes the number of entities with a positive count in the given distribution.
    :param distribution: list of non-negative counts per entity
    :returns: number of entities with count > 0
    """
    return len([v for v in distribution if v > 0])
