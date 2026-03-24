def compute_hhi(distribution):
    """
    Calculates the Herfindahl-Hirschman index of an entity distribution.
    :param distribution: list of non-negative counts per entity, sorted in descending order
    :return: float between 0 and 10,000 that represents the HHI of the given distribution or None if the data is empty
    """
    total = sum(distribution)
    if total == 0:
        return None

    hhi = 0
    for count in distribution:
        hhi += pow(100 * count / total, 2)

    return hhi
