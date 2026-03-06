def compute_hhi(distribution):
    """
    Calculates the Herfindahl-Hirschman index of an entity distribution.
    From investopedia: The HHI is calculated by squaring the market share of each firm competing in a market and then
    summing the resulting numbers. It can range from close to 0 to 10,000, with lower values indicating a less
    concentrated market. The U.S. Department of Justice considers a market with an HHI of less than 1,500 to be a
    competitive marketplace, an HHI of 1,500 to 2,500 to be a moderately concentrated marketplace,
    and an HHI of 2,500 or greater to be a highly concentrated marketplace.
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
