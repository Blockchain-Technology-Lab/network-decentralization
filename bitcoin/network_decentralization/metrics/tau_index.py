def compute_tau_index(distribution, threshold):
    """
    Calculates the tau-decentralization index of an entity distribution.
    :param distribution: list of non-negative counts per entity, sorted in descending order
    :param threshold: float, the parameter of the tau-decentralization index, i.e. the threshold for the power
    ratio that is captured by the index (e.g. 0.66 for 66%)
    :returns: int that corresponds to the tau index of the given distribution, or None if total is 0
    """
    total = sum(distribution)
    if total == 0:
        return None
    tau_index, power_ratio_covered = 0, 0
    for amount in distribution:
        if power_ratio_covered >= threshold:
            break
        tau_index += 1
        power_ratio_covered += amount / total
    return tau_index
