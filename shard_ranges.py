"""Módulo para calcular rangos de shards para créditos y ratings."""

SHARD_ID_LIMIT_CREDITS = 490000  # Límite exclusivo
SHARD_ID_LIMIT_RATINGS = 26024290  # Límite exclusivo


def calculate_shard_ranges(count, is_ratings_join=False):
    """
    Calcula los rangos de shards para créditos o ratings.
    Args:
        count (int): Número de nodos.
        is_ratings_join (bool): Si es una unión de ratings, se aplican rangos específicos.
    Returns:
        list: Lista de tuplas con los rangos de shards.
    """

    if is_ratings_join and count > 1:
        # Rangos específicos para 6 nodos de ratings
        if count == 8:
            predefined_ranges = [
                (0, 586),  # Nodo 1
                (587, 1345),  # Nodo 2
                (1346, 2611),  # Nodo 3
                (2612, 4466),  # Nodo 4
                (4467, 20000),  # Nodo 5 - dividiendo el rango grande
                (20001, 36529),  # Nodo 6 - segunda parte del rango grande
                (36530, 106400),  # Nodo 7 - dividiendo el último rango
                (106401, 176271),  # Nodo 8 - segunda parte del último rango
            ]
            return predefined_ranges

        # Para otros casos (4 o menos nodos), usar distribución automática
        elif count <= 4:
            total_range = SHARD_ID_LIMIT_CREDITS + 1

            # If 4 or fewer nodes, distribute equally
            base_size = total_range // count
            remainder = total_range % count

            ranges = []
            current_start = 0

            for i in range(count):
                extra = 1 if i < remainder else 0
                current_end = current_start + base_size + extra - 1
                ranges.append((current_start, current_end))
                current_start = current_end + 1

            return ranges
        else:
            # First 4 nodes get 5% each, remaining nodes share the 80%
            total_range = SHARD_ID_LIMIT_RATINGS + 1
            small_shard_size = total_range // 300  # 5% each
            used_by_small_shards = 4 * small_shard_size  # 20% total
            remaining_range = total_range - used_by_small_shards  # 80% left
            remaining_nodes = count - 4
            base_size = remaining_range // remaining_nodes
            remainder = remaining_range % remaining_nodes

            ranges = []
            current_start = 0

            # First 4 shards get 5% each
            for i in range(4):
                current_end = current_start + small_shard_size - 1
                ranges.append((current_start, current_end))
                current_start = current_end + 1

            # Remaining nodes share the 80%
            for i in range(remaining_nodes):
                extra = 1 if i < remainder else 0
                current_end = current_start + base_size + extra - 1
                ranges.append((current_start, current_end))
                current_start = current_end + 1

            return ranges

    else:
        total_range = SHARD_ID_LIMIT_CREDITS + 1
        first_shard_size = total_range // 10  # First shard gets 10%
        second_shard_size = total_range // 4  # Second shard gets 25%
        remaining_range = total_range - (first_shard_size + second_shard_size)
        base_size = remaining_range // (count - 2) if count > 2 else 0
        remainder = remaining_range % (count - 2) if count > 2 else 0

        ranges = []
        current_start = 0

        # First shard
        current_end = current_start + first_shard_size - 1
        ranges.append((current_start, current_end))
        current_start = current_end + 1

        # Second shard
        current_end = current_start + second_shard_size - 1
        ranges.append((current_start, current_end))
        current_start = current_end + 1

        # Remaining shards
        for i in range(count - 2):
            extra = 1 if i < remainder else 0
            current_end = current_start + base_size + extra - 1
            ranges.append((current_start, current_end))
            current_start = current_end + 1

        return ranges
