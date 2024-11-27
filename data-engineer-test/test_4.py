def word_mesh(words: list[str]):
    def find_mesh(a, b):
        # Find the longest suffix of `a` that is also a prefix of `b`
        for i in range(len(a)):
            if b.startswith(a[i:]):
                return a[i:]
        return ""

    result = ""
    for i in range(len(words) - 1):
        mesh = find_mesh(words[i], words[i + 1])
        if not mesh:  # If no mesh is found, return "failed to mesh"
            return "failed to mesh"
        result += mesh  # Append the mesh to the result string
    return result

# Run this file for test
assert word_mesh(["beacon", "condominium", "umbilical", "california"]) == "conumcal"
assert word_mesh(["allow", "lowering", "ringmaster", "terror"]) == "lowringter"
assert word_mesh(["abandon", "donation", "onion", "ongoing"]) == "dononon"
assert word_mesh(["jamestown", "ownership", "hippocampus", "pushcart", "cartographer", "pheromone"]) == "ownhippuscartpher"
assert word_mesh(["kingdom", "dominator", "notorious", "usual", "allegory"] ) == "failed to mesh"