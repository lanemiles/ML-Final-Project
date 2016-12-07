smash_map = {}

with open("simple.txt") as inp:

    for line in inp:

        pieces = line.split("\t")
        key = (pieces[0], pieces[1])
        if key not in smash_map:
            smash_map[key] = ""
        if pieces[2][-1] == "\n":
            pieces[2] = pieces[2][:-1]
        smash_map[key] += pieces[2].lower()

with open("simple_smash.txt", 'w') as output:
    for (key, value) in smash_map.items():
        output.write(value + "\n")