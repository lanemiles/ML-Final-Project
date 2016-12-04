# help us
word_count = {}

with open('sorted_normal_wc.txt') as file1:
    for line in file1:
        parts = line.split('\t')
        word = parts[0]
        count = int(parts[1])
        if word not in word_count:
            word_count[word] = 0
        word_count[word] += count

with open('sorted_simple_wc.txt') as file2:
    for line in file2:
        parts = line.split('\t')
        word = parts[0]
        count = int(parts[1])
        if word not in word_count:
            word_count[word] = 0
        word_count[word] += count

print len(word_count.keys())

# with open('total_wc.txt', 'w') as output:
#     for (word, count) in word_count.items():
#         output.write(word + "\t" + str(count) + "\n")
