with open('simple_no_tab.aligned') as f:
    with open('lower_simple_no_tab.aligned', 'w') as output:
        for line in f:
            new_line = line.lower()
            output.write(new_line)