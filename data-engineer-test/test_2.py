
def reverse_string(s: str):
    reversed_s = ""
    for char in s:
        reversed_s = char + reversed_s
    return reversed_s

assert reverse_string("abcd") == "dcba"
assert reverse_string("a3bE5dQPos") == "soPQd5Eb3a"
assert reverse_string("aka$aka") == "aka$aka"