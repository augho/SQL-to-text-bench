import string

def char_classes_of(s: str):
    res = {
        "has_upper": False,
        "has_lower": False,
        "has_digit": False,
        "has_punct": False,
        "has_space": False,
    }

    for char in s:
        if char.isupper(): 
            res["has_upper"] = True
        if char.islower(): 
            res["has_lower"] = True
        if char.isdigit():
            res["has_digit"] = True
        if char.isspace(): 
            res["has_space"] = True    
        if char in string.punctuation:
            res["has_punct"] = True
        # early exit if all found
        if all(res.values()):
            break
    return res


def common_prefix(s1: str, s2: str):
    i = 0
    smallest_length = min(len(s1), len(s2))
    while i < smallest_length and s1[i] == s2[i]:
        i += 1
    return s1[:i]

# def common_prefixes(sample_values, prefix_len=6, top_n=5):
#     prefs = Counter()
#     for v in sample_values:
#         if v is None:
#             continue
#         s = str(v)
#         if len(s) >= 1:
#             prefs[s[:prefix_len]] += 1
#     return prefs.most_common(top_n)