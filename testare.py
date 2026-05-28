logsa = ["u1:login", "u2:login", "u1:view", "u3:login", "u1:logout", "u2:click", "u1:login"]


def generate_dict(logs):
    sorted_dict = {}
    for e in logs:
        if e.split(":")[0] in sorted_dict:
            if e.split(":")[1] in sorted_dict[e.split(":")[0]]:
                pass
            else:
                sorted_dict[e.split(":")[0]].append(e.split(":")[1])
        else:
            sorted_dict[e.split(":")[0]] = []
    new_sorted_dict = {}
    for k, v in sorted_dict.items():
        if len(v) >= 3:
            new_sorted_dict[k] = v
    return print(sorted_dict, new_sorted_dict)


generate_dict(logsa)
