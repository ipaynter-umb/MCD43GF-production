dict1 = {'test1': 1,
         'test2': 2}

dict2 = {'test3': 3,
         'test4': 4}

print(dict1)

dict2 = {**dict1, **dict2}

print(dict2)