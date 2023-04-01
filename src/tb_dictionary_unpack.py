def unpack_dict(dictionary, unpacked_list=None, current_keys=None):
    # If no ongoing unpacked list provided
    if not unpacked_list:
        # Make empty list
        unpacked_list = []
    # If no ongoing current keys provided
    if not current_keys:
        # Make empty list
        current_keys = []
    # Retrieve a list of dictionary items
    list_of_items = list(dictionary.items())
    # While there are items remaining in the list
    while list_of_items:
        # Get an item (key: value)
        item_from_list = list_of_items.pop()
        # If the key points to a value that is a dictionary
        if isinstance(item_from_list[1], dict):
            # Unpack it (send it to this function using the current unpacked list and keys)
            unpacked_list = unpack_dict(item_from_list[1],
                                        unpacked_list=unpacked_list,
                                        current_keys=current_keys + [item_from_list[0]])
        # Otherwise... (value is not a dictionary)
        else:
            # Add a tuple of the list of keys and the value to the unpacked list
            unpacked_list.append((current_keys + [item_from_list[0]], item_from_list[1]))
    # Return the list of tuples where each is (key list, value)
    return unpacked_list


# Test dictionary
test_dict = {
    'blue':
        {
            '1': 'golf',
            '2': 'soccer'
        },
    'red':
        {
            '1':
                {
                    'fish': 'A',
                    'dolphin': 'B',
                    'tiger':
                        {
                            'cinema': 'C'
                        }
                }
        }
}

# Unpack the dictionary to a list of key lists and values
test_list = unpack_dict(test_dict)

# Print the list of tuples. Each tuple is (key list, value)
print(test_list)
