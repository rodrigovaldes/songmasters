

## Create a function to distribute anything on the nodes

def factorial(number):
    '''
    Returns the factorial of a number
    Input:
        number = integer
    Output:
        number = integer
    '''
    if number == 1:
        return 1
    else:
        return number * factorial(number - 1)

def trunk_factorial(number):
    '''
    Returns the combinations, assuming number choose 2
    '''
    return int((number * (number - 1)) / 2)

def create_appropiate_dictionary(elements):
    '''
    Returns a dictonary with empty lists as initials values
    '''
    dicty = {}
    if type(elements) == type([]):
        for element in elements:
            dicty[element] = []
    elif type(elements) == type(1):
        for i in range(elements):
            dicty[i] = []

    return dicty


def compose_number(len_list, num):
    '''

    '''

    if num > len_list - 1:
        new_num = num - len_list
    else:
        new_num = num

    return new_num


def distribute(num):
    '''
    Gives the optimal distribution of elements for comparisons
    '''

    if num % 2 == 1:
        # The total number of elements is unpair
        # Each will get the same number of elements
        elements_by_node = int(trunk_factorial(num) / num)

        dict_task = create_appropiate_dictionary(num)

        local_number = 0
        for i in range(num):
            local_number += 1
            for j in range(elements_by_node):
                dict_task[i].append(compose_number(num, local_number + j))

    return dict_task
