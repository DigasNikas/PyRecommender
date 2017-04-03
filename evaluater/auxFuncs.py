


def uniqueNum(keydic, q):
    # gets a list (keydic) and a key (q)
    # if key is not in the list, adds it to the list
    # and assigns key an incremental numeric ID
    # returns the key ID in the keydic list.
    if q in keydic:
        return keydic[q]
    else:
        k = len(keydic) + 1
        keydic[q] = k
        return k


def incAppCount(keydicCntr, q):
    """
    gets a list (keydicCntr) and a key (q)
    if key is not in the list,
        adds it to the list and assigns initial value of 1
    else
        increases the app count by 1
    """
    if q in keydicCntr:
        keydicCntr[q] += 1
        return
    else:
        keydicCntr[q] = 1
        return


