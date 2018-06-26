#!_NS_INSX/python/bin/python
#-*- encoding: utf-8 -*-
# by libin on 2016-06-12

def _cmpBeginIpValue(a, b):
    if a[0] > b[0]:
        return 1
    elif a[0] < b[0]:
        return -1
    else:
        return 0

#input ip_value list format [[bengin_ip_value, end_ip_value, using_unit_name, id, ...], [...], ...]
def init(logger, ipvaluelist):
    sortlist = []

    #sort ipvaluelist by begin_ip_value
    logger.info("sort ipvaluelist by begin_ip_value")
    ipvaluelist.sort(cmp=_cmpBeginIpValue)

    #Remove the cross
    logger.info("ipvaluelist length: [%d]", len(ipvaluelist))
    logger.info("Remove the cross, Keep id larger values")
    for m in ipvaluelist:
        if len(sortlist) > 0:
            if m[0] <= sortlist[-1][1]:
                if m[3] > sortlist[-1][3]:
                    sortlist[-1] = m
            else:
                sortlist.append(list(m))
        else:
            sortlist.append(list(m))
    logger.info("after Remove the cross, the sortlist length:[%d]", len(sortlist))
    return sortlist

#The return value format [bengin_ip_value, end_ip_value, using_unit_name, id, ...] or None
def binarySearch(a, sortlist):
    low = 0
    high = len(sortlist) - 1
    result = None
    while (low <= high):
        mid = (low + high) / 2
        if (a < sortlist[mid][0]):
            high = mid - 1
        elif (a > sortlist[mid][1]):
            low = mid + 1
        else:
            result = sortlist[mid]
            break
    return result
