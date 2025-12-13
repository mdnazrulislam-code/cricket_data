arr = [1,2,3,4,5,6,7,8,9,10]


# def find_subarray_sum(arr, i, j):
#     subarray_sum = 0

#     for k in range(i, j+1):
#         subarray_sum += arr[k]
#     return subarray_sum



# print(find_subarray_sum(arr, 4, 7))


def has_subarray_sum(arr, k):
    seen_prefix = {0}
    current_prefix = 0


    for numm in arr:
        current_prefix += numm
        print(current_prefix)



        

has_subarray_sum(arr, 15)