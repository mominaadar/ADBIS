from collections import defaultdict
from data_prep import *
import time
    



def append_point_to_list(data, point):
    output = [tuple(item + point) for item in data]
    return output
    
def hash_join_new(tables, int_to_string):
    # Build hash table for follows.object = friendOf.subject
    result = []

    # Build hash tables for join attributes
    follows_hash = defaultdict(list)
    friendOf_hash = defaultdict(list)
    likes_hash = defaultdict(list)
    hasReview_hash = defaultdict(list)

   

    # Build hash tables for each join attribute
    #print(tables['follows'])
    for row in tables['wsdbm:follows']:
        follows_hash[row[0]].append(row)
        #follows_hash[row[1]].append(row)
    for row in tables['wsdbm:friendOf']:
        friendOf_hash[row[0]].append(row)
        #friendOf_hash[row[1]].append(row)
    for row in tables['wsdbm:likes']:
        likes_hash[row[0]].append(row)
        #likes_hash[row[1]].append(row)
    for row in tables['rev:hasReview']:
        hasReview_hash[row[0]].append(row)
        #hasReview_hash[row[1]].append(row)
    
    #hash_table_join1 = {}
    hash_table_join1 = defaultdict(list)

    for key, val in follows_hash.items():
        for row_foll in list(set(val)):
            follow_object = row_foll[1]
            if friendOf_hash.get(follow_object,None) is not None:
                hash_table_join1[follow_object] = append_point_to_list(friendOf_hash[follow_object], row_foll)
    
    
    
    
    
    hash_table_join2 = defaultdict(list)

    #friendOf.object = likes.subject
    for key, val in hash_table_join1.items():
        for row_foll in list(set(val)):
            friendof_object = row_foll[1]
            if likes_hash.get(friendof_object,None) is not None:
                hash_table_join2[friendof_object] = append_point_to_list(likes_hash[friendof_object], row_foll)
    
    
    # # Build hash table for likes.object = hasReview.subject
    #final_result = []
    final_result = defaultdict(list)
    for key, val in hash_table_join2.items():
         for row_foll in list(set(val)):
            likes_object = row_foll[1]
            if hasReview_hash.get(likes_object,None) is not None:
                final_result[likes_object] = append_point_to_list(hasReview_hash[likes_object], row_foll)
    
    #print(len(final_result[0]))

    final_output = []
    for key, val in final_result.items():
        
        for res in val:
            set_out = (res[7],res[6],res[5],res[3],res[1])
            final_output.append(set_out)
    print("Result Length",len(final_output))

    return final_output




def merge_join(tables, int_to_string):

    '''
    join1 = follows.object = friendOf.subject
    join2 = friendOf.object = likes.subject
    join3 = likes.object = hasReview.subject    
    '''
    # Sort the tables based on the join attributes
    follows_table = sorted(tables['wsdbm:follows'], key=lambda row: row[1])
    friendOf_subject_table = sorted(tables['wsdbm:friendOf'], key=lambda row: row[0])
  

    
    # Perform the first join (follows.object = friendOf.subject)
    join1 = []
    i = j = 0
    #print(len(follows_table), len(friendOf_subject_table))
    
    while i < len(follows_table) and j < len(friendOf_subject_table):
        follows_obj = follows_table[i][1]
        friendOf_subject = friendOf_subject_table[j][0]

        if follows_obj == friendOf_subject:
            join1.append(follows_table[i] + friendOf_subject_table[j])
            j += 1
            #i += 1
        
        elif follows_obj < friendOf_subject:
            i += 1
        else:
            j += 1

   
    join1_result = sorted(join1, key=lambda row: row[3])
    likes_subject_table = sorted(tables['wsdbm:likes'], key=lambda row: row[0])
    #hasReview_table = sorted(tables['rev:hasReview'], key=lambda row: row[0])
    join2 = []
    j = k = 0
    while j < len(join1_result) and k < len(likes_subject_table):
        friendOf_obj = join1_result[j][3]
        likes_subject = likes_subject_table[k][0]

        if friendOf_obj == likes_subject:
            join2.append(join1_result[j] + likes_subject_table[k])
            
            k += 1
            #j += 1
        elif friendOf_obj < likes_subject:
            j += 1
        else:
            k += 1
       

    likes_object_table = sorted(join2, key=lambda row: row[5])
    hasReview_table = sorted(tables['rev:hasReview'], key=lambda row: row[0])
    join3 = []
    j = k = 0
    while j < len(likes_object_table) and k < len(hasReview_table):
        likes_obj = likes_object_table[j][5]
        hasreview_subject = hasReview_table[k][0]

        if hasreview_subject == likes_obj:
            join3.append(likes_object_table[j] + hasReview_table[k])
            
            k += 1
            #j += 1
        elif likes_obj < hasreview_subject:
            j += 1
        else:
            k += 1
    
  
    # print("______________________xxxxxxxxxxxxxxxxxxxxxxxxxxxxx___________________________")
    # for rec in join3:
    #     print(int_to_string[rec[0]],int_to_string[rec[1]],int_to_string[rec[2]],int_to_string[rec[3]],int_to_string[rec[4]],int_to_string[rec[5]],int_to_string[rec[6]],int_to_string[rec[7]])
    
    print("Result Length",len(list(set(join3))))
    return list(set(join3))
    
# Call the function with your tables and int_to_string mapping

from multiprocessing import Pool, cpu_count

def process_chunk(chunk_data,friendOf_hash):#args1,args2):
    # Helper function to process a chunk of follows_hash
    
    
    hash_table_join1 = defaultdict(list)
    for chunk in chunk_data:
        for row_foll in list(set(chunk[1])):
            follow_object = row_foll[1]
            if friendOf_hash.get(follow_object, None) is not None:
                hash_table_join1[follow_object] = append_point_to_list(friendOf_hash[follow_object], row_foll)
    return hash_table_join1

def hash_join_new_multiprocessing(tables, int_to_string):
    # Build hash tables for join attributes
    follows_hash = defaultdict(list)
    friendOf_hash = defaultdict(list)

    for row in tables['wsdbm:follows']:
        follows_hash[row[0]].append(row)

    for row in tables['wsdbm:friendOf']:
        friendOf_hash[row[0]].append(row)

    # Determine the number of cores available
    num_cores = 8  # Update this value based on the number of cores you have

    # Split follows_hash into chunks
    follows_chunks = [list(follows_hash.items())[i::num_cores] for i in range(num_cores)]
    args_list = [(chunk, friendOf_hash) for chunk in follows_chunks]
    with Pool(processes=8) as pool:
        results = pool.starmap(process_chunk, [(chunk, friendOf_hash) for chunk in follows_chunks])
        #async_results = [pool.apply_async(process_chunk, args=args_list)]
   

    hash_table_join1 = defaultdict(list)
    for result in results:
        for key, value in result.items():
            hash_table_join1[key].extend(value)
    
    # rest of the is same like hash join, as idea is same.We only demonstrate parrelellism 
   




# Perform the join operations and measure the time taken
if __name__ == '__main__':
    table, string_to_int = prep_data()
    
    int_to_string = {value: key for key, value in string_to_int.items()}    

    start_time = time.perf_counter()
    hash_join_result = hash_join_new(table, int_to_string)
    hash_join_time = time.perf_counter() - start_time
  
    
    
    start_time = time.perf_counter()
    sort_merge_join_result = merge_join(table,int_to_string)
    sort_merge_join_time = time.perf_counter() - start_time
    # # print("\nSort-Merge Join Result:")
    
    print("Sort-Merge Join Time:", sort_merge_join_time)
    print("Hash Join Time:", hash_join_time)
    
    
    start_time = time.perf_counter()
    improved_has_join = hash_join_new_multiprocessing(table, int_to_string)
    improved_has_join_time = time.perf_counter() - start_time
    print("Hash Join Improved Time: (partial implementation)", improved_has_join_time)

    #print("Hash Join Result:")
    # for row in hash_join_result:
    # #   result.append(follows_row + friendOf_row[1] + likes_row[1] + hasReview_row)
    
    #     print(int_to_string[row[0]],int_to_string[row[1]],int_to_string[row[2]],int_to_string[row[3]],int_to_string[row[4]])
        
    
    # if len(hash_join_result) == len(sort_merge_join_result):
    #     for row in sort_merge_join_result:
    #         if row in hash_join_result:
    #             pass
    #         else:
    #             print("Do not match")
    # else:
    #     print("Lenght do not match, has lenght:{}, merge Result Length{}".format(len(hash_join_result),len(sort_merge_join_result)))
