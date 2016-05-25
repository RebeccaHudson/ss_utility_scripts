
items_per_query = 3   #crank waaay up in a minute..


# snamelessly plagarized from:
# http://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks-in-python
def chunk(input, size):
    return map(None, *([iter(input)] * size))



#FOR genomic location search...
#use a 25 item list.
item_list = list(range(34, 75)) 
print item_list

#########three_hundred_words = '/ua/rhudson/Documents/decid.txt'
chunked_list = chunk(item_list, items_per_query)
print(chunked_list)

for list_chunk in chunked_list:
  query_items_to_use = [str(x) for x in list_chunk if x is not None]
  in_clause_arg = "(" + ", ".join(query_items_to_use) + ")"
  query = "select dummy from fake where snpid in " + \
          in_clause_arg + " limit 12;" 
  print(in_clause_arg)













print("quitting early!")
exit()

a_few_snps = '/ua/rhudson/Documents/tinysnp.txt'
item_list = []
with open(a_few_snps, 'r') as f:
  for line in f:
   item_list.append(line.strip())

print("snps from the file")
print(item_list)

split_items = chunk(item_list, items_per_query)
print('chunked items')
print(split_items)

#returns first 3, next 3, next 3.
for list_chunk in split_items:
  query_items_to_use = [repr(x) for x in list_chunk if x is not None]
  in_clause_arg = "(" + ", ".join(query_items_to_use) + ")"
  query = "select dummy from fake where snpid in " + \
          in_clause_arg + " limit 12;" 
  print("in clause argument: " + in_clause_arg)
  print("full query  " + query)























