import re
import Stemmer
from bisect import bisect,bisect_left
from collections import defaultdict
# import os
# import sys

# INVERTED_INDEX_PATH = sys.argv[1]
# query_file = sys.argv[2]

def binarysearch(lis, ele):
    i = bisect_left(lis, ele)
    if i != len(lis) and lis[i] == ele:
        return i
    else:
        return -1

# def def_value():
#     return( {
#         "b":[],
#         "c":[],
#         "i":[],
#         "l":[], 
#         "r":[],
#         "t":[]
#     })

def tokeniser(text):
        text = text.lower()
        # re.sub(r'http\S+', '', str)
        #### text = re.sub(r'\—|\%|\$|\||\.|\*|\[|\]|\:|\;|\,|\{|\}|\(|\)|\=|\+|\-|\_|\#|\!|\`|\"|\?|\/|\>|\<|\&|\\|\n', r' ', text) 
        text = re.sub(r'(http|www)\S+', ' ', text, flags=re.MULTILINE)
        text = re.sub(r'&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-fA-F]{1,6});', ' ', text, flags=re.MULTILINE)
        text = re.sub(r"\s*[^A-Za-z0-9]+\s*", " ", text, flags=re.MULTILINE)
        text = text.split()
        ## Remove stop words and length of word < 2
        text = [word for word in text if len(word)>2 and not(word in stop)]
        text = stemmer.stemWords(text)
        return text 

def isfield(text):
    text_copy = text
    list_remove = ["b:","c:","i:","l:","r:","t:"]
    for item in list_remove:
        text = text.replace(item," ")
    if text == text_copy:
        return False
    else:
        return True

def processFieldQueries(query):
    global answers
    fields = query.split(':')
    search_dict = {}
    for index,field in enumerate(fields[1:]):
        required_type = fields[index][-1]
        search_dict[required_type] = tokeniser(field[:-1].strip())
    print(search_dict)
    answers += search(search_dict)


def processSimpleQueries(query):
    global answers
    global query_list
    search_dict = {}
    query_tokens = tokeniser(query)
    for query_type in query_list:
        search_dict[query_type] = query_tokens
    print(search_dict)
    answers += search(search_dict)

def search(search_dict):
    
    pass

# def build_json(text,text_app):
#     global query_list
#     for ind,word in enumerate(text_app):
#         for key in query_list:
#             # print(word,key)
#             tokens = []
#             offsets = []
#             document_id = bisect(vocab_dict[key],word) - 1
#             # print(document_id)
#             fil = open(INVERTED_INDEX_PATH + '/indexes/' +  key +'/' + key + str(document_id),'r',encoding='utf-8')
#             with open(INVERTED_INDEX_PATH  + '/indexes/' + key +'/' + key + str(document_id) +'off','r',encoding='utf-8') as off:
#                 offset_words = off.read().splitlines()
#             for line in offset_words:
#                 token,offset = line.split(':')
#                 tokens.append(token)
#                 offsets.append(offset)
#             # print(tokens)
#             pos = binarysearch(tokens,word)
#             # print(pos)
#             if pos == -1:
#                 search_dict[text[ind]][key].append(None)
#             else:
#                 req_offset = offsets[pos]
#                 fil.seek(int(req_offset))
#                 posting_list = fil.readline().strip().split(':')[1].split('d')[1:]
#                 search_dict[text[ind]][key] += posting_list

stop = set(open('wiki_stop.txt','r',encoding='utf-8').read().split())
stemmer = Stemmer.Stemmer('english')
query_list = ["b","c","i","l","r","t"]
answers = []
vocab_dict = {}
# search_dict = defaultdict(def_value)

# with open(f'{INVERTED_INDEX_PATH}/indexes/vocab_map.txt',encoding='utf-8') as json_file:
#     vocab_list = json_file.read().splitlines()

queries = ["Billie Jean michael jackson","b:Marc Spector i:Marvel Comics c:1980 comics debuts"]
# with open(query_file,encoding='utf-8') as json_file:
#     queries = json_file.read().splitlines()

# for i in range(len(query_list)):
#     vocab_dict[query_list[i]] = vocab_list[i].split()

for query in queries:
    print(isfield(query))
    if(isfield(query)):
        processFieldQueries(query)
    else:
        processSimpleQueries(query)


# print(vocab_dict)
# str1,str1_app = process_temp(queries)
# build_json(str1,str1_app)

# print(json.dumps(search_dict,indent=2))

    