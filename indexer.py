import multiprocessing
from config import *
from threading import Thread
from multiprocessing import Process
# import json
import xml.etree.ElementTree as etree
# import time
import re
import Stemmer
from collections import defaultdict
# import nltk
# from nltk.corpus import stopwords
import heapq
import sys
import os
import shutil
from bz2file import BZ2File

# nltk.download('stopwords')

class WikiReader():
    def __init__(self,callback):
        self.req_namespace = ARTICLE_NAMESPACE
        self.read_stack = []
        self.text = ""
        self.title = ""
        self.read_namespace = None
        self.count_articles = 0
        self.page_callback = callback

    def extract_name(self,x):
        return  x[x.rfind('}')+1:]

    def parse(self):
        # with BZ2File("../../../Downloads/chunks/wikichunk.bz2") as xml_dump:
        for event, elem in etree.iterparse(WIKI_DUMP_PATH, events=('start', 'end')):
            name_tag = self.extract_name(elem.tag)
            # print("Name: ",name_tag)
            if event == 'start':
                if name_tag == 'page':
                    self.text = ""
                    self.title = ""
            else:
                if name_tag == 'title':
                    self.title = elem.text
                elif name_tag == 'text':
                    self.text = elem.text
                elif name_tag == 'page':
                    self.count_articles += 1
                    # print(self.title)
                    if not(self.text == "" or self.title == "" or self.title is None or self.text is None):
                        self.page_callback((self.title, self.text))
                elem.clear()
        while(not(aq.empty())):
            pass



def process_article():
    indexer = Indexer()
    while not(shutdown_proc.value and aq.empty()):
        page_title,page_text = aq.get()
        indexer.process(page_title,page_text)
        fq.put(({
            "t":indexer.title,
            "i":indexer.infobox,
            "b":indexer.body,
            "c":indexer.category,
            "l":indexer.links,
            "r":indexer.references,
        },indexer.title_text
        ))
        
    while(not(fq.empty())):
        pass

def write_out():
    global shutdown_proc
    global indexes_set
    global tokens_set
    global page_count

    partial_dict = defaultdict(lambda: defaultdict(lambda:defaultdict(int)))
    count_articles = 0
    titles = []
    while not(shutdown_proc.value and fq.empty()):
        proc_info,title= fq.get()
        # indexes_set.update(set(stat_info['indexes']))
        # tokens_set.update(set(stat_info['tokens']))

        # print(proc_info['t'])
        doc_key = f'd{count_articles}'
        titles.append(title)
        for key in proc_info:
            text_list = proc_info[key]
            for word in text_list:
                partial_dict[word][doc_key][key] += 1
        count_articles += 1

        if count_articles % 50000 == 0:
            title_file = open(f'{INVERTED_INDEX_PATH}/indexes/titles/index{int(count_articles/50000) -1}','w+',encoding='utf-8')
            title_file.write('\n'.join(titles))
            titles.clear()

        if count_articles % 10000 == 0:
            new_file = open(f'{INVERTED_INDEX_PATH}/temp/index{page_count}','w+',encoding='utf-8')
            key_list = sorted(partial_dict.keys())
            store_line = ""
            for key in key_list:
                store_line += key+':'
                for document in partial_dict[key]:
                    store_line += f'{document}'
                    store_line += ''.join(''.join((type,str(num))) for (type,num) in partial_dict[key][document].items())
                store_line += '\n'

            new_file.write(store_line)
            new_file.close()
            # wq.put((partial_dict.copy(),page_count))
            page_count += 1
            partial_dict.clear()
    
    title_file = open(f'{INVERTED_INDEX_PATH}/indexes/titles/index{int(count_articles/50000)}','w+',encoding='utf-8')
    title_file.write('\n'.join(titles))
    titles.clear()
    
    new_file = open(f'{INVERTED_INDEX_PATH}/temp/index{page_count}','w+',encoding='utf-8')
    key_list = sorted(partial_dict.keys())
    store_line = ""
    for key in key_list:
        store_line += key+':'
        for document in partial_dict[key]:
            store_line += f'{document}'
            store_line += ''.join(''.join((type,str(num))) for (type,num) in partial_dict[key][document].items())
        store_line += '\n'
    new_file.write(store_line)
    new_file.close()
    page_count += 1

class Indexer:
    def __init__(self):
        self.title = ""
        self.text = ""
        self.infobox = ""
        self.body = ""
        self.category = ""
        self.links = ""
        self.references = ""
        self.title_text = ""
        # self.tokens = ""

    
    def deriveinfobox(self):
        pp = self.text[self.text.find('{{Infobox'):].split('\n')
        # print(pp)
        for i in range(1,len(pp)):
            # print(pp[i])
            if(len(pp[i]) > 0 and pp[i][0] == '}'):
                break
            else:
                pp[i] = pp[i][pp[i].find('=')+1:]
                self.infobox += ' ' + pp[i]
        # print(self.infobox)

    def derivecategory(self):
        self.category = ' '.join(re.findall(r'\[\[Category:(.*?)]]',self.text,flags=re.MULTILINE))
    

    def derivelinks(self):
        link = re.search(r'==External links==((.|\n)*?)\n\n',self.text,flags=re.MULTILINE)
        if link:
            self.links = link.group(1)
            # print(link.group(1))
    
    def derivereferences(self):
        reference = re.search(r'==References==((.|\n)*?)\n\n',self.text,flags=re.MULTILINE)
        if reference:
            self.references = reference.group(1)
            # print(reference.group(1))

    def derivebody(self):
        self.body = self.text.replace(self.infobox,'')
        self.body = self.body.replace(self.category,'')
        self.body = self.body.replace(self.links,'')
        self.body = self.body.replace(self.references,'')

    def process(self,title,text):
        self.title = title
        self.text = text
        self.title_text = title
        self.infobox = ""
        self.body = ""
        self.category = ""
        self.links = ""
        self.references = ""
        # self.tokens = text

        self.deriveinfobox()
        self.derivecategory()
        self.derivelinks()
        self.derivereferences()
        self.derivebody()
        # self.derivetokens()

        # print(self.title)
        # print(self.infobox)
        # print(self.links)
        # print(self.references)
        # print(self.category)

        ## Case Folding
        # print(self.category)
        self.title = self.tokeniser(self.title)
        self.text = self.tokeniser(self.text)
        self.category = self.tokeniser(self.category)
        self.references = self.tokeniser(self.references)
        self.links = self.tokeniser(self.links)
        self.infobox = self.tokeniser(self.infobox)
        self.body = self.tokeniser(self.body)
        
    # def derivetokens(self):
    #     text = self.tokens
    #     text = re.sub(r'\—|\%|\$|\||\.|\*|\[|\]|\:|\;|\,|\{|\}|\(|\)|\=|\+|\-|\_|\#|\!|\`|\"|\?|\/|\>|\<|\&|\\|\u2013|\n', r' ', text)
    #     text = text.split()
    #     self.tokens = text
        

    def tokeniser(self,text):
        text = text.lower()
        # re.sub(r'http\S+', '', str)
        text = re.sub(r'(http|www)\S+', ' ', text, flags=re.MULTILINE)
        text = re.sub(r'&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-fA-F]{1,6});', ' ', text, flags=re.MULTILINE)
        text = re.sub(r"\s*[^A-Za-z0-9]+\s*", " ", text, flags=re.MULTILINE)
        numbers = re.findall(r" \b\d{1,4}\b",text, flags=re.MULTILINE)
        text = re.sub(r"[0-9]", " ", text, flags=re.MULTILINE)
        text = text.split()
        ## Remove stop words and length of word < 2
        text = [word for word in text if len(word)>2 and not(word in stop)]
        text = stemmer.stemWords(text)
        for number in numbers:
            text.append(number.strip())
        return text     

def def_value():
    return("")

def write_to_disk():
    global shutdown_final
    global query_list
    global num_indices
    
    unpacked_dictionary = {
        "t":defaultdict(def_value),
        "b":defaultdict(def_value),
        "i":defaultdict(def_value),
        "c":defaultdict(def_value),
        "l":defaultdict(def_value),
        "r":defaultdict(def_value)
        }     
    file_counter = {}
    
    sparse_list = ["c","l","r","t"]
    large_list = ["b","i"]
    for key in query_list:
        file_counter[key] = 0

    while not(shutdown_final and wq.empty()):
        final_dictionary,page_num = wq.get()
        for key in final_dictionary:
            posting_list = final_dictionary[key].strip()
            prev_char = ''
            prev_docid = ''
            number_string = ''
            # print(posting_list)
            for i in posting_list[1:]:
                if i.isalpha():
                    if i == 'd':
                        # unpacked_dictionary[prev_char][key] += prev_docid 
                        unpacked_dictionary[prev_char][key] += prev_docid + 'o' + number_string
                        prev_docid = ''
                    else:
                        if prev_docid == '':
                            prev_docid = f'd{number_string}'
                        else:
                            # unpacked_dictionary[prev_char][key] += prev_docid 
                            unpacked_dictionary[prev_char][key] += prev_docid + 'o' + number_string
                        prev_char = i
                    number_string = ''
                else:
                    number_string += i

        if((int(page_num)+1) % 10 == 0):
            for key in sparse_list:
                # print(str(int((page_num+1)/10)-1))
                if len(unpacked_dictionary[key].keys()) != 0:
                    num_indices += len(unpacked_dictionary[key].keys())
                    fil = open(INVERTED_INDEX_PATH + '/indexes/' +  key + '/' + key+ str(file_counter[key] ),'w+')
                    fil_off = open(INVERTED_INDEX_PATH + '/indexes/' + key + '/' + key + str(file_counter[key])+'off','w+')
                    document_mapping[key].append(list(unpacked_dictionary[key].keys())[0])
                    for word in unpacked_dictionary[key]:
                        fil_off.write(word+':'+str(fil.tell())+ '\n') 
                        fil.write(word+':'+unpacked_dictionary[key][word]+'\n')
                    fil.close()
                    fil_off.close()
                    unpacked_dictionary[key].clear()
                    file_counter[key] += 1


        for key in large_list:
            if len(unpacked_dictionary[key].keys()) != 0:
                num_indices += len(unpacked_dictionary[key].keys())
                fil = open(INVERTED_INDEX_PATH + '/indexes/' + key + '/' + key + str(file_counter[key]),'w+')
                fil_off = open(INVERTED_INDEX_PATH + '/indexes/' +  key + '/' + key + str(file_counter[key])+'off','w+')
                document_mapping[key].append(list(unpacked_dictionary[key].keys())[0])
                for word in unpacked_dictionary[key]:
                    fil_off.write(word+':'+str(fil.tell())+'\n')
                    fil.write(word+':'+unpacked_dictionary[key][word]+'\n')
                fil_off.close()
                fil.close()
                unpacked_dictionary[key].clear()
                file_counter[key] += 1

    for key in sparse_list:
        if len(unpacked_dictionary[key].keys()) != 0:
            num_indices += len(unpacked_dictionary[key].keys())
            fil = open(INVERTED_INDEX_PATH + '/indexes/' + key + '/' + key + str(file_counter[key]),'w+')
            fil_off = open(INVERTED_INDEX_PATH + '/indexes/' + key + '/' + key + str(file_counter[key]) + 'off','w+')
            document_mapping[key].append(list(unpacked_dictionary[key].keys())[0])
            for word in unpacked_dictionary[key]:
                fil_off.write(word+':'+str(fil.tell()) +'\n') 
                fil.write(word+':'+unpacked_dictionary[key][word]+'\n')
            fil.close()
            fil_off.close()
            unpacked_dictionary[key].clear()
            file_counter[key] += 1

def kwaymerge():
    global page_count
    global INVERTED_INDEX_PATH
    indices = []
    posting_list = []
    final_dictionary = defaultdict(def_value)
    file_done = []
    for i in range(page_count):
        indices.append((0,0))
        posting_list.append("")
        file_done.append(0)

    file_dir = []

    for i in range(page_count):
        fil = open(f'{INVERTED_INDEX_PATH}/temp/index{i}','r',encoding= 'utf-8')
        file_dir.append(fil)

    for i in range(page_count):
        lin = file_dir[i].readline()[:-1]
        word, posting = lin.split(':')
        indices[i] = (word,i)
        posting_list[i] = posting

    heapq.heapify(indices)

    counter = 0
    num_files = 0
    prev_word = ''
    state = 0
    while not(all(file_done)):
        if(len(indices) == 0):
            break
        word,file_index = heapq.heappop(indices)
        final_dictionary[word] += posting_list[file_index]
        lin = file_dir[file_index].readline()[:-1]
        if lin == "":
            file_done[file_index] = 1
            file_dir[file_index].close()
        else:
            new_word,new_posting = lin.split(':')
            heapq.heappush(indices,(new_word,file_index))
            posting_list[file_index] = new_posting
        
        if prev_word != word:
            counter += 1
            state = 0

        prev_word = word
        
        if counter % 5000 == 0 and state == 0:
            wq.put((final_dictionary,num_files))
            num_files += 1
            state = 1
            final_dictionary.clear()

    while(not(wq.empty())):
        pass
                     

if __name__ == "__main__":
    WIKI_DUMP_PATH = sys.argv[1]
    INVERTED_INDEX_PATH = sys.argv[2]
    STAT_FILE = sys.argv[3]
    os.makedirs(f"{INVERTED_INDEX_PATH}/temp", exist_ok=True)
    os.makedirs(f"{INVERTED_INDEX_PATH}/indexes", exist_ok=True)
    
    shutdown = False
    manager = multiprocessing.Manager()
    fq = manager.Queue(maxsize=QUEUE_SIZE)
    aq = manager.Queue(maxsize=QUEUE_SIZE)
    wq = manager.Queue(maxsize=QUEUE_SIZE)
    num_indices = 0

    query_list = ["b","c","i","l","r","t",'titles']
    for key in query_list:
        os.makedirs(f"{INVERTED_INDEX_PATH}/indexes/{key}", exist_ok=True)
    # tokens_set = set()
    # indexes_set = set()
    document_mapping = {}
    for key in query_list:
        document_mapping[key] = []
    
    
    shutdown_proc = manager.Value('b',False)
    stop = set(open('wiki_stop.txt','r').read().split())
    stemmer = Stemmer.Stemmer('english')
    page_count = 0

    reader = WikiReader(aq.put)

    processes = []
    for i in range(4):
        process = Process(target=process_article)
        processes.append(process)
        process.start()

    write_thread = Thread(target=write_out)
    write_thread.start()

    reader.parse()
    print(reader.count_articles)
    shutdown_proc.value = True

    print("Shutdown initiated")
    write_thread.join()

    shutdown_final = False
    write_disk= Thread(target=write_to_disk)
    write_disk.start()

    kwaymerge()
    shutdown_final = True

    write_disk.join()
    
    print("Index created successfully")

    fil = open(f'{INVERTED_INDEX_PATH}/indexes/vocab_map.txt','w+')
    for key in document_mapping:
        fil.write(' '.join(document_mapping[key])+'\n')
    
    fil.close()
    if aq.empty() and fq.empty():
        for process in processes:
            process.kill()

    print(num_indices)
    # fil = open(STAT_FILE,'w+')
    # fil.write(str(len(tokens_set))+'\n' + str(len(indexes_set)))
    # fil.close()


    try:
        shutil.rmtree(f'{INVERTED_INDEX_PATH}/temp')
    except OSError as e:
        print("Error: %s : %s" % (f'{INVERTED_INDEX_PATH}/temp', e.strerror))
