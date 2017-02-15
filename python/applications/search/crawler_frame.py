import logging
from datamodel.search.datamodel import ProducedLink, OneUnProcessedGroup, robot_manager
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re, os
from time import time
import urlparse
import io

# try:
#     # For python 2
#     from urlparse import urlparse, parse_qs, urljoin
# except ImportError:
#     # For python 3
#     from urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
url_count = (set() 
    if not os.path.exists("successful_urls.txt") else 
    set([line.strip() for line in open("successful_urls.txt").readlines() if line.strip() != ""]))
MAX_LINKS_TO_DOWNLOAD = 3000

@Producer(ProducedLink)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):

    def __init__(self, frame):
        self.starttime = time()
        # Set app_id <student_id1>_<student_id2>...
        self.app_id = "0972787_0972788"
        # Set user agent string to IR W17 UnderGrad <student_id1>, <student_id2> ...
        # If Graduate studetn, change the UnderGrad part to Grad.
        self.UserAgentString = "IR W17 UnderGrad 0972787, 0972788"
		
        self.frame = frame
        assert(self.UserAgentString != None)
        assert(self.app_id != "")
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def initialize(self):
        self.count = 0
        l = ProducedLink("http://www.ics.uci.edu/", self.UserAgentString)
        print l.full_url
        self.frame.add(l)

    def update(self):
        for g in self.frame.get(OneUnProcessedGroup):
            print "Got a Group"
            outputLinks, urlResps = process_url_group(g, self.UserAgentString)
            for urlResp in urlResps:
                if urlResp.bad_url and self.UserAgentString not in set(urlResp.dataframe_obj.bad_url):
                    urlResp.dataframe_obj.bad_url += [self.UserAgentString]
            for l in outputLinks:
                # if not is_valid(l):
                #     print "outputLinks invalid"
                #     print "-------------------------------"
                if is_valid(l) and robot_manager.Allowed(l, self.UserAgentString):
                    lObj = ProducedLink(l, self.UserAgentString)
                    self.frame.add(lObj)
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def shutdown(self):
        global avgDownloadTime
        global dic
        global invalidCount
        global mostOutLinks
        global pageSize

        #print "downloaded ", len(url_count), " in ", time() - self.starttime, " seconds."
        #avgDownloadTime = len(url_count) / (time() - self.starttime)
        file_object = open('Analytics.txt', 'w')
        file_object.write("Different urls from each subdomains: \n")
        for key in dic:
            file_object.write(str(key) + ": " + str(dic[key]) + "\n")
        file_object.write("\nNumber of invalid links: " + str(invalidCount) + "\n")
        file_object.write("The page with the most out links: " + str(mostOutLinks) + "\n")
        #file_object.write("The average download time per URL: " + str(avgDownloadTime) + " second\n")
        file_object.write("The average download size per URL: " + str(pageSize / len(url_count)) + " bytes\n")
        file_object.close( )

def save_count(urls):
    global url_count
    urls = set(urls).difference(url_count)
    url_count.update(urls)
    if len(urls):
        with open("successful_urls.txt", "a") as surls:
            surls.write(("\n".join(urls) + "\n").encode("utf-8"))

        for url in urls:
            urlParsed = urlparse.urlparse(url)
            hostname = urlParsed.hostname
            domain = hostname.split(".")
            for i in range(0, len(domain) - 1):
                subdomain = combine(domain[i:])
                if subdomain in dic.keys():
                    dic[subdomain] += 1
                else:
                    dic[subdomain] = 1

        if len(url_count) < MAX_LINKS_TO_DOWNLOAD:
            temp_object = open('temp.txt', 'w')
            temp_object.write(str(len(dic)) + "\n")
            for key in dic:
                temp_object.write(str(key)+ "\n")
                temp_object.write(str(dic[key]) + "\n")
            temp_object.write(str(invalidCount) + "\n")
            temp_object.write(str(mostOutLinks) + "\n")
            temp_object.write(str(mostOutLinksCount) + "\n")
            temp_object.write(str(pageSize) + "\n")
        else:
            os.remove('temp.txt')

def process_url_group(group, useragentstr):
    global invalidCount
    rawDatas, successfull_urls = group.download(useragentstr, is_valid)
    
    if successfull_urls:
        save_count(successfull_urls)
    return extract_next_links(rawDatas)
    
#######################################################################################
'''
STUB FUNCTIONS TO BE FILLED OUT BY THE STUDENT.
'''

dic = dict()
invalidCount = 0
avgDownloadTime = 0
mostOutLinks = None
mostOutLinksCount = 0
pageSize = 0

#refresh the vars used last time
if os.path.exists("temp.txt"):
    input = open('temp.txt', 'r')
    dicLen = int(input.readline())
    for i in range(0,dicLen):
        key = input.readline()[:-1] #remove "\n"
        value = int(input.readline())
        dic[key] = value
    invalidCount = int(input.readline())
    mostOutLinks = input.readline()[:-1] #remove "\n"
    mostOutLinksCount = int(input.readline())
    pageSize = int(input.readline())
    input.close()


def combine(domains):
    result = ""
    for d in domains:
        result += d + "."
    return result[:-1]
     
def extract_next_links(rawDatas):
    global dic
    global mostOutLinks
    global mostOutLinksCount
    global pageSize

    outputLinks = list()
    #print rawDatas
    '''
    rawDatas is a list of objs -> [raw_content_obj1, raw_content_obj2, ....]
    Each obj is of type UrlResponse  declared at L28-42 datamodel/search/datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.

    Suggested library: lxml
    '''
    
    domainPath = set()
    for t in rawDatas:
        if t.is_redirected:
            url = t.final_url
        else:
            url = t.url
        content = t.content
        if content == "":
            continue
        pageSize += len(content)
        page = etree.HTML(content.lower())
        hrefs = page.xpath(u"//a")
        
        hrefCount = len(hrefs)
        if hrefCount > mostOutLinksCount:
            mostOutLinks = url
            mostOutLinksCount = hrefCount
            
        for href in hrefs:
            rawHref = href.get("href")
            absHref = urlparse.urljoin(url, rawHref)
            
            pos = absHref.find("?")
            path = absHref[:pos]
            if path not in domainPath:
                domainPath.add(path)  
                outputLinks.append(absHref)      
    if outputLinks:
        file_object = open('OutputLinks.txt', 'a')
        file_object.write("1: " + str(outputLinks) + "\n")
        file_object.close()
    else:
        file_object = open('OutputLinks.txt', 'a')
        file_object.write("2: " + str(rawDatas) + "\n")
        file_object.close()
    return (outputLinks, [])

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.

    This is a great place to filter out crawler traps.
    '''
    global invalidCount
    parsed = urlparse.urlparse(url)
    file_object = open('Invalid.txt', 'a')
    if parsed.scheme not in set(["http", "https"]): #not start with http or https
        invalidCount += 1
        file_object.write("1: " + url.encode('utf8') + "\n")
        #print "1: " + url
        return False
    if re.match(r"^.*?(/.+?/).*?\1.*$|^.*?/(.+?/)\2.*$", url.lower()): #duplicate path in url #https://support.archive-it.org/hc/en-us/articles/208332963-Modify-your-crawl-scope-with-a-Regular-Expression
        invalidCount += 1
        file_object.write("2: " + url.encode('utf8') + "\n")
        #print "2: " + url
        return False
    if re.match(r"^.*calendar.*$", url.lower()): #url contatins "calendar" #https://support.archive-it.org/hc/en-us/articles/208332963-Modify-your-crawl-scope-with-a-Regular-Expression
        invalidCount += 1
        file_object.write("3: " + url.encode('utf8') + "\n")
        #print "3: " + url
        return False
    if re.match(r".*\.php.*\.php.*$", url.lower()): #duplicate ".php" in url e.g. http://www.ics.uci.edu/grad/resources.php/faculty/area/community/degrees/index.php
        invalidCount += 1
        file_object.write("4: " + url.encode('utf8') + "\n")
        #print "4: " + url
        return False
    if re.match(r".*mailto:.*$", url.lower()): #url contatins "mailto:" e.g. http://www.ics.uci.edu/grad/resources.php/ugrad/courses/mailto: i%63%73%77%65%62m%61s%74%65r@%69c%73%2e%75c%69%2eedu
        invalidCount += 1
        file_object.write("5: " + url.encode('utf8') + "\n")
        #print "5: " + url
        return False
    hostname = parsed.hostname
    if not re.match(r"^.*uci\.edu$", hostname.lower()): #hostname does not end with "uci.edu" e.g. http://www.ics.uci.edu./ or http://www.ics.uci.eduCh6UCDHtExample.pdf
        invalidCount += 1
        file_object.write("6: " + url.encode('utf8') + "\n")
        #print "6: " + url
        return False
    if re.match(r"^.*/\.\./.*$", url.lower()): #invalid path e.g. #http://www.ics.uci.edu/~eppstein/pix/eosya/../iday/Argentina.html
        invalidCount += 1
        file_object.write("7: " + url.encode('utf8') + "\n")
        #print "7: " + url
        return False
    if re.match(r"^.*ganglia.*$", hostname.lower()): #hostname contatins "ganglia" # e.g. ganglia.ics.uci.edu
        invalidCount += 1
        file_object.write("8: " + url.encode('utf8') + "\n")
        #print "8: " + url
        return False
    if re.match(r"^.*\.(py|java|cpp)$", url.lower()): #url is a code file e.g. http://chemdb.ics.uci.edu/cgibin/Smi2DepictWeb.py
        invalidCount += 1
        file_object.write("9: " + url.encode('utf8') + "\n")
        #print "9: " + url
        return False
    if re.match(r"^.*\.h5$", url.lower()): #url is a .h5 file e.g. mlphysics.ics.uci.edu/data/hepjets/images/test_pile_5000000.h5
        invalidCount += 1
        file_object.write("10: " + url.encode('utf8') + "\n")
        #print "10: " + url
        return False
    try:
        isvalid = ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())
        if not isvalid:
            file_object.write("11: " + url.encode('utf8') + "\n")
            #print "11: " + url
            invalidCount += 1
        return isvalid

    except TypeError:
        print ("TypeError for ", parsed)
    
    file_object.close()



