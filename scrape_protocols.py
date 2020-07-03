#%%
from bs4 import BeautifulSoup, NavigableString, Tag
import pandas as pd
import requests as req
import re

#%%
def get_protocol_info(page, url):
    soup = BeautifulSoup(page)
    title = soup.find('div', {'class', 'title'}).text.strip().lower()
    date = "".join(re.findall(r'от\s+([\d.]*)', title)[0])
    text = soup.find('div', {'class', 'text'})
    registrations = text.find_all(lambda tag:tag.name=="p" and "Зарегистрировались" in tag.text)
    count_deps_regs = [{str(idx) + ' Зарегистрировались' : get_number(registration)} for idx, registration in enumerate(registrations)]
    count_deps = dict((key,d[key]) for d in count_deps_regs for key in d)
    breaks = text.find_all(lambda tag:tag.name=="p" and "Объявляется перерыв" in tag.text)
    count_breaks = [{str(idx) + ' перерыв' : get_time(n_break)} for idx, n_break in enumerate(breaks)]
    breaks = dict((key,d[key]) for d in count_breaks for key in d)
    no_deputies_tag = text.find(lambda tag:tag.name=="p" and ("отсутств") in tag.text)
    try:
        no_deps = no_deputies_tag.text.strip()#.split(', ')
    except:
        no_deps = 'Все присутствовали'
    export = {"title" : title, "date": date, "no_deps": no_deps, 'url': url}
    export.update(count_deps)
    export.update(breaks)
    return export

def get_number(tag):
    return "".join(re.findall(r'\d+',tag.text))

def get_time(tag):
    time = re.findall(r'\d+\s+\w+',tag.text)
    if len(time) == 0:
        return "не указано время"
    else:
        return ' '.join(time)

def get_all_links(base_link, slice_link):
    result = []
    resp = req.get(base_link)
    tree = BeautifulSoup(resp.text, "lxml")
    for link in tree.findAll("div", {"class": "news-read-more"}):
        print(link.a['href'])
        result.append(slice_link + link.a['href'])
    return result

#%%
#get all links with protocols from kenesh.kg
search_link = "http://kenesh.kg/ru/article/list/21?page="
slice_link = 'http://kenesh.kg'
page_num = 31


urls = [get_all_links(search_link + str(number+1), slice_link) for number in range(page_num)]

#%%
#get info from protocols
res = []
for idx, url in enumerate(urls):
    print('check ', url)
    print(str(idx) + '/' + str(len(urls)))
    page = req.get(url).text
    res.append(get_protocol_info(page, url))

#%%
#clean no_deps columns
cols = ['title', 'date', 'count_no_deps', 'no_deps', 'url', '0 Зарегистрировались', '0 перерыв',
       '1 Зарегистрировались', '1 перерыв', '2 Зарегистрировались',
        '2 перерыв', '3 Зарегистрировались',
       '4 Зарегистрировались', '5 Зарегистрировались', '6 Зарегистрировались']

replacement = {
    "На заседании отсутствуют депутаты": "",
    "На заседании отсутствует депутат": "",
    "На внеочередном заседании отсутствуют депутаты": "",
    "\r\n": " ",
    "культурного наследия», «О телевидении": "культурного наследия»; «О телевидении",
    ":": ""
}

df_deps_buttons = pd.DataFrame(res)
df_deps_buttons.no_deps = df_deps_buttons.no_deps.replace(replacement, regex=True).str.strip()
df_deps_buttons['count_no_deps'] = df_deps_buttons.no_deps.str.split(', ').str.len()
df_deps_buttons.loc[df_deps_buttons.no_deps == 'Все присутствовали', 'count_no_deps'] = 0
df_deps_buttons[cols].to_csv('protocol_6_kenesh.csv')

#%%
