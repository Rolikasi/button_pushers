#%%
import re
import pandas as pd
import ast
from difflib import SequenceMatcher
import itertools
import numpy as np

df = pd.read_csv('scraped_protocol_6_kenesh.csv', index_col=0)
df.columns
#%%
#Кто чаще всех отсутствовал

replacement = {
    '\\xa0': ' ',
    'право голосования передано депутату,': '',
    'С.Шер-Нияз' : 'Шер-Нияз С.',
    'Мадеминов М.Г. Мамашова А.Т.': 'Мадеминов М.Г., Мамашова А.Т.',
    'право голосования по вопросу избрания председателя Комитета Жогорку Кенеша Кыргызской Республики по аграрной политике, водным ресурсам, экологии и региональному развитию передано депутату': '',
    'право голосования по вопросу избрания заместителя председателя Комитета Жогорку Кенеша Кыргызской Республики по правопорядку, борьбе с преступностью и противодействию коррупции передано депутату': '',
    "право голосования передано депутату": '',
    'в письменной форме': '',
    'а письменной форме': '',
    'право голосования «за» передано депутату': '',
    'по проекту Закона Кыргызской Республики «О внесении изменений в некоторые законодательные акты Кыргызской Республики \(в законы Кыргызской Республики «Об охране и использовании историко-культурного наследия»; «О телевидении и радиовещании»\)»': '',
    'по вопросу об избрании аудитора Счетной палаты Кыргызской Республики': '',
    'право голосования по вопросу избрания некоторых членов Правительства Кыргызской Республики передано депутату': "",
    'в части голосования по кандидатурам в состав Наблюдательного совета Общественной телерадиовещательной корпорации Кыргызской Республики': '',
    'по вопросу об избрании депутата Касымалиевой А.К. на должность заместителя Торага Жогорку Кенеша Кыргызской Республики': '',
    'по вопросу об избрании судьи Верховного суда Кыргызской Республики': '',
    'по вопросу «Об утверждении программы деятельности, определении структуры и состава Правительства Кыргызской Республики, представленных кандидатом на должность Премьер-министра Кыргызской Республики Жээнбековым С.Ш.»':''
}
df_deps = df.drop(['0 Зарегистрировались', '0 перерыв', '1 Зарегистрировались',
       '1 перерыв', '2 Зарегистрировались', '2 перерыв',
       '3 Зарегистрировались', '4 Зарегистрировались', '5 Зарегистрировались',
       '6 Зарегистрировались'], 1)

df_deps.no_deps = df_deps.no_deps.replace(replacement, regex=True)
df_deps.no_deps = df_deps.no_deps.str.split(', ')
df_deps['count_no_deps'] = df_deps.no_deps.str.len()
df_deps = df_deps.explode('no_deps').reset_index().rename(columns={'index': 'protocol_id'})
df_deps['from_dep'] = df_deps.no_deps.str.extract(r'(.*?)\s*\(', expand=False).str.strip()
df_deps['to_dep'] = df_deps.no_deps.str.extract(r'\(([^)]+)', expand=False).str.strip()
df_deps['miss_dep'] = df_deps[df_deps.from_dep.isnull()].no_deps.str.strip()
df_deps['no_dep'] = df_deps.miss_dep.combine_first(df_deps.from_dep).str.strip()
name_replace = {'Алимбеков Н.': 'Алимбеков Н.К.',
 'Жунус уулу А.': 'Жунус уулу Алтынбек',
 'Тусунбаев А.А.': 'Турсунбаев А.А.',
 'Маннанов И. А.': 'Маннанов И.А.',
 'Жээнбеков А.Ш.': 'Жеенбеков А.Ш.',
 'Гаипкулов И.Т.': 'Гайпкулов И.Т.',
 'Казакбаев Р.А': 'Казакбаев Р.А.',
 'Артыков А.': 'Артыков А.А.',
 'Самат Г.К.': 'Самат Гульнара Клара',
 'Самат Г.-К.': 'Самат Гульнара Клара',
 'Маматов А.': 'Маматов А.М.',
 'Сурабалдиева Э.К.': 'Сурабалдиева Э.Ж.',
 'Бакчиев Дж.А.': 'Бакчиев Ж.А.',
 'Бакчиев А.Дж.': 'Бакчиев Ж.А.',
 'Бакчиеву Дж.А.': 'Бакчиеву Ж.А.',
 'Бакчиеву А.Дж.': 'Бакчиеву Ж.А.',
 'Шайназарову Т.У.': 'Шайназаров Т.У.',
 'Жутанов А.С.': 'Жутанов А.Т.',
 'Ибраимжанов Б. С.': 'Ибраимжанов Б.С.',
 'Сарсеитов И.З.': 'Сарсейитов И.З.',
 'Бабанов О.Т': 'Бабанов О.Т.',
 'Шадиев А.А': 'Шадиев А.А.',
 '':'не указано'}

to_dep_replace = {
 'Турусбекову Ч.А.': 'Турсунбекову Ч.А.',
 'Артыкову А.': 'Артыкову А.А.',
 'ОмуркуловуИ.Ш.': 'Омуркулову И.Ш.',
 'Строковой А.Э.': 'Строковой Е.Г.',
 'Турускулову К.Ж.': 'Турускулову Ж.К.',
 'Сулайманов А.Т.': 'Сулайманову А.Т.',
 'Бекешову Д.Д.': 'Бекешеву Д.Д.',
 'Байбакпаеву Э.Дж.': 'Байбакпаеву Э.Ж.',
 'Айдарову А.С.': 'Айдарову С.А.',
 'Шарипову З.Э.': 'Шарапову З.Э.',
 'Зулушеву К.А.': 'Зулушеву К.Т.',
 'Сыдыкову Б.С.': 'Сыдыкову Б.У.',
 'Сабирову А.С.': 'Сабирову М.Э.',
 'бабанову О.Т.': 'Бабанову О.Т.'
}

df_deps.no_dep = df_deps.no_dep.replace(name_replace)
df_deps.from_dep = df_deps.from_dep.replace(name_replace)
df_deps.miss_dep = df_deps.miss_dep.replace(name_replace)
df_deps.to_dep = df_deps.to_dep.replace(to_dep_replace)
# %%
# export deps files
for column in ['no_dep', 'miss_dep', 'from_dep', 'to_dep']:
    df_deps[column].value_counts().rename_axis('dep').to_frame(column).to_csv('export/' + column +'.csv')
df_deps[['protocol_id', 'title', 'date', 'url', 'count_no_deps', 'no_dep',
       'miss_dep', 'from_dep', 'to_dep']].to_csv('export/exploded_no_deps.csv')

concat_dfs = []
for name in ['no_dep', 'miss_dep', 'from_dep']:
    concat_dfs.append(pd.read_csv('export/' + name +'.csv', index_col='dep'))

pd.concat(concat_dfs, axis=1).to_csv('export/miss_no_from_dep.csv')


# %%
def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()
def find_similar(df, col):
    cleanedList = [x for x in df[col].unique() if str(x) != 'nan']
    found_similar = {}
    for name, nextname in itertools.combinations(cleanedList, 2):
        if similar(name, nextname) > 0.8:
            print("similar", name, 'and', nextname, similar(name, nextname))
            found_similar.update({nextname: name})
    return found_similar

for col in ['no_dep', 'miss_dep', 'from_dep', 'to_dep']:
    print(find_similar(df_deps, col))


# %%

df_regs = df.copy()
df_regs['last_reg'] = df_regs.ffill(axis=1).iloc[:, -1]
df_regs.loc[216, 'last_reg'] = df_regs.loc[216, '1 Зарегистрировались']
df_regs.loc[216, '0 перерыв'] = 'до 14.00 часов' # ошибка парсера
df_regs.loc[215, 'last_reg'] = np.nan
df_regs.last_reg = df_regs.last_reg.astype(float)
df_regs = df_regs.rename(columns= {'0 Зарегистрировались': 'first_reg'})
export_mean = df_regs.describe()[['count_no_deps', 'first_reg', 'last_reg']].loc[['min', 'max', 'mean']]
export_mean.to_csv('export/mean_min_max_regs.csv')
#%%
reg_cols = ['title', 'count_no_deps', 'first_reg', 'last_reg', 'url']
df_regs.sort_values('first_reg').head(10)[reg_cols].to_csv('export/first_reg_top_10_min.csv')
df_regs.sort_values('last_reg').head(10)[reg_cols].to_csv('export/last_reg_top_10_min.csv')
df_regs.sort_values('first_reg', ascending=False, na_position='last').head(10)[reg_cols].to_csv('export/first_reg_top_10_max.csv')
df_regs.sort_values('last_reg', ascending=False, na_position='last').head(10)[reg_cols].to_csv('export/last_reg_top_10_max.csv')

# %%
