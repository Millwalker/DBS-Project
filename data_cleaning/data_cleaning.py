import json
import collections
import csv

from country_list import countries_for_language

with open('original_21_07_20.json','r') as json_file:
    original=json.load(json_file)

with open('population_density.json','r') as json_file:
    pop_den_data=json.load(json_file)

#Datentypänderung, day, month, year zu Int
for r in original['records']:
    r['day']=int(r['day'])
    r['month']=int(r['month'])
    r['year']=int(r['year'])

ocountries={}

for i in original['records']:
    count=i['geoId']
    if count not in ocountries:
        ocountries[count]=i['countriesAndTerritories']

count=dict(countries_for_language('en'))

no_countries=[]
for key in ocountries:
    if key not in count:
        no_countries.append(key)

jpn_conf=ocountries[no_countries[0]] #first entry

i=0 #delete international conveyance japan
cur_len=len(original['records'])
while i<cur_len:
    if original['records'][i]['countriesAndTerritories']==jpn_conf:
        original['records'].pop(i)
        cur_len-=1
    else:
        i+=1

#renaming Cumulative_number_for_14_days..., casting to float, filling empty spaces with zero
for x in original['records']:
    x['cases_per_100000_last_14_days']= x.pop("Cumulative_number_for_14_days_of_COVID-19_cases_per_100000")
    if x['cases_per_100000_last_14_days']=="":
        x['cases_per_100000_last_14_days']=0.0
    else:
        x['cases_per_100000_last_14_days']=float(x['cases_per_100000_last_14_days'])

countries={}

#listing all countries in dict
for rec in original['records']:
    land=rec['countriesAndTerritories']
    if land in countries:
        pass
    else:
        countries[land]=1

pop_den_dict={}
#putting the population densities of countries in a dict
for c in pop_den_data:
    old_name=c['country']
    new_name=""
    for char in old_name:
        if char==' ':
            new_name+='_'
        else:
            new_name+=char
    pop_den_dict[new_name]=c['density']

no_density=[]
for key in countries:
    if key not in pop_den_dict:
        no_density.append(key)

#adding population_densities that are left by hand, copied from Terminal
#15 of them can be found under another name in the original data_set
#others will be added from wikipedia or worldpopulationreview
#https://worldpopulationreview.com/
#https://en.wikipedia.org/wiki/Caribbean_Netherlands
#https://en.wikipedia.org/wiki/Guernsey
#https://en.wikipedia.org/wiki/Jersey
#https://en.wikipedia.org/wiki/Kosovo

pop_den_dict['Bonaire, Saint Eustatius and Saba']=77.0 #Caribbean_Netherlands
pop_den_dict['British_Virgin_Islands']=198.68
pop_den_dict['Brunei_Darussalam']=81.39696395
pop_den_dict['Cote_dIvoire']=78.83405346
pop_den_dict['Curaçao']=370.0 #worldpopulationreview
pop_den_dict['Czechia']=137.6028879
pop_den_dict['Democratic_Republic_of_the_Congo']=37.08259236
pop_den_dict['Eswatini']=67.0 #Swaziland worldpopulationreview
pop_den_dict['Falkland_Islands_(Malvinas)']=0.28
pop_den_dict['Fiji']=48.35703339
pop_den_dict['Guernsey']=965.0
pop_den_dict['Guinea_Bissau']=54.0 #worldpopulationreview
pop_den_dict['Holy_See']=924.0
pop_den_dict['Isle_of_Man']=149.0 #worldpopulationreview
pop_den_dict['Jersey']=912.0
pop_den_dict['Kosovo']=159.0
pop_den_dict['Libya']=3.795632381
pop_den_dict['Montenegro']=45.0 #worldpopulationreview
pop_den_dict['Russia']=8.822079555
pop_den_dict['Serbia']=99.0 #worldpopulationreview
pop_den_dict['Sint_Maarten']=1261.0 #worldpopulationreview
pop_den_dict['Sri_Lanka']=345.558922
pop_den_dict['Taiwan']=658.0 #worldpopulationreview
pop_den_dict['Timor_Leste']=85.27
pop_den_dict['Turks_and_Caicos_islands']=39.64736842
pop_den_dict['United_Republic_of_Tanzania']=63.0 #worldpopulationreview
pop_den_dict['United_States_of_America']= 35.76608858
pop_den_dict['United_States_Virgin_Islands']=305.6485714

#adding all populations densities
for rec in original['records']:
    rec['population_density']=pop_den_dict[rec['countriesAndTerritories']]

no_density=[]
for key in countries:
    if key not in pop_den_dict:
        no_density.append(key)

#print(no_density)

#Covid19 ID
#listing continents and countries in dict
continents=collections.OrderedDict()
for rec in original['records']:
    cur_cont=rec['continentExp']
    cur_country=rec['countriesAndTerritories']
    if cur_cont not in continents:
        continents[cur_cont]=collections.OrderedDict()
    if cur_country not in continents[cur_cont]:
        continents[cur_cont][cur_country]=1

Covid19IDs=[]

#creating Covid19 IDs
for rec in original['records']:
    tmpID=""
    tmpID+= str(list(continents.keys()).index(rec['continentExp']))
    land_str=str(list(continents[rec['continentExp']].keys()).index(rec['countriesAndTerritories']))
    if len(land_str)==1:
        tmpID+='0'
    tmpID+=land_str
    tmpID+= str(rec['dateRep'])
    Covid19IDs.append(tmpID)

#insert Ids
for i in range(len(original['records'])):
    original['records'][i]['covid19_Id']=Covid19IDs[i]

#creating data_sets for database
covid19_occurrences={'covid19_occurrences':[]}
countries={'countries':[]}
has={'has':[]}

#h_geoIds=[]
count_attr=[[],[],[],[],[],[]]

used_countries={}
for rec in original['records']:
    tmp19={'covid19_Id':rec['covid19_Id'],'date':rec['dateRep'],'day':rec['day'],'month':rec['month'],'year':rec['year'],'cases':rec['cases'],'deaths':rec['deaths'],'cases_per_100000_last_14_days':rec['cases_per_100000_last_14_days']}
    tmp_h={'geoId':rec['geoId'],'covid19_Id':rec['covid19_Id']}
    #h_geoIds.append(rec['geoId'])
    if rec['geoId'] not in used_countries:
        tmp_c={'geoId':rec['geoId'],'country_Name':rec['countriesAndTerritories'],'countryterritoryCode':rec['countryterritoryCode'],'continent':rec['continentExp'],'population2019':rec['popData2019'],'population_density':rec['population_density']}
        countries['countries'].append(tmp_c)
        used_countries[rec['geoId']]=1
        count_attr[0].append(rec['geoId'])
        count_attr[1].append(rec['countriesAndTerritories'])
        count_attr[2].append(rec['countryterritoryCode'])
        count_attr[3].append(rec['continentExp'])
        count_attr[4].append(rec['popData2019'])
        count_attr[5].append(rec['population_density'])
    covid19_occurrences['covid19_occurrences'].append(tmp19)
    has['has'].append(tmp_h)


#with open('covid19_occurrences.json', 'w') as f:
#  json.dump(covid19_occurrences, f, ensure_ascii=False, indent=2)

#with open('countries.json', 'w') as f:
#  json.dump(countries, f, ensure_ascii=False, indent=2)

#with open('has.json', 'w') as f:
#  json.dump(has, f, ensure_ascii=False, indent=2)
num_records=len(original['records'])

"""
with open('has_n.json', 'w') as f:
    for i in range(num_records):
        f.write(json.dumps({
            'geoId': original['records'][i]['geoId'],
            'covid19_Id': Covid19IDs[i],
        }) + "\n")
"""

with open('covid19_occurrences.csv', 'w') as csvfile:
    field_names = ['covid19_Id','date','day','month','year','cases','deaths','cases_per_100000_last_14_days']
    writer = csv.DictWriter(csvfile, fieldnames=field_names)
    writer.writeheader()
    for rec in original['records']:
        writer.writerow({
            'covid19_Id': rec['covid19_Id'],
            'date': rec['dateRep'],
            'day': rec['day'],
            'month': rec['month'],
            'year': rec['year'],
            'cases': rec['cases'],
            'deaths': rec['deaths'],
            'cases_per_100000_last_14_days': rec['cases_per_100000_last_14_days']
        })

with open('countries.csv', 'w') as csvfile:
    field_names = ['geoId', 'country_Name', 'countryterritoryCode', 'continent', 'population2019', 'population_density']
    writer = csv.DictWriter(csvfile, fieldnames=field_names)
    writer.writeheader()
    for i in range(len(count_attr[0])):
        writer.writerow({
            'geoId': count_attr[0][i],
            'country_Name': count_attr[1][i],
            'countryterritoryCode': count_attr[2][i],
            'continent': count_attr[3][i],
            'population2019': count_attr[4][i],
            'population_density': count_attr[5][i]
        })

with open('has.csv', 'w') as csvfile:
    field_names = ['geoId', 'covid19_Id']
    writer = csv.DictWriter(csvfile, fieldnames=field_names)
    writer.writeheader()
    for i in range(num_records):
        writer.writerow({
            'geoId': original['records'][i]['geoId'],
            'covid19_Id': Covid19IDs[i],
        })

print("Successfully created the files covid19_occurrences.csv, countries.csv and has.csv in your current folder.")
"""
Fragen:
1. Kann der Datensatz einfach ausgetauscht werden, wenn wir ihn ohnehin in den Anderen übergeführt hätten?
2. '_' Chars durch ' ' ersetzen?
3. Groß- und Kleinschreibung wichtig?
Als Nächstes:
https://www.digitalocean.com/community/tutorials/how-to-install-and-use-postgresql-on-ubuntu-18-04-de
https://info.crunchydata.com/blog/fast-csv-and-json-ingestion-in-postgresql-with-copy
https://www.liquidweb.com/kb/creating-and-deleting-a-postgresql-database/
"""
