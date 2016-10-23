from riko.modules.fetchdata import pipe
from riko.collections.sync import SyncPipe
from riko.modules import fetchdata, join

# url
poverty_url = 'opendata.go.ke/resource/i5bp-z9aq.json'
schools_url = 'opendata.go.ke/resource/fbd2-c7tq.json'

# Fetch KODI data
stream = pipe(conf={'url': poverty_url})
# print next (stream)

# Filtering and truncating
sort_conf = {'rule': {'sort_key': 'poverty_rate_2005_06_', 'sort_dir': 'desc'}}
filter_conf = {'rule': {'field': 'poverty_rate_2005_06_', 'op': 'greater', 'value': 70}}

stream = (
	SyncPipe('fetchdata', conf={'url':poverty_url})
	.filter(conf=filter_conf)
	.sort(conf=sort_conf)
	.truncate(conf={'count':'5'})
	.output)
# print list(stream)

# counting
next(fetchdata.pipe(conf={'url':schools_url}))
# #######################################################
count_conf = {'count_key':'division'}
stream = SyncPipe('fetchdata', conf={'url':schools_url}).count(conf=count_conf).output
# print list(stream)

#MoreSums
sum_conf = {'sum_key':'teachers_toilets', 'group_key':'county'}
stream = SyncPipe('fetchdata', conf={'url':schools_url}).sum(conf=sum_conf).output
# print list(stream)

# joining
#  join Kenya Primary School data to the poverty data.

# HACK: create a helper function to lower case the key name 
def lower_case(stream, key): 
    for item in stream:
        item[key] = item.get(key, '').lower()
        yield item

# joining them
poverty_stream = fetchdata.pipe(conf={'url': poverty_url})
schools_stream = fetchdata.pipe(conf={'url': schools_url})
join_conf = {'join_key': 'district_name', 'other_join_key': 'district'}

poverty_l_stream = lower_case(poverty_stream, join_conf['join_key'])
schools_l_stream = lower_case(schools_stream, join_conf['other_join_key'])
joined_stream = pipe(poverty_l_stream, conf=join_conf, other=schools_l_stream)
print next(joined_stream)
