#!/bin/bash
# awful chickenscratch commenting below

# find first seen date
file_name=`ls /data/steam | head -n1`
file_name=${file_name%%.*}
first_seen_date=${file_name##*_}
first_seen_date=${first_seen_date:0:4}-${first_seen_date:4:2}-${first_seen_date:6:2}
echo $first_seen_date


reset()
{	# uh. resets
	ess purge local
	ess server reset
}

datastore_setup()
{	# do basic setup to set up local datastore and add category of steam
	# data logs

	ess select local
	ess summary
	ess category add steam_queries "/data/steam/*steam*.csv.gz" --overwrite
	ess summary steam_queries
}

server_setup()
{	# create database to store rows of unique appids, vector to pick out
	# first occurrence of each appid. start server and flash up summary.

	ess create database game_entries
	ess create table appid_stats s:query_date s,pkey:appid s:Title i:grade i:n_reviews f:retail_price f:sale_price i:time0 i:cur_time i,tkey:delta_t f:percent
	ess create vector first_seen s,pkey:appid s,+first:query_date i,+max:n_reviews
	#	ess create vector popular s,pkey:appid i,+max:n_reviews
	ess server commit
	ess server summary
}

process_data()
{	# stream steam_queries to fill game_entries database with the first
	# occurence of each unique appid. then export the results to csv
	# with any bundles (comma separated appids) filtered out.

	ess stream steam_queries "*" "*" \
	"aq_pp -f,eok - -d %cols -imp game_entries:first_seen" --progress --debug

	ess exec "aq_udb -exp game_entries:first_seen -o,notitle -\\
	 | aq_pp -d s:appid s:query_date i:n_reviews -mapf appid '%*,%%bundles%%' \\
	-mapc s:bundles '%%bundles%%' -filt 'bundles == \"\"'" \
	> all_first_seen_dates.csv
}

filter_incomplete()
{	# grep out all of the appids that appear in the first log, because those
	# do not have full data capture range, and so are incomplete.

	grep -v $first_seen_date all_first_seen_dates.csv > first_seen_dates.csv
	
}

convert_time()
{	# take query_date and convert into seconds under first_time parameter
	# then output to new csv.
	aq_pp -f,+1 first_seen_dates.csv -d s:appid s:query_date i:n_reviews x \
		-eval i:time0 'DateToTime(query_date,"Y.m.d")' \
		-c appid time0 n_reviews > nreviewtest.csv

}

calc_stats()
{	# Define column spec for steam_queries to use -cmb to add the 
	# first seen (time0) data from lookup table (appids_list.csv).
	# For valid entries, calculate the cur_time, time elapsed since
	# released (delta_t), and discount percentage (percent).
	ess stream steam_queries "*" "*" "aq_pp -f,+1,eok - -d \\
		s:query_date s:appid s:Title X i:grade i:n_reviews \\
		s:full_price s:discount_price X \\
		-cmb,+1 appids_list.csv s:appid i:time0 \\
		-filt 'time0 > 0' \\
		-eval i:cur_time 'DateToTime(query_date,\"Y.m.d\")' \\
		-eval i:delta_t 'cur_time-time0' \\
		-eval f:retail_price 'ToF(full_price)' \\
		-eval f:sale_price 'ToF(discount_price)' \\
		-if -filt 'retail_price>0' -eval f:percent \\
		'(retail_price-sale_price)/retail_price' \\
		-endif \\
		-c query_date appid Title grade n_reviews retail_price \\
		sale_price time0 cur_time delta_t percent \\
		-imp game_entries:appid_stats" | head -n 150

}

#puttingback_inudb()
	# export results with udb command only pick a few columns like
	# appid, title, time0, time, grade, n_reviews


reset
datastore_setup
server_setup
process_data
filter_incomplete
convert_time
#calc_stats

