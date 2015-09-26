#!/bin/bash

# This script processes logs of data from the Steam store, which are 
# kept in /data/steam in gzipped .csv format. Our goal is to take these
# logs and run calculations on these games' prices over time, and then
# output the results in an easy-to-use format for further analysis.
# By using datastores, databases, tables, and vectors, we compute
# statistics and filter out invalid entries. Finally, we can
# save our results of discount percentage over time for each game
# to a .csv file. From there, we can use Python, Tableau, Excel, and
# other programs to visualize our findings.



calc_first_seen()
{	# automatically detect first log timestamp in order to determine
	# which game titles start showing up in the Steam store after
	# the first query. We only want to record games that we have 
	# data for the entire time they are available for sale.

	file_name=`ls /data/steam | head -n1`
	file_name=${file_name%%.*}
	first_seen_date=${file_name##*_}
	first_seen_date=${first_seen_date:0:4}-${first_seen_date:4:2}-${first_seen_date:6:2}
	echo $first_seen_date
}

reset()
{	# Resets the datastore and database.
	ess purge local
	ess server reset
}

datastore_setup()
{	# Set up a local datastore and add a category called steam_queries.
	# In doing so, we collect and organize all files matching the steam
	# data logs' name pattern and thus have all files together ready
	# for processing.

	ess select local
	ess summary
	ess category add steam_queries "/data/steam/*steam*.csv.gz" --overwrite
	ess summary steam_queries
}

server_setup()
{	# We create a database named game_entries to store a table called 
	# appid_stats and a vector called lookup_stats so we can analyze 
	# our logs. We set up a table called appid_stats that stores every 
	# query and datapoint of every instance of a game showing up in the
	# steam store and the stats (like the  price) for each unique appid.
	# A vector called lookup_stats is created such that we can find
	# the first time a title appeared, the most recent grade, and the 
	# maximum number of reviews for each unique game. This is so we can
	# later include these statistics for further analysis.
	ess create database game_entries
	ess create table appid_stats s:query_date s,pkey:appid s:Title i:grade f:retail_price f:sale_price i:time0 i:reviews i:cur_time i,tkey:delta_t f:percent
	ess create vector lookup_stats s,pkey:appid s,+first:query_date i,+last:grade i,+max:n_reviews
	ess server commit
	ess server summary
}

process_data()
{	# With ess stream, we apply the lookup_stats vector to all the 
	# entries in the datastore. The lookup_stats vector collects
	# computes statistics for each unique appid (that cannot be
	# calculated when iterating through line by line only once).
	# Next with aq_udb and aq_pp, we process these lookup statistics
	# and filter out any bundle appids (which have commas), because
	# we are focusing on single standalone games. Finally pipe this
	# out into a .csv file for further processing.

	ess stream steam_queries "*" "*" \
	"aq_pp -f,eok - -d %cols -imp game_entries:lookup_stats"
	
	ess exec "aq_udb -exp game_entries:lookup_stats -o,notitle -\\
	 | aq_pp -d s:appid s:query_date i:grade i:n_reviews \\
	-mapf appid '%*,%%bundles%%' \\
	-mapc s:bundles '%%bundles%%' -filt 'bundles == \"\"' \\
	-renam n_reviews reviews" \
	> all_first_seen_dates.csv
}

filter_incomplete_records()
{	# Using grep, we remove any appids that are seen in the first 
	# data log. We do this so that we can examine the entire trajectory
	# of a game's sale history, otherwise we don't know what 
	# happened before the first recorded log.

	grep -v $first_seen_date all_first_seen_dates.csv > first_seen_dates.csv
	
}

generate_lookup()
{	# We then convert the query_date into seconds and filter out
	# unreviewed games, for clearer analysis. Next we output the
	# results to a .csv to be used as a lookup table

	aq_pp -f,+1 first_seen_dates.csv \
		-d s:appid s:query_date i:grade i:reviews x \
		-eval i:time0 'DateToTime(query_date,"Y.m.d")' \
		-filt 'reviews > 0' \
		-c appid time0 grade reviews > appids_list.csv

}

popularity_sort()
{	# In order to further refine our lookup table, we sort by 
	# popularity using aq_ord and only output the top 25 games.
	# The echo statement is to put the header row back in because
	# the header is removed by the aq_ord command.

	`echo '"appid","time0","grade","reviews"' > sorted_appids.csv`	
	aq_ord -f,+1 appids_list.csv -sort,dec i:4 | \
	head -n 25 >> sorted_appids.csv
	

}

calc_stats()
{	# Using our lookup table, we add those calculated statistics
	# to each datapoint row. Additionally, we modify our column
	# spec to exclude less relevant information. Then we filter out
	# unreviewed and free games released after the first date.
	# We then calculate more statistics to determine the time that
	# has passed since release (delta_t) and the discount percentage
	# (percent). After this command is run, we will have gotten our
	# desired data to plot game discount price over time.

	ess stream steam_queries "*" "*" "aq_pp -f,+1,eok - -d \\
		s:query_date s:appid s:Title X X X \\
		s:full_price s:discount_price X \\
		-cmb,+1 sorted_appids.csv s:appid i:time0 i:grade i:reviews \\
		-filt 'time0 > 0' \\
		-filt 'reviews > 0' \\
		-eval i:cur_time 'DateToTime(query_date,\"Y.m.d\")' \\
		-eval i:delta_t 'cur_time-time0' \\
		-eval f:retail_price 'ToF(full_price)' \\
		-eval f:sale_price 'ToF(discount_price)' \\
		-if -filt 'retail_price>0' -eval f:percent \\
		'(retail_price-sale_price)/retail_price' \\
		-endif \\
		-c query_date appid Title retail_price \\
		sale_price time0 grade reviews cur_time delta_t percent \\
		-imp game_entries:appid_stats" --progress | head -n 150 
		
}


output_records()
{	# Finally we simply use aq_udb to output the results from the 
	# game_entries table to top_records.csv. This .csv file can
	# now be used in a variety of programs to generate graphs
	# and visualizations.
	ess exec "aq_udb -exp game_entries:appid_stats" > top_records.csv
}


# Run all of the functions
calc_first_seen
reset
datastore_setup
server_setup
process_data
filter_incomplete_records
generate_lookup
popularity_sort
calc_stats
output_records
