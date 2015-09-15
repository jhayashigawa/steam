# Generated on 2015-09-15 01:12:48.482559
@Server:
127.0.0.1|127.0.0.1:10010

@Table:appid_stats
s:query_date
s,pkey:appid
s:Title
i:grade
i:n_reviews
f:retail_price
f:sale_price
i:time0
i:cur_time
i,tkey:delta_t
f:percent


@Vector:first_seen
s,pkey:appid
s,+first:query_date


