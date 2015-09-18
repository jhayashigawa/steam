# Generated on 2015-09-18 22:32:53.383003
@Server:
127.0.0.1|127.0.0.1:10010

@Table:appid_stats
s:query_date
s,pkey:appid
s:Title
i:grade
f:retail_price
f:sale_price
i:time0
i:reviews
i:cur_time
i,tkey:delta_t
f:percent


@Vector:lookup_stats
s,pkey:appid
s,+first:query_date
i,+last:grade
i,+max:n_reviews


