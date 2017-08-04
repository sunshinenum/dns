#!/usr/bin/python
# coding=utf-8


def feature_extraction():
    data_tbl_name = "constructed_data_50"
    feature_tbl_name = "features_constructed_50"
    sql = """
-- import raw data
drop table if exists dns.%s;
CREATE TABLE dns.%s(id bigint, min double, sec bigint, name string,thirdld string, sld String, tld string, src_ip string, label double) row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH "/media/ybc/S/MachineLearning/data/dns/queriesGenerated_0.3_0.2" OVERWRITE INTO TABLE dns.%s;

-- you need to score names

-- extract features
use dns;
drop table if exists dns.%s;
create table dns.%s as select 
main.min, main.id, main.sld, main.name, gpm.gpm, nsp.nsp, ipsp.ipsp, ipc.ipc, len_lvl.name_len, 
len_lvl.name_lvl, nvl(is_ns.is_ns, 0.0) as is_ns, nvl(bigram.score, 0.0) as score,
-- in-addr
case when main.sld = "in-addr"
     then 0.0
     else 1.0
end as is_arpa,
-- label
main.label
from dns.%s main
left outer join
-- name space during a period of time of a sld
(select min, sld,  ln(count(name)) as nsp from dns.%s group by min, sld) nsp
on main.min = nsp.min and main.sld = nsp.sld
-- qpm during a period of time of a sld
left outer join
(select min, sld,  ln(count(id)) as gpm from dns.%s group by min, sld) gpm
on main.min = gpm.min and main.sld = gpm.sld
-- ip space during a period of time of a sld
left outer join
(select min, sld,  ln(count(src_ip)) as ipsp from dns.%s group by min, sld) ipsp
on main.min = ipsp.min and main.sld = ipsp.sld
-- ip
left outer join
(select min, src_ip, ln(count(id)) as ipc from dns.%s group by min, src_ip) ipc
on main.min = ipc.min and main.src_ip = ipc.src_ip
-- name length, level
left outer join
(select id, name,  length(name) as name_len, size(split(name, "\\.")) as name_lvl
from dns.%s) len_lvl
on main.id = len_lvl.id
-- is ns?
left outer join
(select id, name,
case when find_in_set(thirdld, "ns1,ns2,ns3,ns4,ns5,ns6,ns7,ns8,ns9,ns10,ns11,ns12,ns13,ns14,ns15,ns16,ns17,ns18,ns19,ns20") = 0
     then 0.0
     else 1.0
end as is_ns
from dns.%s) is_ns
on main.id = is_ns.id
-- bigram score
left outer join
(select name, log(score) as score from dns.bigram_%s) bigram
on main.name = bigram.name
;
""" % (data_tbl_name, data_tbl_name, data_tbl_name, feature_tbl_name, feature_tbl_name
       , data_tbl_name, data_tbl_name, data_tbl_name, data_tbl_name
       , data_tbl_name, data_tbl_name, data_tbl_name, data_tbl_name)
    print sql


if __name__ == '__main__':
    feature_extraction()
