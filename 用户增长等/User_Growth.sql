-- 20180101-20180107第一周

-- 第一周新增用户
select 'new' as type,count(uid) as num
from tag_user_info
where pt=to_char(dateadd(getdate(),-1,'dd'),'yyyymmdd')
and to_char(reg_tm,'yyyymmdd')>='${bdp.system.bizdate}' --20180101
and to_char(reg_tm,'yyyymmdd')<to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),7,'dd'),'yyyymmdd')
union all
-- 第一周保留用户数
select 'retained' as type,count(t1.uid) as num
from(
	select uid 
	from tag_user_behavior
	where pt>='${bdp.system.bizdate}'
	and pt<to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),7,'dd'),'yyyymmdd')
)t1
left semi join(
	select uid 
	from tag_user_behavior
	where pt<'${bdp.system.bizdate}'
	and pt>=to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),-7,'dd'),'yyyymmdd')
)t2
on t1.uid=t2.uid
union all

-- 第一周召回用户
select 'resurrected' as type,count(uid) as num
from(
	select t1.uid 
		,case when t2.uid is not null and t3.uid is null then 1 else 0 end as return_user_ind
	from (
		select uid from tag_user_behavior
		where pt>='${bdp.system.bizdate}'
		and pt<to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),7,'dd'),'yyyymmdd')
		group by uid
	)t1
	left outer join (
		select uid from tag_user_behavior
		where pt< to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),-7,'dd'),'yyyymmdd')
		group by uid
	)t2
	on t1.uid=t2.uid
	left outer join (
		select uid from tag_user_behavior
		where pt>= to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),-7,'dd'),'yyyymmdd')
		and pt<'${bdp.system.bizdate}'
		group by uid
	)t3
	on t1.uid=t3.uid
)t
union all

-- 第一周流失用户
select 'churned' as type,count(t1.uid) as num
from (
	-- 上周用户
	select uid 
	from tag_user_behavior
	where pt>= to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),-7,'dd'),'yyyymmdd')
	and pt<'${bdp.system.bizdate}'
)t1
left outer join (
	-- 本周用户
	select uid 
	from tag_user_behavior
	where pt>='${bdp.system.bizdate}'
	and pt<to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),7,'dd'),'yyyymmdd')
)t2
on t1.uid=t2.uid
where t2.uid is null

union all

-- 留存率(retention rate)上周注册用户且在这周登陆过的用户
select 'retention_rate' as type, sum(case when t2.uid is null then 0 else 1 end)/count(t1.uid) as num
from (
	select uid
	from tag_user_info
	where pt=to_char(dateadd(getdate(),-1,'dd'),'yyyymmdd')
	and to_char(reg_tm,'yyyymmdd')<'${bdp.system.bizdate}' 
	and to_char(reg_tm,'yyyymmdd')>=to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),-7,'dd'),'yyyymmdd')
	group by uid
)t1
left outer join(
	select uid 
	from tag_user_behavior
	where pt>='${bdp.system.bizdate}'
	and pt<to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),7,'dd'),'yyyymmdd')
)t2
on t1.uid=t2.uid



-- 汇总
select tt1.pt
	, tt1.new
	, tt2.retained
	, tt3.resurrected
	, tt4.churned, tt5.retention_rate
	, (tt1.new+tt3.resurrected)/tt4.churned as quick_ratio
from(
select '${bdp.system.bizdate}' as pt, count(uid) as new
	from tag_user_info
	where pt=to_char(dateadd(getdate(),-1,'dd'),'yyyymmdd')
	and to_char(reg_tm,'yyyymmdd')>='${bdp.system.bizdate}' --20180101
	and to_char(reg_tm,'yyyymmdd')<to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),7,'dd'),'yyyymmdd')
)tt1
left join(
	-- 第一周保留用户数
	select '${bdp.system.bizdate}' as pt, count(t1.uid) as retained
	from(
		select uid 
		from tag_user_behavior
		where pt>='${bdp.system.bizdate}'
		and pt<to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),7,'dd'),'yyyymmdd')
	)t1
	left semi join(
		select uid 
		from tag_user_behavior
		where pt<'${bdp.system.bizdate}'
		and pt>=to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),-7,'dd'),'yyyymmdd')
	)t2
	on t1.uid=t2.uid
)tt2
on tt1.pt=tt2.pt
left join(
	-- 第一周召回用户
	select '${bdp.system.bizdate}' as pt, count(uid) as resurrected
	from(
		select t1.uid 
			,case when t2.uid is not null and t3.uid is null then 1 else 0 end as return_user_ind
		from (
			select uid from tag_user_behavior
			where pt>='${bdp.system.bizdate}'
			and pt<to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),7,'dd'),'yyyymmdd')
			group by uid
		)t1
		left outer join (
			select uid from tag_user_behavior
			where pt< to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),-7,'dd'),'yyyymmdd')
			group by uid
		)t2
		on t1.uid=t2.uid
		left outer join (
			select uid from tag_user_behavior
			where pt>= to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),-7,'dd'),'yyyymmdd')
			and pt<'${bdp.system.bizdate}'
			group by uid
		)t3
		on t1.uid=t3.uid
	)t
	where return_user_ind=1
)tt3
on tt1.pt=tt3.pt
left join(
	-- 第一周流失用户
	select '${bdp.system.bizdate}' as pt, count(t1.uid) as churned
	from (
		-- 上周用户
		select uid 
		from tag_user_behavior
		where pt>= to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),-7,'dd'),'yyyymmdd')
		and pt<'${bdp.system.bizdate}'
	)t1
	left outer join (
		-- 本周用户
		select uid 
		from tag_user_behavior
		where pt>='${bdp.system.bizdate}'
		and pt<to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),7,'dd'),'yyyymmdd')
	)t2
	on t1.uid=t2.uid
	where t2.uid is null
)tt4
on tt1.pt=tt4.pt
left join(
	-- 留存率(retention rate)上周注册用户且在这周登陆过的用户
	select '${bdp.system.bizdate}' as pt, sum(case when t2.uid is null then 0 else 1 end)/count(t1.uid) as retention_rate
	from (
		select uid
		from tag_user_info
		where pt=to_char(dateadd(getdate(),-1,'dd'),'yyyymmdd')
		and to_char(reg_tm,'yyyymmdd')<'${bdp.system.bizdate}' 
		and to_char(reg_tm,'yyyymmdd')>=to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),-7,'dd'),'yyyymmdd')
		group by uid
	)t1
	left outer join(
		select uid 
		from tag_user_behavior
		where pt>='${bdp.system.bizdate}'
		and pt<to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),7,'dd'),'yyyymmdd')
	)t2
	on t1.uid=t2.uid
)tt5
on tt1.pt=tt5.pt
