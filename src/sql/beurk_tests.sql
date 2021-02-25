
--
-- \dt+    -> tables
-- \ds+    -> sequences
-- \dS+ <table>    -> types et contraintes
--

select * from users;
select * from users where login like 'ltaulell';
select * from hosts where hostname like 'r410%';

ALTER TABLE job_ ALTER COLUMN job_name TYPE varchar(255);
ALTER TABLE job_ ALTER COLUMN job_name DROP NOT NULL;
ALTER TABLE job_ ALTER COLUMN ru_wallclock DROP NOT NULL;
ALTER TABLE job_ ALTER COLUMN ru_utime DROP NOT NULL;
ALTER TABLE job_ ALTER COLUMN ru_stime DROP NOT NULL;
ALTER TABLE job_ ALTER COLUMN slots DROP NOT NULL;
ALTER TABLE job_ ALTER COLUMN cpu DROP NOT NULL;
ALTER TABLE job_ ALTER COLUMN mem DROP NOT NULL;
ALTER TABLE job_ ALTER COLUMN io DROP NOT NULL;
ALTER TABLE job_ ALTER COLUMN maxvmem DROP NOT NULL;

-- explain, pour les query plan
-- voir où ça bouffe du temps

select sum(ru_utime) from job_ where id_user = (select id_user from users where login = 'cmichel');

select sum(ru_utime) from job_ where id_groupe = (select id_groupe from groupes where group_name = 'icbms')

select max(ru_wallclock) from job_ where id_groupe = (select id_groupe from groupes where group_name = 'icbms');

select max(ru_wallclock) from job_ where id_groupe = (select id_groupe from groupes where group_name = 'chimie');

select max(slots) from job_ where id_groupe = (select id_groupe from groupes where group_name = 'chimie');

select * from job_ where start_time = (select max(start_time) from job_);
select * from job_ where submit_time = (select max(submit_time) from job_);
select * from job_ where end_time = (select max(end_time) from job_);

-- dernier job inséré
select max(id_job_) from job_;

-- info sur le dernier job inséré
SELECT q.queue_name, h.hostname, g.group_name, u.login, j.job_id, j.submit_time
FROM job_ j
INNER JOIN queues q ON j.id_queue = q.id_queue
INNER JOIN hosts h ON j.id_host = h.id_host
INNER JOIN groupes g ON j.id_groupe = g.id_groupe
INNER JOIN users u ON j.id_user = u.id_user
WHERE j.id_job_ = 
    (SELECT max(id_job_) FROM job_)
    ;

-- # https://www.epochconverter.com/
-- epoch (2012-01-01) 1325376000
-- epoch (2012-12-31) 1356912000

-- https://sql.sh/cours/jointures

-- total cpu d'un group (chimie) entre 01-01-2012 et 31-12-2012 (start_time)
select sum(cpu) from job_ where id_groupe = 
    (select id_groupe from groupes where group_name = 'chimie') 
AND start_time >= 1325376000 AND start_time <= 1356912000;
-- total cpu d'un group (chimie) entre 01-01-2012 et 31-12-2012 (start_time)
SELECT sum(job_.cpu)
FROM job_, groupes
WHERE job_.id_groupe = groupes.id_groupe
  AND groupes.group_name = 'chimie'
  AND job_.start_time >= 1325376000
  AND job_.start_time <= 1356912000
  ;

-- total cpu, par groupe, entre 01-01-2012 et 31-12-2012 (start_time)
SELECT g.group_name, sum(j.cpu) AS sum_value
FROM job_ j
INNER JOIN groupes g ON j.id_groupe = g.id_groupe
WHERE j.id_groupe = ANY 
    (SELECT id_groupe
    FROM groupes)
AND start_time >= 1325376000 AND start_time <= 1356912000 
GROUP BY g.group_name, j.id_groupe
ORDER BY sum_value DESC ;
-- total cpu, par groupe, entre 01-01-2012 et 31-12-2012 (start_time), plus rapide
SELECT groupes.group_name, sum(job_.cpu) AS sum_cpu
FROM job_, groupes
WHERE job_.id_groupe = groupes.id_groupe
  AND start_time >= 1325376000 
  AND start_time <= 1356912000
GROUP BY groupes.group_name
ORDER BY sum_cpu DESC;

-- top ten, id_host, par cluster
SELECT j.id_host, sum(ru_utime) AS sum_value
FROM job_ j
WHERE id_host = ANY 
    (SELECT id_host 
    FROM hosts_in_clusters 
    WHERE id_cluster = 
        (SELECT id_cluster 
        FROM clusters 
        WHERE cluster_name = 'E5'
        )
    )
GROUP BY j.id_host
ORDER BY sum_value DESC
LIMIT 10 ;

-- pareil, plus rapide
SELECT hosts.hostname, 
    sum(job_.cpu) AS sum_cpu, 
    sum(job_.ru_utime) AS sum_utime
FROM job_,
    hosts,
    hosts_in_clusters,
    clusters
WHERE job_.id_host = hosts.id_host AND 
    hosts.id_host = hosts_in_clusters.id_host AND
    hosts_in_clusters.id_cluster = clusters.id_cluster AND
    clusters.cluster_name = 'E5'
GROUP BY hosts.hostname, job_.id_host
ORDER BY sum_cpu DESC
LIMIT 10 ;

-- top ten, with hostname, par cluster
SELECT h.hostname, sum(j.cpu), sum(j.ru_utime) AS sum_value
FROM job_ j
INNER JOIN hosts h ON j.id_host = h.id_host
WHERE j.id_host = ANY 
    (SELECT id_host 
    FROM hosts_in_clusters 
    WHERE id_cluster = 
        (SELECT id_cluster 
        FROM clusters 
        WHERE cluster_name = 'E5'
        )
    )
GROUP BY h.hostname, j.id_host
ORDER BY sum_value DESC
LIMIT 10 ;

-- top ten, with cluster_name, hostname, tout clusters confondus
SELECT 
  clusters.cluster_name, 
  hosts.hostname, 
  sum(job_.cpu) AS sum_cpu_value,
  sum(job_.ru_utime) AS sum_cpu_utime
FROM 
  job_, 
  clusters, 
  hosts_in_clusters, 
  hosts
WHERE 
  hosts_in_clusters.id_cluster = clusters.id_cluster AND
  hosts.id_host = job_.id_host AND
  hosts.id_host = hosts_in_clusters.id_host
GROUP BY
  clusters.cluster_name, hosts.hostname
ORDER BY
  sum_cpu_value DESC
LIMIT 10;

