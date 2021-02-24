
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


-- explain

select sum(ru_utime) from job_ where id_user = (select id_user from users where login = 'cmichel');
select sum(ru_utime) from job_ where id_groupe = (select id_groupe from groupes where group_name = 'icbms')

select max(ru_wallclock) from job_ where id_groupe = (select id_groupe from groupes where group_name = 'icbms');
select max(ru_wallclock) from job_ where id_groupe = (select id_groupe from groupes where group_name = 'chimie');
select max(slots) from job_ where id_groupe = (select id_groupe from groupes where group_name = 'chimie');

-- # https://www.epochconverter.com/
-- epoch (2012-01-01) 1325376000
-- epoch (2012-12-31) 1356912000

select sum(cpu) from job_ where id_groupe = (select id_groupe from groupes where group_name = 'chimie') AND start_time >= 1325376000 AND start_time <= 1356912000;

-- essais

-- top ten, id_host
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

-- https://sql.sh/cours/jointures
-- top ten, with hostname
SELECT h.hostname, sum(j.ru_utime) AS sum_value
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
