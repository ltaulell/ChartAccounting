--
-- proper SQL, for database queries
--
-- fixed example:
-- groupe = chimie,
-- year = 2012 (in epoch, 01-01-2012, 01-01-2013)
--

-- combo (nb jobs réussi, nb jobs plantés) -> taux de réussite
-- nb de jobs réussis, nb heures consommées d'un groupe (chimie, 2012)
SELECT groupes.group_name, COUNT(job_.id_job_), SUM(job_.cpu) AS sum_cpu
FROM job_, groupes
WHERE job_.id_groupe = groupes.id_groupe
    AND groupes.group_name = 'chimie'
    AND (job_.failed = 0 OR job_.exit_status = 0)
    AND job_.start_time >= 1325376000
    AND job_.start_time <= 1356998400
GROUP BY groupes.id_groupe ;
-- nb de jobs plantés d'un groupe (chimie, 2012)
SELECT groupes.group_name, COUNT(job_.id_job_)
FROM job_, groupes
WHERE job_.id_groupe = groupes.id_groupe
    AND groupes.group_name = 'chimie'
    AND job_.failed != 0
    AND job_.exit_status != 0
    AND job_.start_time >= 1325376000
    AND job_.start_time <= 1356998400
GROUP BY groupes.id_groupe ;

-- execution time
-- min, avg, max for ru_wallclock, groupe chimie, 2012
SELECT 
    groupes.group_name, 
    min(job_.ru_wallclock),
    avg(job_.ru_wallclock),
    max(job_.ru_wallclock)
FROM 
    job_, groupes 
WHERE job_.id_groupe = groupes.id_groupe 
    AND groupes.group_name = 'chimie' 
    AND job_.start_time >= 1325376000 
    AND job_.start_time <= 1356998400 
GROUP BY groupes.group_name ;
-- au dessus de avg, groupe chimie, 2012
SELECT 
    groupes.group_name, COUNT(job_.id_job_)
FROM 
    job_, groupes
WHERE 
    job_.id_groupe = groupes.id_groupe
    AND groupes.group_name = 'chimie'
    AND (job_.failed = 0 OR job_.exit_status = 0)
    AND job_.start_time >= 1325376000
    AND job_.start_time <= 1356998400
    -- avg donné par requête imbriquée
    AND job_.ru_wallclock > (
    SELECT 
        AVG(job_.ru_wallclock)
    FROM 
        job_, groupes
    WHERE 
        job_.id_groupe = groupes.id_groupe
        AND groupes.group_name = 'chimie'
        AND (job_.failed = 0 OR job_.exit_status = 0)
        AND job_.start_time >= 1325376000
        AND job_.start_time <= 1356998400
    GROUP BY groupes.group_name
    )
GROUP BY groupes.group_name ;
-- en dessous de avg, groupe chimie, 2012
SELECT 
    groupes.group_name, COUNT(job_.id_job_)
FROM 
    job_, groupes
WHERE 
    job_.id_groupe = groupes.id_groupe
    AND groupes.group_name = 'chimie'
    AND (job_.failed = 0 OR job_.exit_status = 0)
    AND job_.start_time >= 1325376000
    AND job_.start_time <= 1356998400
    -- avg donné par requête imbriquée
    AND job_.ru_wallclock < (
    SELECT 
        AVG(job_.ru_wallclock)
    FROM 
        job_, groupes
    WHERE 
        job_.id_groupe = groupes.id_groupe
        AND groupes.group_name = 'chimie'
        AND (job_.failed = 0 OR job_.exit_status = 0)
        AND job_.start_time >= 1325376000
        AND job_.start_time <= 1356998400
    GROUP BY groupes.group_name
    )
GROUP BY groupes.group_name ;
-- nb jobs réussis, groupe chimie, 2012, durée inférieure à 1 jour (86400)
SELECT groupes.group_name, COUNT(job_.id_job_)
FROM job_, groupes
WHERE job_.id_groupe = groupes.id_groupe
    AND groupes.group_name = 'chimie'
    AND (job_.failed = 0 OR job_.exit_status = 0)
    AND job_.start_time >= 1325376000
    AND job_.start_time <= 1356998400
    AND job_.ru_wallclock < 86400
GROUP BY groupes.group_name ;
-- nb jobs réussis, groupe chimie, 2012, durée entre 1 jour (86400) et 1 week (604800)
SELECT groupes.group_name, COUNT(job_.id_job_)
FROM job_, groupes
WHERE job_.id_groupe = groupes.id_groupe
    AND groupes.group_name = 'chimie'
    AND (job_.failed = 0 OR job_.exit_status = 0)
    AND job_.start_time >= 1325376000
    AND job_.start_time <= 1356998400
    AND job_.ru_wallclock > 86400
    AND job_.ru_wallclock < 604800
GROUP BY groupes.group_name ;
-- nb jobs réussis, groupe chimie, 2012, durée entre 1 week (604800) et 1 mois (18144000)
SELECT groupes.group_name, COUNT(job_.id_job_)
FROM job_, groupes
WHERE job_.id_groupe = groupes.id_groupe
    AND groupes.group_name = 'chimie'
    AND (job_.failed = 0 OR job_.exit_status = 0)
    AND job_.start_time >= 1325376000
    AND job_.start_time <= 1356998400
    AND job_.ru_wallclock > 604800
    AND job_.ru_wallclock < 18144000
GROUP BY groupes.group_name ;
-- nb jobs réussis, groupe chimie, 2012, durée supérieure à 1 mois (18144000)
SELECT groupes.group_name, COUNT(job_.id_job_)
FROM job_, groupes
WHERE job_.id_groupe = groupes.id_groupe
    AND groupes.group_name = 'chimie'
    AND (job_.failed = 0 OR job_.exit_status = 0)
    AND job_.start_time >= 1325376000
    AND job_.start_time <= 1356998400
    AND job_.ru_wallclock > 18144000
GROUP BY groupes.group_name ;

-- TODO
-- memory usage


-- slots usage
-- min, avg, max for slots, groupe chimie, 2012
SELECT 
    groupes.group_name, 
    min(job_.slots),
    avg(job_.slots),
    max(job_.slots)
FROM 
    job_, groupes 
WHERE job_.id_groupe = groupes.id_groupe 
    AND groupes.group_name = 'chimie' 
    AND job_.start_time >= 1325376000 
    AND job_.start_time <= 1356998400 
GROUP BY groupes.group_name ;

-- temps d'attente


-- Top Tens




