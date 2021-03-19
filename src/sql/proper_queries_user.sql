--
-- proper SQL, for database queries
--
-- fixed example:
-- user = cmichel,
-- groupe = chimie,
-- year = 2012 (in epoch, 01-01-2012, 01-01-2013)
--

-- nb de jobs réussis d'un user (cmichel, 2012)
SELECT users.login, COUNT(job_.id_job_)
FROM job_, users
WHERE job_.id_user = users.id_user
    AND users.login = 'cmichel'
    AND (job_.failed = 0 OR job_.exit_status = 0)
    AND job_.start_time >= 1325376000
    AND job_.start_time <= 1356998400
GROUP BY users.login ;
-- nb de jobs plantés d'un user (cmichel, 2012)
SELECT users.login, COUNT(job_.id_job_)
FROM job_, users
WHERE job_.id_user = users.id_user
    AND users.login = 'cmichel'
    AND job_.failed != 0
    AND job_.exit_status != 0
    AND job_.start_time >= 1325376000
    AND job_.start_time <= 1356998400
GROUP BY users.login ;

-- max, avg, min for ru_wallclock, user cmichel, 2012
SELECT
    users.login,
    MAX(job_.ru_wallclock) AS max_wall,
    AVG(job_.ru_wallclock) AS avg_wall,
    MIN(job_.ru_wallclock) AS min_wall
FROM 
    job_, users
WHERE
    job_.id_user = users.id_user
    AND users.login = 'cmichel'
    AND (job_.failed = 0 OR job_.exit_status = 0)
    AND job_.start_time >= 1325376000
    AND job_.start_time <= 1356998400
GROUP BY users.login ;
-- jobs au dessus de avg, user cmichel, 2012
SELECT 
    users.login, COUNT(job_.id_job_)
FROM 
    job_, users
WHERE 
    job_.id_user = users.id_user
    AND users.login = 'cmichel'
    AND (job_.failed = 0 OR job_.exit_status = 0)
    AND job_.start_time >= 1325376000
    AND job_.start_time <= 1356998400
    -- avg donné par requête imbriquée
    AND job_.ru_wallclock > (
    SELECT 
        AVG(job_.ru_wallclock)
    FROM 
        job_, users
    WHERE 
        job_.id_user = users.id_user
        AND users.login = 'cmichel'
        AND (job_.failed = 0 OR job_.exit_status = 0)
        AND job_.start_time >= 1325376000
        AND job_.start_time <= 1356998400
    GROUP BY users.login
    )
GROUP BY users.login ;
-- jobs en dessous de avg, user cmichel, 2012
SELECT 
    users.login, COUNT(job_.id_job_)
FROM 
    job_, users
WHERE 
    job_.id_user = users.id_user
    AND users.login = 'cmichel'
    AND (job_.failed = 0 OR job_.exit_status = 0)
    AND job_.start_time >= 1325376000
    AND job_.start_time <= 1356998400
    -- avg donné par requête imbriquée
    AND job_.ru_wallclock < (
    SELECT 
        AVG(job_.ru_wallclock)
    FROM 
        job_, users
    WHERE 
        job_.id_user = users.id_user
        AND users.login = 'cmichel'
        AND (job_.failed = 0 OR job_.exit_status = 0)
        AND job_.start_time >= 1325376000
        AND job_.start_time <= 1356998400
    GROUP BY users.login
    )
GROUP BY users.login ;

-- composite cmichel vs chimie
-- nb de jobs réussis, nb d'heures d'un user (cmichel, 2012)
SELECT users.login, COUNT(job_.id_job_) AS nb_job, SUM(job_.cpu) AS sum_cpu
FROM job_, users
WHERE job_.id_user = users.id_user
    AND users.login = 'cmichel'
    AND (job_.failed = 0 OR job_.exit_status = 0)
    AND job_.start_time >= 1325376000
    AND job_.start_time <= 1356998400
GROUP BY users.login ;
-- nb de jobs réussis, nb d'heures d'un groupe (chimie, 2012) moins un user particulier
SELECT groupes.group_name, COUNT(job_.id_job_) AS nb_job, SUM(job_.cpu) AS sum_cpu
FROM job_, groupes, users
WHERE job_.id_groupe = groupes.id_groupe
    AND groupes.group_name = 'chimie'
    AND (job_.failed = 0 OR job_.exit_status = 0)
    AND job_.start_time >= 1325376000
    AND job_.start_time <= 1356998400
    AND job_.id_user = users.id_user
    AND users.login != 'cmichel'
GROUP BY groupes.id_groupe ;

