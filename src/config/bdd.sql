
create table IF NOT EXISTS users (
    id_user bigserial PRIMARY KEY,
    login varchar(255) NOT NULL UNIQUE
    );

create table IF NOT EXISTS hosts (
    id_host bigserial PRIMARY KEY,
    hostname varchar(255) NOT NULL UNIQUE
    );

create table IF NOT EXISTS groupes (
    id_groupe bigserial PRIMARY KEY,
    group_name varchar(255) NOT NULL UNIQUE
    );

create table IF NOT EXISTS metagroups (
    id_metagroup bigserial PRIMARY KEY,
    metaname varchar(255) NOT NULL UNIQUE
    );

create table IF NOT EXISTS clusters (
    id_cluster bigserial PRIMARY KEY,
    cluster_name varchar(255) NOT NULL UNIQUE
    );

create table IF NOT EXISTS queues (
    id_queue bigserial PRIMARY KEY,
    queue_name  varchar(255) NOT NULL UNIQUE
    );

-- liaison users/groupes
create table IF NOT EXISTS users_in_groupe (
    id_groupe bigint NOT NULL,
    id_user bigint NOT NULL,
    PRIMARY KEY (id_groupe, id_user),
    FOREIGN KEY (id_groupe) REFERENCES groupes (id_groupe),
    FOREIGN KEY (id_user) REFERENCES users (id_user)
    );

-- liaison hosts/queues
create table IF NOT EXISTS hosts_in_queue (
    id_queue bigint NOT NULL,
    id_host bigint NOT NULL,
    PRIMARY KEY (id_queue, id_host),
    FOREIGN KEY (id_queue) REFERENCES queues (id_queue),
    FOREIGN KEY (id_host) REFERENCES hosts (id_host)
    );

-- liaison hosts/clusters
create table IF NOT EXISTS hosts_in_cluster (
    id_cluster bigint NOT NULL,
    id_host bigint NOT NULL,
    PRIMARY KEY (id_cluster, id_host),
    FOREIGN KEY (id_cluster) REFERENCES clusters (id_cluster),
    FOREIGN KEY (id_host) REFERENCES hosts (id_host)
    );

-- liaison group/metagroups
create table IF NOT EXISTS groupe_in_meta (
    id_metagroup bigint NOT NULL,
    id_groupe bigint NOT NULL,
    PRIMARY KEY (id_metagroup, id_groupe),
    FOREIGN KEY (id_metagroup) REFERENCES metagroups (id_metagroup),
    FOREIGN KEY (id_groupe) REFERENCES groupes (id_groupe)
    );

-- liaison user/metagroups
create table IF NOT EXISTS user_in_meta (
    id_metagroup bigint NOT NULL,
    id_user bigint NOT NULL,
    PRIMARY KEY (id_metagroup, id_user),
    FOREIGN KEY (id_metagroup) REFERENCES metagroups (id_metagroup),
    FOREIGN KEY (id_user) REFERENCES users (id_user)
    );

-- table centrale
create table IF NOT EXISTS job_ (
    id_queue bigint NOT NULL,
    id_host bigint NOT NULL,
    id_groupe bigint NOT NULL,
    id_user bigint NOT NULL,
    FOREIGN KEY (id_queue) REFERENCES queues (id_queue),
    FOREIGN KEY (id_host) REFERENCES hosts (id_host),
    FOREIGN KEY (id_groupe) REFERENCES groupes (id_groupe),
    FOREIGN KEY (id_user) REFERENCES users (id_user),
    job_name varchar(128) NOT NULL,
    job_id integer NOT NULL,
    submit_time bigint NOT NULL,
    start_time bigint NOT NULL,
    end_time bigint NOT NULL,
    failed integer,
    exit_status integer,
    ru_wallclock real NOT NULL,
    ru_utime real NOT NULL,
    ru_stime real NOT NULL,
    project varchar(255),
    slots integer NOT NULL,
    cpu real NOT NULL,
    mem real NOT NULL,
    io real NOT NULL,
    maxvmem real NOT NULL,
    PRIMARY KEY (id_queue, id_host, id_user, job_id, start_time, end_time)
    );
