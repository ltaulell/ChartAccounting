-- $Id$
-- insert clusters data

-- create table IF NOT EXISTS clusters (
--    id_cluster bigserial PRIMARY KEY,
--    cluster_name varchar(255) NOT NULL UNIQUE
--    );

INSERT INTO clusters(cluster_name) VALUES ('default') RETURNING id;
INSERT INTO clusters(cluster_name) VALUES ('X5') RETURNING id;
INSERT INTO clusters(cluster_name) VALUES ('E5') RETURNING id;
INSERT INTO clusters(cluster_name) VALUES ('Lake') RETURNING id;
INSERT INTO clusters(cluster_name) VALUES ('Epyc') RETURNING id;
-- INSERT INTO clusters(cluster_name) VALUES ('') RETURNING id;
